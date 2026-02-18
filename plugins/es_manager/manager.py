"""
Elasticsearch 管理器插件 - Phase 9

功能：
- 连接选择和切换
- 索引列表浏览
- 文档 CRUD 操作
- 分页显示

依赖:
    pip install requests
"""

import json
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QSplitter,
    QListWidget, QListWidgetItem, QTableWidget, QTableWidgetItem,
    QComboBox, QPushButton, QLineEdit, QLabel, QMessageBox,
    QDialog, QTextEdit, QSpinBox, QMenu, QHeaderView,
    QGroupBox, QFormLayout, QApplication, QAbstractItemView
)
from PySide6.QtCore import Qt, QSize
from PySide6.QtGui import QFont, QAction

from core.managers.connection_manager import ConnectionManager
from core.workers.es_worker import ESWorker, ESClient


class JsonEditorDialog(QDialog):
    """
    JSON 编辑器对话框
    
    用于查看和编辑 ES 文档
    """
    
    def __init__(self, doc_data: dict, editable: bool = True, parent=None):
        super().__init__(parent)
        
        self.doc_data = doc_data
        self.editable = editable
        self.result_data = None
        
        self.setWindowTitle("文档详情")
        self.resize(600, 500)
        
        self._setup_ui()
        self._load_data()
    
    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(10)
        
        # ID 显示
        self.id_label = QLabel()
        self.id_label.setStyleSheet("color: #969696; font-weight: bold;")
        layout.addWidget(self.id_label)
        
        # JSON 编辑区
        self.text_edit = QTextEdit()
        self.text_edit.setFont(QFont("Consolas", 11))
        self.text_edit.setStyleSheet("""
            QTextEdit {
                background-color: #1e1e1e;
                color: #d4d4d4;
                border: 1px solid #333;
                padding: 10px;
            }
        """)
        self.text_edit.setReadOnly(not self.editable)
        layout.addWidget(self.text_edit)
        
        # 按钮
        btn_layout = QHBoxLayout()
        
        if self.editable:
            self.format_btn = QPushButton("📝 格式化 JSON")
            self.format_btn.clicked.connect(self._format_json)
            btn_layout.addWidget(self.format_btn)
        
        btn_layout.addStretch()
        
        if self.editable:
            self.save_btn = QPushButton("💾 保存")
            self.save_btn.setStyleSheet("""
                QPushButton {
                    background-color: #238636;
                    color: white;
                    padding: 8px 20px;
                    border: none;
                    border-radius: 4px;
                }
                QPushButton:hover { background-color: #2ea043; }
            """)
            self.save_btn.clicked.connect(self._on_save)
            btn_layout.addWidget(self.save_btn)
        
        self.close_btn = QPushButton("关闭")
        self.close_btn.clicked.connect(self.reject)
        btn_layout.addWidget(self.close_btn)
        
        layout.addLayout(btn_layout)
    
    def _load_data(self):
        """加载文档数据"""
        doc_id = self.doc_data.get("_id", "unknown")
        self.id_label.setText(f"文档 ID: {doc_id}")
        
        source = self.doc_data.get("_source", {})
        formatted = json.dumps(source, ensure_ascii=False, indent=2)
        self.text_edit.setText(formatted)
    
    def _format_json(self):
        """格式化 JSON"""
        try:
            data = json.loads(self.text_edit.toPlainText())
            formatted = json.dumps(data, ensure_ascii=False, indent=2)
            self.text_edit.setText(formatted)
        except json.JSONDecodeError as e:
            QMessageBox.warning(self, "格式错误", f"JSON 格式错误: {e}")
    
    def _on_save(self):
        """保存修改"""
        try:
            self.result_data = json.loads(self.text_edit.toPlainText())
            self.accept()
        except json.JSONDecodeError as e:
            QMessageBox.warning(self, "格式错误", f"JSON 格式错误: {e}")
    
    def get_result(self) -> dict:
        """获取编辑后的数据"""
        return self.result_data


class ESManagerWidget(QWidget):
    """
    Elasticsearch 管理器主界面
    
    布局:
    +-------------------------------------------+
    | [ES 连接 ▼] [刷新] [添加文档]             |
    +------------------+------------------------+
    | 索引列表          | 文档表格               |
    | [过滤...]         | ID | Source (JSON)     |
    | - index1          +------------------------+
    | - index2          | 分页: [<] 1 [>]        |
    +------------------+------------------------+
    """
    
    def __init__(self, title: str = "Elasticsearch 管理器", parent=None):
        super().__init__(parent)
        
        self.title_text = title
        self.connection_manager = ConnectionManager()
        self.es_worker: ESWorker = None
        self.es_client: ESClient = None
        
        # 状态
        self.current_index: str = ""
        self.current_page: int = 1
        self.page_size: int = 20
        self.total_docs: int = 0
        
        self._setup_ui()
        self._apply_styles()
        self._load_es_connections()
    
    def _setup_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(15)
        
        # 标题
        title = QLabel(f"🔍 {self.title_text}")
        font = QFont()
        font.setPointSize(18)
        font.setBold(True)
        title.setFont(font)
        title.setStyleSheet("color: #cccccc;")
        main_layout.addWidget(title)
        
        # 顶部工具栏
        toolbar = QHBoxLayout()
        
        # 连接选择
        toolbar.addWidget(QLabel("ES 连接:"))
        self.conn_combo = QComboBox()
        self.conn_combo.setMinimumWidth(250)
        self.conn_combo.currentIndexChanged.connect(self._on_connection_changed)
        toolbar.addWidget(self.conn_combo)
        
        # 刷新按钮
        self.refresh_btn = QPushButton("🔄 刷新")
        self.refresh_btn.clicked.connect(self._refresh_indices)
        toolbar.addWidget(self.refresh_btn)
        
        toolbar.addStretch()
        
        # 添加文档按钮
        self.add_doc_btn = QPushButton("➕ 添加文档")
        self.add_doc_btn.setEnabled(False)
        self.add_doc_btn.clicked.connect(self._on_add_doc)
        toolbar.addWidget(self.add_doc_btn)
        
        main_layout.addLayout(toolbar)
        
        # 主分割器
        splitter = QSplitter(Qt.Horizontal)
        
        # ===== 左侧：索引列表 =====
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(0, 0, 0, 0)
        
        # 索引过滤
        self.index_filter = QLineEdit()
        self.index_filter.setPlaceholderText("过滤索引...")
        self.index_filter.textChanged.connect(self._filter_indices)
        left_layout.addWidget(self.index_filter)
        
        # 索引列表
        self.index_list = QListWidget()
        self.index_list.setMinimumWidth(200)
        self.index_list.itemClicked.connect(self._on_index_selected)
        left_layout.addWidget(self.index_list)
        
        # 索引统计
        self.index_stats = QLabel("选择连接加载索引")
        self.index_stats.setStyleSheet("color: #6e6e6e; font-size: 12px;")
        left_layout.addWidget(self.index_stats)
        
        splitter.addWidget(left_widget)
        
        # ===== 右侧：文档表格 =====
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        right_layout.setContentsMargins(0, 0, 0, 0)
        
        # 当前索引显示
        self.current_index_label = QLabel("请选择索引")
        self.current_index_label.setStyleSheet("color: #cccccc; font-weight: bold; padding: 5px;")
        right_layout.addWidget(self.current_index_label)
        
        # 文档表格
        self.doc_table = QTableWidget()
        self.doc_table.setColumnCount(2)
        self.doc_table.setHorizontalHeaderLabels(["ID", "Source (JSON Preview)"])
        self.doc_table.horizontalHeader().setStretchLastSection(True)
        self.doc_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.doc_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.doc_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.doc_table.setAlternatingRowColors(True)
        self.doc_table.doubleClicked.connect(self._on_doc_double_clicked)
        self.doc_table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.doc_table.customContextMenuRequested.connect(self._on_doc_context_menu)
        right_layout.addWidget(self.doc_table)
        
        # 分页控制
        pagination = QHBoxLayout()
        
        self.prev_btn = QPushButton("◀ 上一页")
        self.prev_btn.setEnabled(False)
        self.prev_btn.clicked.connect(self._on_prev_page)
        pagination.addWidget(self.prev_btn)
        
        pagination.addStretch()
        
        self.page_label = QLabel("第 1 页")
        self.page_label.setStyleSheet("color: #969696;")
        pagination.addWidget(self.page_label)
        
        pagination.addStretch()
        
        self.next_btn = QPushButton("下一页 ▶")
        self.next_btn.setEnabled(False)
        self.next_btn.clicked.connect(self._on_next_page)
        pagination.addWidget(self.next_btn)
        
        right_layout.addLayout(pagination)
        
        splitter.addWidget(right_widget)
        splitter.setSizes([250, 650])
        
        main_layout.addWidget(splitter)
        
        # 状态栏
        self.status_label = QLabel("就绪 - 请选择 ES 连接")
        self.status_label.setStyleSheet("color: #969696; padding: 10px; background-color: #252526; border-radius: 4px;")
        main_layout.addWidget(self.status_label)
    
    def _apply_styles(self):
        self.setStyleSheet("""
            QWidget { background-color: #1e1e1e; color: #cccccc; }
            QLineEdit, QComboBox {
                background-color: #3c3c3c;
                border: 1px solid #3c3c3c;
                padding: 6px;
                border-radius: 4px;
            }
            QPushButton {
                background-color: #0e639c;
                color: white;
                padding: 8px 16px;
                border: none;
                border-radius: 4px;
            }
            QPushButton:hover { background-color: #1177bb; }
            QPushButton:disabled { background-color: #3c3c3c; color: #6e6e6e; }
            QListWidget {
                background-color: #252526;
                border: 1px solid #333;
                border-radius: 4px;
                padding: 5px;
            }
            QListWidget::item {
                padding: 8px;
                border-bottom: 1px solid #333;
            }
            QListWidget::item:selected {
                background-color: #094771;
                color: white;
            }
            QListWidget::item:hover {
                background-color: #2a2d2e;
            }
            QTableWidget {
                background-color: #1e1e1e;
                border: 1px solid #333;
                gridline-color: #333;
            }
            QTableWidget::item {
                padding: 6px;
                border-bottom: 1px solid #2d2d2d;
            }
            QTableWidget::item:selected {
                background-color: #094771;
            }
            QHeaderView::section {
                background-color: #2d2d30;
                color: #cccccc;
                padding: 8px;
                border: none;
                border-right: 1px solid #3c3c3c;
                font-weight: bold;
            }
        """)
    
    def _load_es_connections(self):
        """加载所有 ES 连接配置"""
        self.conn_combo.clear()
        self.conn_combo.addItem("-- 选择 ES 连接 --", None)
        
        profiles = self.connection_manager.load_profiles()
        es_profiles = [p for p in profiles if p.get("db_type") == "elasticsearch"]
        
        for profile in es_profiles:
            name = profile.get("name", "未命名")
            host = profile.get("host", "localhost")
            self.conn_combo.addItem(f"{name} ({host})", profile)
        
        if not es_profiles:
            self.status_label.setText("未找到 ES 连接配置，请先创建")
            self.status_label.setStyleSheet("color: #dcdcaa;")
    
    def _on_connection_changed(self, index):
        """切换连接"""
        if index <= 0:
            self.index_list.clear()
            self.doc_table.setRowCount(0)
            self.add_doc_btn.setEnabled(False)
            return
        
        profile = self.conn_combo.itemData(index)
        if not profile:
            return
        
        # 初始化客户端
        try:
            self.es_client = ESClient(
                host=profile.get("host", "localhost"),
                port=profile.get("port", 9200),
                username=profile.get("username", ""),
                password=profile.get("password", "")
            )
            self._refresh_indices()
            self.add_doc_btn.setEnabled(True)
            self.status_label.setText(f"已连接: {profile.get('name', '')}")
            self.status_label.setStyleSheet("color: #4ec9b0;")
        except Exception as e:
            QMessageBox.warning(self, "连接失败", f"无法连接到 ES: {e}")
    
    def _refresh_indices(self):
        """刷新索引列表"""
        if not self.es_client:
            return
        
        self.status_label.setText("加载索引列表...")
        
        # 使用 Worker 异步加载
        profile = self.conn_combo.currentData()
        self.es_worker = ESWorker(profile, parent=self)
        self.es_worker.indices_ready.connect(self._on_indices_loaded)
        self.es_worker.error_occurred.connect(self._on_error)
        self.es_worker.list_indices()
        self.es_worker.start()
    
    def _on_indices_loaded(self, indices: list):
        """索引列表加载完成"""
        self.all_indices = indices
        self._filter_indices()
        
        total = len(indices)
        self.index_stats.setText(f"共 {total} 个索引")
        self.status_label.setText(f"已加载 {total} 个索引")
    
    def _filter_indices(self):
        """过滤索引列表"""
        filter_text = self.index_filter.text().lower()
        self.index_list.clear()
        
        for idx in getattr(self, 'all_indices', []):
            name = idx.get("name", "")
            if filter_text in name.lower():
                item = QListWidgetItem(f"{name}\n  📄 {idx.get('docs_count', 0)} docs | 💾 {idx.get('store_size', '0b')}")
                item.setData(Qt.UserRole, idx)
                # 根据健康状态设置颜色
                health = idx.get("health", "")
                if health == "green":
                    item.setForeground(Qt.green)
                elif health == "yellow":
                    item.setForeground(Qt.yellow)
                elif health == "red":
                    item.setForeground(Qt.red)
                self.index_list.addItem(item)
    
    def _on_index_selected(self, item: QListWidgetItem):
        """选择索引"""
        idx_data = item.data(Qt.UserRole)
        self.current_index = idx_data.get("name", "")
        self.current_page = 1
        
        self.current_index_label.setText(f"索引: {self.current_index}")
        self._load_docs()
    
    def _load_docs(self):
        """加载文档列表"""
        if not self.current_index or not self.es_client:
            return
        
        self.status_label.setText(f"加载文档... 第 {self.current_page} 页")
        
        profile = self.conn_combo.currentData()
        self.es_worker = ESWorker(profile, parent=self)
        self.es_worker.docs_ready.connect(self._on_docs_loaded)
        self.es_worker.error_occurred.connect(self._on_error)
        self.es_worker.search_docs(self.current_index, self.current_page, self.page_size)
        self.es_worker.start()
    
    def _on_docs_loaded(self, result: dict):
        """文档加载完成"""
        hits = result.get("hits", {})
        docs = hits.get("hits", [])
        total = hits.get("total", {}).get("value", 0)
        
        self.total_docs = total
        self._update_table(docs)
        self._update_pagination()
        
        self.status_label.setText(f"索引: {self.current_index} | 共 {total} 条 | 当前第 {self.current_page} 页")
    
    def _update_table(self, docs: list):
        """更新文档表格"""
        self.doc_table.setRowCount(len(docs))
        
        for row, doc in enumerate(docs):
            doc_id = doc.get("_id", "")
            source = doc.get("_source", {})
            
            # ID 列
            id_item = QTableWidgetItem(doc_id)
            id_item.setData(Qt.UserRole, doc)  # 保存完整文档数据
            self.doc_table.setItem(row, 0, id_item)
            
            # Source 列（截断显示）
            source_text = json.dumps(source, ensure_ascii=False)
            if len(source_text) > 100:
                source_text = source_text[:97] + "..."
            self.doc_table.setItem(row, 1, QTableWidgetItem(source_text))
    
    def _update_pagination(self):
        """更新分页按钮状态"""
        self.page_label.setText(f"第 {self.current_page} 页")
        self.prev_btn.setEnabled(self.current_page > 1)
        
        max_page = (self.total_docs + self.page_size - 1) // self.page_size
        self.next_btn.setEnabled(self.current_page < max_page)
    
    def _on_prev_page(self):
        """上一页"""
        if self.current_page > 1:
            self.current_page -= 1
            self._load_docs()
    
    def _on_next_page(self):
        """下一页"""
        max_page = (self.total_docs + self.page_size - 1) // self.page_size
        if self.current_page < max_page:
            self.current_page += 1
            self._load_docs()
    
    def _on_doc_double_clicked(self, index):
        """双击文档查看详情"""
        row = index.row()
        item = self.doc_table.item(row, 0)
        if not item:
            return
        
        doc_data = item.data(Qt.UserRole)
        
        dialog = JsonEditorDialog(doc_data, editable=True, parent=self)
        if dialog.exec() == QDialog.Accepted:
            # 更新文档
            new_data = dialog.get_result()
            if new_data:
                self._update_doc(doc_data.get("_id"), new_data)
    
    def _on_doc_context_menu(self, position):
        """右键菜单"""
        row = self.doc_table.rowAt(position.y())
        if row < 0:
            return
        
        item = self.doc_table.item(row, 0)
        if not item:
            return
        
        doc_data = item.data(Qt.UserRole)
        doc_id = doc_data.get("_id", "")
        
        menu = QMenu(self)
        
        view_action = QAction("👁️ 查看", self)
        view_action.triggered.connect(lambda: self._view_doc(doc_data))
        menu.addAction(view_action)
        
        edit_action = QAction("✏️ 编辑", self)
        edit_action.triggered.connect(lambda: self._edit_doc(doc_data))
        menu.addAction(edit_action)
        
        menu.addSeparator()
        
        delete_action = QAction("🗑️ 删除", self)
        delete_action.triggered.connect(lambda: self._delete_doc(doc_id))
        menu.addAction(delete_action)
        
        menu.exec(self.doc_table.viewport().mapToGlobal(position))
    
    def _view_doc(self, doc_data: dict):
        """查看文档"""
        dialog = JsonEditorDialog(doc_data, editable=False, parent=self)
        dialog.exec()
    
    def _edit_doc(self, doc_data: dict):
        """编辑文档"""
        dialog = JsonEditorDialog(doc_data, editable=True, parent=self)
        if dialog.exec() == QDialog.Accepted:
            new_data = dialog.get_result()
            if new_data:
                self._update_doc(doc_data.get("_id"), new_data)
    
    def _update_doc(self, doc_id: str, data: dict):
        """更新文档"""
        if not self.current_index or not doc_id:
            return
        
        reply = QMessageBox.question(
            self, "确认更新",
            f"确定要更新文档 {doc_id} 吗？",
            QMessageBox.Yes | QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            profile = self.conn_combo.currentData()
            self.es_worker = ESWorker(profile, parent=self)
            self.es_worker.operation_finished.connect(self._on_operation_finished)
            self.es_worker.error_occurred.connect(self._on_error)
            self.es_worker.update_doc(self.current_index, doc_id, data)
            self.es_worker.start()
    
    def _delete_doc(self, doc_id: str):
        """删除文档"""
        if not self.current_index or not doc_id:
            return
        
        reply = QMessageBox.warning(
            self, "⚠️ 确认删除",
            f"确定要删除文档 {doc_id} 吗？\n此操作不可恢复！",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            profile = self.conn_combo.currentData()
            self.es_worker = ESWorker(profile, parent=self)
            self.es_worker.operation_finished.connect(self._on_operation_finished)
            self.es_worker.error_occurred.connect(self._on_error)
            self.es_worker.delete_doc(self.current_index, doc_id)
            self.es_worker.start()
    
    def _on_add_doc(self):
        """添加新文档"""
        if not self.current_index:
            QMessageBox.warning(self, "提示", "请先选择一个索引")
            return
        
        # 创建空文档
        empty_doc = {"_id": "_new", "_source": {}}
        dialog = JsonEditorDialog(empty_doc, editable=True, parent=self)
        
        if dialog.exec() == QDialog.Accepted:
            new_data = dialog.get_result()
            if new_data:
                profile = self.conn_combo.currentData()
                self.es_worker = ESWorker(profile, parent=self)
                self.es_worker.operation_finished.connect(self._on_operation_finished)
                self.es_worker.error_occurred.connect(self._on_error)
                self.es_worker.create_doc(self.current_index, new_data)
                self.es_worker.start()
    
    def _on_operation_finished(self, success: bool, message: str):
        """操作完成回调"""
        if success:
            QMessageBox.information(self, "成功", message)
            self._load_docs()  # 刷新列表
        else:
            QMessageBox.warning(self, "失败", message)
    
    def _on_error(self, error_msg: str):
        """错误处理"""
        self.status_label.setText(f"错误: {error_msg}")
        self.status_label.setStyleSheet("color: #f48771;")
        QMessageBox.critical(self, "错误", error_msg)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    widget = ESManagerWidget()
    widget.resize(900, 600)
    widget.show()
    sys.exit(app.exec())
