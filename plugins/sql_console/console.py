"""
SQL 执行控制台插件 - Phase 5

功能：
- 选择已保存的数据库连接
- 输入并执行 SQL 语句
- 以表格形式展示 SELECT 结果
- 显示非查询语句的影响行数

依赖:
    pip install sqlalchemy pymysql
"""

import sys
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QSplitter,
    QComboBox,
    QTextEdit,
    QPushButton,
    QLabel,
    QMessageBox,
    QTableWidget,
    QTableWidgetItem,
    QGroupBox,
    QFrame,
    QHeaderView,
    QApplication,
    QAbstractItemView
)
from PySide6.QtCore import Qt, QSize
from PySide6.QtGui import QFont, QKeySequence, QShortcut

from core.managers.connection_manager import ConnectionManager
from core.workers.sql_worker import SQLWorker


class SQLConsoleWidget(QWidget):
    """
    SQL 执行控制台插件
    
    布局:
    +------------------------------------------+
    | 配置选择: [选择连接 ▼]                   |
    +------------------------------------------+
    | SQL 输入区 (QTextEdit)                   |
    |                                          |
    |                                          |
    +------------------------------------------+
    | [执行查询 Ctrl+Enter]                    |
    +------------------------------------------+
    | 结果表格 (QTableWidget)                  |
    |                                          |
    +------------------------------------------+
    | 状态栏: 就绪 | 共 X 行 | 耗时 X ms        |
    +------------------------------------------+
    """
    
    def __init__(self, title: str = "SQL 控制台", parent=None):
        super().__init__(parent)
        
        self.title_text = title
        self.connection_manager = ConnectionManager()
        self.sql_worker: SQLWorker = None
        
        self._setup_ui()
        self._apply_styles()
        self._load_connections()
        self._setup_shortcuts()
    
    def _setup_ui(self) -> None:
        """设置界面布局"""
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(15)
        
        # ========== 标题区 ==========
        title_label = QLabel(f"🗄️ {self.title_text}")
        title_font = QFont()
        title_font.setPointSize(18)
        title_font.setBold(True)
        title_label.setFont(title_font)
        title_label.setStyleSheet("color: #cccccc;")
        main_layout.addWidget(title_label)
        
        # ========== 连接选择区 ==========
        conn_group = QGroupBox("数据库连接")
        conn_group.setStyleSheet("""
            QGroupBox {
                color: #cccccc;
                border: 1px solid #333333;
                border-radius: 4px;
                margin-top: 12px;
                padding-top: 12px;
                font-weight: bold;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 12px;
                padding: 0 8px;
            }
        """)
        
        conn_layout = QHBoxLayout(conn_group)
        conn_layout.setSpacing(10)
        conn_layout.setContentsMargins(15, 15, 15, 15)
        
        conn_label = QLabel("选择连接:")
        conn_label.setStyleSheet("color: #969696;")
        conn_layout.addWidget(conn_label)
        
        self.conn_combo = QComboBox()
        self.conn_combo.setMinimumWidth(300)
        self.conn_combo.setPlaceholderText("-- 请先选择一个已保存的数据库连接 --")
        self.conn_combo.currentIndexChanged.connect(self._on_connection_changed)
        conn_layout.addWidget(self.conn_combo)
        
        # 刷新按钮
        self.refresh_btn = QPushButton("🔄")
        self.refresh_btn.setFixedSize(32, 32)
        self.refresh_btn.setToolTip("刷新连接列表")
        self.refresh_btn.clicked.connect(self._load_connections)
        conn_layout.addWidget(self.refresh_btn)
        
        conn_layout.addStretch()
        main_layout.addWidget(conn_group)
        
        # ========== 分割器（输入区 + 结果区） ==========
        self.splitter = QSplitter(Qt.Vertical)
        
        # ---------- 上半部分：SQL 输入 ----------
        input_widget = QWidget()
        input_layout = QVBoxLayout(input_widget)
        input_layout.setContentsMargins(0, 0, 0, 0)
        input_layout.setSpacing(10)
        
        # SQL 输入框标签
        input_label = QLabel("SQL 语句")
        input_label.setStyleSheet("color: #969696; font-weight: bold;")
        input_layout.addWidget(input_label)
        
        # SQL 输入框
        self.sql_input = QTextEdit()
        self.sql_input.setPlaceholderText(
            "在此输入 SQL 语句，支持 Ctrl+Enter 执行...\n\n"
            "示例:\n"
            "  SELECT * FROM users LIMIT 10;\n"
            "  SHOW TABLES;\n"
            "  DESCRIBE users;"
        )
        self.sql_input.setMinimumHeight(150)
        input_layout.addWidget(self.sql_input)
        
        # 执行按钮
        btn_layout = QHBoxLayout()
        self.execute_btn = QPushButton("▶ 执行查询 (Ctrl+Enter)")
        self.execute_btn.setFixedHeight(36)
        self.execute_btn.setCursor(Qt.PointingHandCursor)
        self.execute_btn.clicked.connect(self._on_execute)
        btn_layout.addWidget(self.execute_btn)
        
        # 停止按钮
        self.stop_btn = QPushButton("⏹ 停止")
        self.stop_btn.setFixedHeight(36)
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self._on_stop)
        btn_layout.addWidget(self.stop_btn)
        
        # 清空按钮
        self.clear_btn = QPushButton("🗑 清空结果")
        self.clear_btn.setFixedHeight(36)
        self.clear_btn.clicked.connect(self._on_clear)
        btn_layout.addWidget(self.clear_btn)
        
        btn_layout.addStretch()
        input_layout.addLayout(btn_layout)
        
        self.splitter.addWidget(input_widget)
        
        # ---------- 下半部分：结果展示 ----------
        result_widget = QWidget()
        result_layout = QVBoxLayout(result_widget)
        result_layout.setContentsMargins(0, 0, 0, 0)
        result_layout.setSpacing(10)
        
        # 结果标签
        result_header = QHBoxLayout()
        self.result_label = QLabel("查询结果")
        self.result_label.setStyleSheet("color: #969696; font-weight: bold;")
        result_header.addWidget(self.result_label)
        
        result_header.addStretch()
        result_layout.addLayout(result_header)
        
        # 结果表格
        self.result_table = QTableWidget()
        self.result_table.setColumnCount(0)
        self.result_table.setRowCount(0)
        self.result_table.setAlternatingRowColors(True)
        self.result_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.result_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.result_table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.result_table.horizontalHeader().setStretchLastSection(True)
        self.result_table.horizontalHeader().setDefaultSectionSize(120)
        self.result_table.verticalHeader().setDefaultSectionSize(25)
        result_layout.addWidget(self.result_table)
        
        self.splitter.addWidget(result_widget)
        
        # 设置分割比例（输入区:结果区 = 1:2）
        self.splitter.setSizes([250, 400])
        
        main_layout.addWidget(self.splitter, stretch=1)
        
        # ========== 状态栏 ==========
        status_frame = QFrame()
        status_frame.setStyleSheet("""
            QFrame {
                background-color: #252526;
                border-top: 1px solid #333333;
                border-radius: 4px;
            }
        """)
        status_layout = QHBoxLayout(status_frame)
        status_layout.setContentsMargins(15, 10, 15, 10)
        
        self.status_label = QLabel("就绪 - 请选择数据库连接")
        self.status_label.setStyleSheet("color: #969696;")
        status_layout.addWidget(self.status_label)
        
        status_layout.addStretch()
        
        self.rows_label = QLabel("")
        self.rows_label.setStyleSheet("color: #6e6e6e;")
        status_layout.addWidget(self.rows_label)
        
        main_layout.addWidget(status_frame)
    
    def _apply_styles(self) -> None:
        """应用深色主题样式"""
        # 连接选择下拉框
        self.conn_combo.setStyleSheet("""
            QComboBox {
                background-color: #3c3c3c;
                color: #cccccc;
                border: 1px solid #3c3c3c;
                border-radius: 4px;
                padding: 8px 12px;
                font-size: 13px;
                min-height: 20px;
            }
            QComboBox:focus {
                border: 1px solid #007acc;
            }
            QComboBox::drop-down {
                border: none;
                width: 24px;
            }
            QComboBox QAbstractItemView {
                background-color: #3c3c3c;
                color: #cccccc;
                border: 1px solid #454545;
                selection-background-color: #094771;
            }
        """)
        
        # SQL 输入框
        self.sql_input.setStyleSheet("""
            QTextEdit {
                background-color: #1e1e1e;
                color: #d4d4d4;
                border: 1px solid #333333;
                border-radius: 4px;
                padding: 12px;
                font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
                font-size: 13px;
                selection-background-color: #264f78;
            }
            QTextEdit:focus {
                border: 1px solid #007acc;
            }
        """)
        
        # 执行按钮
        self.execute_btn.setStyleSheet("""
            QPushButton {
                background-color: #0e639c;
                color: #ffffff;
                border: none;
                border-radius: 4px;
                padding: 0 24px;
                font-size: 13px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #1177bb;
            }
            QPushButton:pressed {
                background-color: #094771;
            }
            QPushButton:disabled {
                background-color: #3c3c3c;
                color: #6e6e6e;
            }
        """)
        
        # 停止按钮
        self.stop_btn.setStyleSheet("""
            QPushButton {
                background-color: #c75450;
                color: #ffffff;
                border: none;
                border-radius: 4px;
                padding: 0 20px;
                font-size: 13px;
            }
            QPushButton:hover {
                background-color: #d96864;
            }
            QPushButton:pressed {
                background-color: #a0403d;
            }
            QPushButton:disabled {
                background-color: #3c3c3c;
                color: #6e6e6e;
            }
        """)
        
        # 清空按钮
        self.clear_btn.setStyleSheet("""
            QPushButton {
                background-color: #3c3c3c;
                color: #cccccc;
                border: 1px solid #454545;
                border-radius: 4px;
                padding: 0 16px;
                font-size: 13px;
            }
            QPushButton:hover {
                background-color: #454545;
            }
        """)
        
        # 结果表格
        self.result_table.setStyleSheet("""
            QTableWidget {
                background-color: #1e1e1e;
                border: 1px solid #333333;
                border-radius: 4px;
                gridline-color: #333333;
                font-family: 'Consolas', 'Monaco', monospace;
                font-size: 12px;
            }
            QTableWidget::item {
                padding: 6px 10px;
                color: #d4d4d4;
                border-bottom: 1px solid #2d2d2d;
            }
            QTableWidget::item:selected {
                background-color: #094771;
                color: #ffffff;
            }
            QTableWidget::item:alternate {
                background-color: #252526;
            }
            QHeaderView::section {
                background-color: #2d2d30;
                color: #cccccc;
                padding: 8px 10px;
                border: none;
                border-right: 1px solid #3c3c3c;
                border-bottom: 1px solid #3c3c3c;
                font-weight: bold;
            }
            QHeaderView::section:hover {
                background-color: #3c3c3c;
            }
        """)
    
    def _setup_shortcuts(self) -> None:
        """设置快捷键"""
        # Ctrl+Enter 执行查询
        execute_shortcut = QShortcut(QKeySequence("Ctrl+Return"), self)
        execute_shortcut.activated.connect(self._on_execute)
        
        # Ctrl+Shift+Enter 也可以执行
        execute_shortcut2 = QShortcut(QKeySequence("Ctrl+Enter"), self)
        execute_shortcut2.activated.connect(self._on_execute)
    
    def _load_connections(self) -> None:
        """加载已保存的数据库连接"""
        current_text = self.conn_combo.currentText()
        
        self.conn_combo.clear()
        self.conn_combo.addItem("-- 请先选择一个已保存的数据库连接 --", None)
        
        try:
            profiles = self.connection_manager.load_profiles()
            
            for profile in profiles:
                name = profile.get("name", "未命名")
                db_type = profile.get("db_type", "unknown")
                host = profile.get("host", "")
                display = f"{name} ({db_type}://{host})"
                self.conn_combo.addItem(display, profile)
            
            # 恢复之前的选择
            if current_text:
                index = self.conn_combo.findText(current_text)
                if index >= 0:
                    self.conn_combo.setCurrentIndex(index)
            
            count = len(profiles)
            self.status_label.setText(f"已加载 {count} 个连接配置")
            
        except Exception as e:
            self.status_label.setText(f"加载连接失败: {e}")
    
    def _on_connection_changed(self, index: int) -> None:
        """连接选择改变时的处理"""
        if index <= 0:
            self.status_label.setText("就绪 - 请选择数据库连接")
            return
        
        profile = self.conn_combo.itemData(index)
        if profile:
            name = profile.get("name", "未命名")
            db_type = profile.get("db_type", "")
            self.status_label.setText(f"已选择: {name} ({db_type})")
    
    def _on_execute(self) -> None:
        """执行 SQL 查询"""
        # 检查连接选择
        conn_index = self.conn_combo.currentIndex()
        if conn_index <= 0:
            QMessageBox.warning(self, "未选择连接", "请先选择一个数据库连接")
            return
        
        # 获取 SQL 语句
        sql_text = self.sql_input.toPlainText().strip()
        if not sql_text:
            QMessageBox.warning(self, "空 SQL", "请输入 SQL 语句")
            return
        
        # 获取连接配置
        profile = self.conn_combo.itemData(conn_index)
        if not profile:
            QMessageBox.warning(self, "配置错误", "无法获取连接配置")
            return
        
        # 如果有正在执行的查询，先停止
        if self.sql_worker and self.sql_worker.is_running():
            self.sql_worker.stop()
        
        # 清空之前的结果
        self._clear_results()
        
        # 更新 UI 状态
        self._set_executing_state(True)
        self.status_label.setText("正在执行 SQL...")
        self.status_label.setStyleSheet("color: #569cd6;")
        
        # 创建并启动 SQL 执行线程
        self.sql_worker = SQLWorker(profile, sql_text, parent=self)
        self.sql_worker.select_result_signal.connect(self._on_select_result)
        self.sql_worker.execute_result_signal.connect(self._on_execute_result)
        self.sql_worker.error_signal.connect(self._on_error)
        self.sql_worker.finished_signal.connect(lambda: self._set_executing_state(False))
        
        self.sql_worker.start()
    
    def _on_stop(self) -> None:
        """停止执行"""
        if self.sql_worker and self.sql_worker.is_running():
            self.sql_worker.stop()
            self.status_label.setText("已停止")
            self._set_executing_state(False)
    
    def _on_clear(self) -> None:
        """清空结果"""
        self._clear_results()
        self.status_label.setText("就绪")
        self.status_label.setStyleSheet("color: #969696;")
    
    def _clear_results(self) -> None:
        """清空结果表格"""
        self.result_table.clear()
        self.result_table.setColumnCount(0)
        self.result_table.setRowCount(0)
        self.rows_label.setText("")
    
    def _on_select_result(self, headers: list, rows: list) -> None:
        """
        处理 SELECT 查询结果
        
        Args:
            headers: 表头列表
            rows: 数据行列表（每行是一个字符串列表）
        """
        # 设置表格结构
        self.result_table.setColumnCount(len(headers))
        self.result_table.setRowCount(len(rows))
        self.result_table.setHorizontalHeaderLabels(headers)
        
        # 填充数据
        for row_idx, row_data in enumerate(rows):
            for col_idx, cell_value in enumerate(row_data):
                item = QTableWidgetItem(str(cell_value))
                item.setFlags(item.flags() & ~Qt.ItemIsEditable)  # 只读
                self.result_table.setItem(row_idx, col_idx, item)
        
        # 调整列宽
        self.result_table.resizeColumnsToContents()
        
        # 更新状态
        row_count = len(rows)
        self.status_label.setText(f"查询成功")
        self.status_label.setStyleSheet("color: #4ec9b0;")
        self.rows_label.setText(f"共 {row_count} 行数据 | {len(headers)} 列")
        
        self.result_label.setText(f"查询结果 (SELECT)")
    
    def _on_execute_result(self, rowcount: int, message: str) -> None:
        """
        处理非查询语句结果
        
        Args:
            rowcount: 影响行数
            message: 消息文本
        """
        self.status_label.setText(message)
        self.status_label.setStyleSheet("color: #4ec9b0;")
        self.rows_label.setText(f"")
        
        self.result_label.setText("执行结果 (非查询)")
        
        # 弹窗提示
        QMessageBox.information(self, "执行成功", message)
    
    def _on_error(self, error_msg: str) -> None:
        """处理错误"""
        self.status_label.setText("执行失败")
        self.status_label.setStyleSheet("color: #f48771;")
        
        QMessageBox.critical(self, "SQL 执行错误", error_msg)
    
    def _set_executing_state(self, executing: bool) -> None:
        """设置执行状态"""
        self.execute_btn.setEnabled(not executing)
        self.stop_btn.setEnabled(executing)
        self.conn_combo.setEnabled(not executing)
        
        if executing:
            self.execute_btn.setText("⏳ 执行中...")
        else:
            self.execute_btn.setText("▶ 执行查询 (Ctrl+Enter)")
        
        QApplication.processEvents()
    
    def closeEvent(self, event) -> None:
        """关闭时确保线程停止"""
        if self.sql_worker and self.sql_worker.is_running():
            self.sql_worker.stop()
        event.accept()


# 插件入口点
if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # 应用暗色主题
    app.setStyleSheet("""
        QWidget {
            background-color: #1e1e1e;
            color: #cccccc;
        }
    """)
    
    console = SQLConsoleWidget(title="SQL 控制台")
    console.resize(900, 700)
    console.show()
    
    sys.exit(app.exec())
