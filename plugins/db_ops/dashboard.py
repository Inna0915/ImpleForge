"""
数据库运维仪表盘插件 - Phase 5 (修订版)

功能：
- 选择已保存的数据库连接
- 根据数据库类型动态显示支持的运维操作按钮
- 支持 Oracle 数据泵导入导出
- 显示操作结果

依赖安装:
    # Oracle 支持 (12c+ thin mode)
    pip install oracledb
    
    # SQL Server 支持
    pip install pymssql
    
    # MongoDB 支持
    pip install pymongo
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
    QGridLayout,
    QComboBox,
    QPushButton,
    QLabel,
    QMessageBox,
    QTextEdit,
    QGroupBox,
    QFrame,
    QLineEdit,
    QFileDialog,
    QTableWidget,
    QTableWidgetItem,
    QAbstractItemView,
    QSplitter,
    QApplication,
    QSizePolicy
)
from PySide6.QtCore import Qt, QSize
from PySide6.QtGui import QFont, QKeySequence, QShortcut

from core.managers.connection_manager import ConnectionManager
from core.strategies.db_ops import (
    get_supported_operations,
    is_capability_supported,
    get_db_capabilities
)


class DatabaseOpsWidget(QWidget):
    """
    数据库运维仪表盘
    
    布局:
    +------------------------------------------+
    | 连接选择: [连接 ▼] [🔗 连接/刷新]        |
    +------------------------------------------+
    | 操作按钮区 (根据数据库类型动态渲染)       |
    | [🔍 查看死锁] [📜 Binlog] [👥 进程列表]  |
    +------------------------------------------+
    | Oracle 数据泵区 (仅 Oracle 显示)          |
    | 路径: [/path/to/dmp ▼] [浏览]            |
    | [📤 Expdp 导出] [📥 Impdp 导入]          |
    +------------------------------------------+
    | 结果显示区 (QTextEdit / QTableWidget)    |
    +------------------------------------------+
    """
    
    def __init__(self, title: str = "数据库运维仪表盘", parent=None):
        super().__init__(parent)
        
        self.title_text = title
        self.connection_manager = ConnectionManager()
        self.current_profile: dict = None
        self.current_db_type: str = ""
        
        self._setup_ui()
        self._apply_styles()
        self._load_connections()
    
    def _setup_ui(self) -> None:
        """设置界面布局"""
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(15)
        
        # ========== 标题区 ==========
        title_label = QLabel(f"🖥️ {self.title_text}")
        title_font = QFont()
        title_font.setPointSize(18)
        title_font.setBold(True)
        title_label.setFont(title_font)
        title_label.setStyleSheet("color: #cccccc;")
        main_layout.addWidget(title_label)
        
        subtitle = QLabel("选择数据库连接，执行运维操作")
        subtitle.setStyleSheet("color: #969696; margin-bottom: 10px;")
        main_layout.addWidget(subtitle)
        
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
        self.conn_combo.setPlaceholderText("-- 请选择数据库连接 --")
        self.conn_combo.currentIndexChanged.connect(self._on_connection_changed)
        conn_layout.addWidget(self.conn_combo)
        
        # 连接/刷新按钮
        self.connect_btn = QPushButton("🔗 连接/刷新")
        self.connect_btn.setFixedHeight(36)
        self.connect_btn.setCursor(Qt.PointingHandCursor)
        self.connect_btn.clicked.connect(self._on_connect)
        conn_layout.addWidget(self.connect_btn)
        
        # 刷新列表按钮
        self.refresh_list_btn = QPushButton("🔄")
        self.refresh_list_btn.setFixedSize(36, 36)
        self.refresh_list_btn.setToolTip("刷新连接列表")
        self.refresh_list_btn.clicked.connect(self._load_connections)
        conn_layout.addWidget(self.refresh_list_btn)
        
        conn_layout.addStretch()
        main_layout.addWidget(conn_group)
        
        # ========== 分割器（操作区 + 结果区） ==========
        self.splitter = QSplitter(Qt.Vertical)
        
        # ---------- 操作区 ----------
        ops_widget = QWidget()
        ops_layout = QVBoxLayout(ops_widget)
        ops_layout.setContentsMargins(0, 0, 0, 0)
        ops_layout.setSpacing(15)
        
        # 操作按钮区
        self.ops_group = QGroupBox("运维操作")
        self.ops_group.setStyleSheet(conn_group.styleSheet())
        self.ops_layout = QGridLayout(self.ops_group)
        self.ops_layout.setSpacing(10)
        self.ops_layout.setContentsMargins(15, 20, 15, 15)
        
        # 默认提示
        self.ops_hint = QLabel("请先选择数据库连接")
        self.ops_hint.setStyleSheet("color: #6e6e6e; padding: 30px;")
        self.ops_hint.setAlignment(Qt.AlignCenter)
        self.ops_layout.addWidget(self.ops_hint, 0, 0, 1, 4)
        
        ops_layout.addWidget(self.ops_group)
        
        # Oracle 数据泵区（默认隐藏）
        self.pump_group = QGroupBox("Oracle 数据泵 (Data Pump)")
        self.pump_group.setStyleSheet(conn_group.styleSheet())
        self.pump_group.setVisible(False)
        
        pump_layout = QVBoxLayout(self.pump_group)
        pump_layout.setSpacing(10)
        pump_layout.setContentsMargins(15, 20, 15, 15)
        
        # 路径选择
        path_layout = QHBoxLayout()
        path_label = QLabel("DMP 文件路径:")
        path_label.setStyleSheet("color: #969696;")
        path_layout.addWidget(path_label)
        
        self.pump_path_input = QLineEdit()
        self.pump_path_input.setPlaceholderText("选择 .dmp 文件路径...")
        path_layout.addWidget(self.pump_path_input)
        
        self.browse_btn = QPushButton("浏览...")
        self.browse_btn.setFixedWidth(80)
        self.browse_btn.clicked.connect(self._on_browse_dmp)
        path_layout.addWidget(self.browse_btn)
        
        pump_layout.addLayout(path_layout)
        
        # 操作按钮
        pump_btn_layout = QHBoxLayout()
        
        self.expdp_btn = QPushButton("📤 Expdp 导出")
        self.expdp_btn.setFixedHeight(36)
        self.expdp_btn.setToolTip("执行 Oracle 数据泵导出")
        self.expdp_btn.clicked.connect(lambda: self._on_pump_operation("expdp"))
        pump_btn_layout.addWidget(self.expdp_btn)
        
        self.impdp_btn = QPushButton("📥 Impdp 导入")
        self.impdp_btn.setFixedHeight(36)
        self.impdp_btn.setToolTip("执行 Oracle 数据泵导入")
        self.impdp_btn.clicked.connect(lambda: self._on_pump_operation("impdp"))
        pump_btn_layout.addWidget(self.impdp_btn)
        
        pump_btn_layout.addStretch()
        pump_layout.addLayout(pump_btn_layout)
        
        ops_layout.addWidget(self.pump_group)
        ops_layout.addStretch()
        
        self.splitter.addWidget(ops_widget)
        
        # ---------- 结果区 ----------
        result_widget = QWidget()
        result_layout = QVBoxLayout(result_widget)
        result_layout.setContentsMargins(0, 0, 0, 0)
        result_layout.setSpacing(10)
        
        # 结果标签
        result_header = QHBoxLayout()
        self.result_label = QLabel("操作结果")
        self.result_label.setStyleSheet("color: #969696; font-weight: bold;")
        result_header.addWidget(self.result_label)
        
        # 清除结果按钮
        self.clear_result_btn = QPushButton("🗑 清除")
        self.clear_result_btn.setFixedHeight(28)
        self.clear_result_btn.clicked.connect(self._clear_results)
        result_header.addWidget(self.clear_result_btn)
        
        result_header.addStretch()
        result_layout.addLayout(result_header)
        
        # 结果显示（文本）
        self.result_text = QTextEdit()
        self.result_text.setReadOnly(True)
        self.result_text.setPlaceholderText("操作结果将在此显示...")
        result_layout.addWidget(self.result_text)
        
        # 结果表格（查询结果）
        self.result_table = QTableWidget()
        self.result_table.setColumnCount(0)
        self.result_table.setRowCount(0)
        self.result_table.setAlternatingRowColors(True)
        self.result_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.result_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.result_table.horizontalHeader().setStretchLastSection(True)
        self.result_table.setVisible(False)
        result_layout.addWidget(self.result_table)
        
        self.splitter.addWidget(result_widget)
        
        # 设置分割比例
        self.splitter.setSizes([350, 350])
        
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
        
        self.db_type_label = QLabel("")
        self.db_type_label.setStyleSheet("color: #6e6e6e;")
        status_layout.addWidget(self.db_type_label)
        
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
        
        # 按钮样式
        self.connect_btn.setStyleSheet("""
            QPushButton {
                background-color: #0e639c;
                color: #ffffff;
                border: none;
                border-radius: 4px;
                padding: 0 20px;
                font-size: 13px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #1177bb;
            }
            QPushButton:pressed {
                background-color: #094771;
            }
        """)
        
        # 操作按钮通用样式
        op_btn_style = """
            QPushButton {
                background-color: #2d2d30;
                color: #cccccc;
                border: 1px solid #3c3c3c;
                border-radius: 4px;
                padding: 12px 20px;
                font-size: 13px;
                min-width: 140px;
                text-align: left;
            }
            QPushButton:hover {
                background-color: #3c3c3c;
                border-color: #505050;
            }
            QPushButton:pressed {
                background-color: #094771;
                border-color: #007acc;
            }
        """
        self.browse_btn.setStyleSheet(op_btn_style)
        self.expdp_btn.setStyleSheet(op_btn_style)
        self.impdp_btn.setStyleSheet(op_btn_style)
        self.clear_result_btn.setStyleSheet(op_btn_style)
        
        # 数据泵输入框
        self.pump_path_input.setStyleSheet("""
            QLineEdit {
                background-color: #3c3c3c;
                color: #cccccc;
                border: 1px solid #3c3c3c;
                border-radius: 4px;
                padding: 8px 12px;
                font-size: 13px;
            }
            QLineEdit:focus {
                border: 1px solid #007acc;
            }
        """)
        
        # 结果显示区
        self.result_text.setStyleSheet("""
            QTextEdit {
                background-color: #1e1e1e;
                color: #d4d4d4;
                border: 1px solid #333333;
                border-radius: 4px;
                padding: 12px;
                font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
                font-size: 12px;
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
        """)
    
    def _load_connections(self) -> None:
        """加载已保存的数据库连接"""
        current_text = self.conn_combo.currentText()
        
        self.conn_combo.clear()
        self.conn_combo.addItem("-- 请选择数据库连接 --", None)
        
        try:
            profiles = self.connection_manager.load_profiles()
            
            for profile in profiles:
                name = profile.get("name", "未命名")
                db_type = profile.get("db_type", "unknown")
                host = profile.get("host", "")
                display = f"{name} [{db_type}]"
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
        """连接选择改变"""
        if index <= 0:
            self._reset_ui()
            return
        
        profile = self.conn_combo.itemData(index)
        if profile:
            db_type = profile.get("db_type", "unknown")
            self.db_type_label.setText(f"类型: {db_type}")
    
    def _on_connect(self) -> None:
        """连接/刷新按钮点击"""
        index = self.conn_combo.currentIndex()
        if index <= 0:
            QMessageBox.warning(self, "未选择连接", "请先选择一个数据库连接")
            return
        
        profile = self.conn_combo.itemData(index)
        if not profile:
            return
        
        self.current_profile = profile
        self.current_db_type = profile.get("db_type", "unknown")
        
        # 更新 UI
        self._update_operation_buttons()
        self._update_oracle_pump_visibility()
        
        db_type = self.current_db_type
        name = profile.get("name", "未命名")
        
        self.status_label.setText(f"已连接: {name} ({db_type})")
        self.status_label.setStyleSheet("color: #4ec9b0;")
        
        # 显示连接信息
        self._show_connection_info(profile)
    
    def _update_operation_buttons(self) -> None:
        """根据数据库类型更新操作按钮"""
        # 清除现有按钮
        while self.ops_layout.count():
            item = self.ops_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        
        if not self.current_db_type:
            self.ops_hint = QLabel("请先选择数据库连接")
            self.ops_hint.setStyleSheet("color: #6e6e6e; padding: 30px;")
            self.ops_hint.setAlignment(Qt.AlignCenter)
            self.ops_layout.addWidget(self.ops_hint, 0, 0, 1, 4)
            return
        
        # 获取支持的操作
        operations = get_supported_operations(self.current_db_type)
        
        if not operations:
            self.ops_hint = QLabel(f"数据库类型 '{self.current_db_type}' 暂无支持的操作")
            self.ops_hint.setStyleSheet("color: #dcdcaa; padding: 30px;")
            self.ops_hint.setAlignment(Qt.AlignCenter)
            self.ops_layout.addWidget(self.ops_hint, 0, 0, 1, 4)
            return
        
        # 创建操作按钮
        row, col = 0, 0
        max_cols = 4
        
        for op in operations:
            btn = QPushButton(op["label"])
            btn.setToolTip(f"{op['tooltip']}\n快捷键: {op.get('shortcut', '无')}")
            btn.setFixedHeight(45)
            btn.setCursor(Qt.PointingHandCursor)
            btn.clicked.connect(lambda checked, op_id=op["id"]: self._on_operation_click(op_id))
            
            # 应用样式
            btn.setStyleSheet("""
                QPushButton {
                    background-color: #2d2d30;
                    color: #cccccc;
                    border: 1px solid #3c3c3c;
                    border-radius: 4px;
                    padding: 10px 15px;
                    font-size: 13px;
                    text-align: center;
                }
                QPushButton:hover {
                    background-color: #3c3c3c;
                    border-color: #0e639c;
                }
                QPushButton:pressed {
                    background-color: #0e639c;
                    color: #ffffff;
                }
            """)
            
            self.ops_layout.addWidget(btn, row, col)
            
            col += 1
            if col >= max_cols:
                col = 0
                row += 1
        
        # 添加弹性空间
        self.ops_layout.setRowStretch(row + 1, 1)
    
    def _update_oracle_pump_visibility(self) -> None:
        """更新 Oracle 数据泵区域可见性"""
        if not self.current_db_type:
            self.pump_group.setVisible(False)
            return
        
        # 仅 Oracle 显示数据泵区域
        is_oracle = self.current_db_type.lower() == "oracle"
        self.pump_group.setVisible(is_oracle)
        
        if is_oracle:
            self.result_label.setText("操作结果 / 数据泵日志")
    
    def _on_operation_click(self, operation_id: str) -> None:
        """操作按钮点击"""
        if not self.current_profile:
            return
        
        # 模拟执行操作
        self._log_message(f"执行操作: {operation_id}")
        self._log_message(f"数据库类型: {self.current_db_type}")
        self._log_message(f"连接: {self.current_profile.get('name', '')}")
        
        # 根据操作类型显示不同消息
        messages = {
            "deadlock": "正在查询死锁信息...\n（后续 Phase 将实现具体 SQL 执行）",
            "binlog": "正在获取 Binlog 状态...\n（后续 Phase 将实现具体 SQL 执行）",
            "processlist": "正在获取进程列表...\n（后续 Phase 将实现具体 SQL 执行）",
            "replication": "正在查询复制状态...\n（后续 Phase 将实现具体 SQL 执行）",
            "slow_query": "正在分析慢查询...\n（后续 Phase 将实现具体 SQL 执行）",
            "table_stats": "正在获取表统计信息...\n（后续 Phase 将实现具体 SQL 执行）",
            "kill_session": "正在获取会话列表（请选择要终止的会话）...\n（后续 Phase 将实现具体 SQL 执行）",
            "awr": "正在生成 AWR 报告...\n（后续 Phase 将实现具体 SQL 执行）",
            "tablespace": "正在查询表空间使用情况...\n（后续 Phase 将实现具体 SQL 执行）",
            "oplog": "正在查询 Oplog 状态...\n（后续 Phase 将实现具体 SQL 执行）",
            "replica_status": "正在查询副本集状态...\n（后续 Phase 将实现具体 SQL 执行）",
            "dmv": "正在查询 DMV...\n（后续 Phase 将实现具体 SQL 执行）",
        }
        
        msg = messages.get(operation_id, f"操作 '{operation_id}' 执行中...")
        self._log_message(msg)
        
        # 实际执行时，这里会调用具体的 SQL 执行逻辑
        QMessageBox.information(
            self,
            "操作执行",
            f"操作 '{operation_id}' 已触发\n\n"
            f"数据库: {self.current_profile.get('name', '')}\n"
            f"类型: {self.current_db_type}\n\n"
            f"（具体 SQL 执行逻辑将在后续 Phase 实现）"
        )
    
    def _on_browse_dmp(self) -> None:
        """浏览 DMP 文件"""
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "选择 DMP 文件",
            "",
            "Oracle Dump Files (*.dmp);;All Files (*)",
            options=QFileDialog.DontConfirmOverwrite
        )
        
        if file_path:
            self.pump_path_input.setText(file_path)
    
    def _on_pump_operation(self, operation: str) -> None:
        """数据泵操作"""
        dmp_path = self.pump_path_input.text().strip()
        
        if not dmp_path:
            QMessageBox.warning(self, "路径为空", "请选择 DMP 文件路径")
            return
        
        if not self.current_profile or self.current_db_type != "oracle":
            QMessageBox.warning(self, "不支持", "数据泵仅支持 Oracle 数据库")
            return
        
        self._log_message(f"执行数据泵操作: {operation}")
        self._log_message(f"DMP 路径: {dmp_path}")
        self._log_message(f"目标数据库: {self.current_profile.get('name', '')}")
        
        op_name = "导出 (Expdp)" if operation == "expdp" else "导入 (Impdp)"
        
        QMessageBox.information(
            self,
            f"Oracle 数据泵 - {op_name}",
            f"操作: {op_name}\n"
            f"DMP 文件: {dmp_path}\n"
            f"目标数据库: {self.current_profile.get('name', '')}\n\n"
            f"（具体数据泵执行逻辑将在后续 Phase 实现）"
        )
    
    def _show_connection_info(self, profile: dict) -> None:
        """显示连接信息"""
        info = f"""
连接信息:
  名称: {profile.get('name', 'N/A')}
  类型: {profile.get('db_type', 'N/A')}
  主机: {profile.get('host', 'N/A')}:{profile.get('port', 'N/A')}
  数据库: {profile.get('database', 'N/A')}
  用户名: {profile.get('username', 'N/A')}

支持的操作:
"""
        # 添加支持的操作列表
        caps = get_db_capabilities(profile.get('db_type', ''))
        supported = [k for k, v in caps.items() if v]
        if supported:
            for op in supported:
                info += f"  - {op}\n"
        else:
            info += "  （暂无支持的操作）\n"
        
        self._log_message(info)
    
    def _log_message(self, message: str) -> None:
        """添加日志消息"""
        from datetime import datetime
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.result_text.append(f"[{timestamp}] {message}")
        
        # 滚动到底部
        scrollbar = self.result_text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
    
    def _clear_results(self) -> None:
        """清除结果"""
        self.result_text.clear()
        self.result_table.clear()
        self.result_table.setRowCount(0)
        self.result_table.setColumnCount(0)
    
    def _reset_ui(self) -> None:
        """重置 UI"""
        self.current_profile = None
        self.current_db_type = ""
        self.db_type_label.setText("")
        self.status_label.setText("就绪 - 请选择数据库连接")
        self.status_label.setStyleSheet("color: #969696;")
        
        # 清除按钮
        while self.ops_layout.count():
            item = self.ops_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        
        self.ops_hint = QLabel("请先选择数据库连接")
        self.ops_hint.setStyleSheet("color: #6e6e6e; padding: 30px;")
        self.ops_hint.setAlignment(Qt.AlignCenter)
        self.ops_layout.addWidget(self.ops_hint, 0, 0, 1, 4)
        
        self.pump_group.setVisible(False)


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
    
    dashboard = DatabaseOpsWidget(title="数据库运维仪表盘")
    dashboard.resize(900, 700)
    dashboard.show()
    
    sys.exit(app.exec())
