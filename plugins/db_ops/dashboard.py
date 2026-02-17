"""
数据库运维仪表盘插件 - Phase 6 (执行版)

功能：
- 选择已保存的数据库连接
- 根据数据库类型动态显示支持的运维操作按钮
- 真实执行 SQL 查询并显示结果
- 支持 Oracle 数据泵导入导出

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
    QStackedWidget,
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
from core.strategies.sql_registry import SQLRegistry
from core.workers.db_ops_worker import DBOpsWorker
from core.workers.datapump_worker import DataPumpWorker


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
        
        # 工作线程
        self.db_worker: DBOpsWorker = None
        self.datapump_worker: DataPumpWorker = None
        
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
        pump_layout.setSpacing(12)
        pump_layout.setContentsMargins(15, 20, 15, 15)
        
        # 说明标签
        pump_hint = QLabel("⚠️ 数据泵在服务器端执行，文件保存在服务器指定目录中")
        pump_hint.setStyleSheet("color: #dcdcaa; font-size: 11px;")
        pump_layout.addWidget(pump_hint)
        
        # Directory 名称（服务器端目录对象）
        dir_layout = QHBoxLayout()
        dir_label = QLabel("Directory 名称:")
        dir_label.setStyleSheet("color: #969696;")
        dir_label.setFixedWidth(100)
        dir_layout.addWidget(dir_label)
        
        self.pump_dir_input = QLineEdit("DATA_PUMP_DIR")
        self.pump_dir_input.setToolTip("Oracle 服务器端目录对象名称，默认 DATA_PUMP_DIR")
        dir_layout.addWidget(self.pump_dir_input)
        
        dir_info_btn = QPushButton("?")
        dir_info_btn.setFixedSize(28, 28)
        dir_info_btn.setToolTip("需要在服务器上预先创建目录对象:\nCREATE DIRECTORY DATA_PUMP_DIR AS '/path/to/dir';")
        dir_layout.addWidget(dir_info_btn)
        
        pump_layout.addLayout(dir_layout)
        
        # DMP 文件名（仅文件名，不含路径）
        path_layout = QHBoxLayout()
        path_label = QLabel("DMP 文件名:")
        path_label.setStyleSheet("color: #969696;")
        path_label.setFixedWidth(100)
        path_layout.addWidget(path_label)
        
        self.pump_file_input = QLineEdit()
        self.pump_file_input.setPlaceholderText("export.dmp (仅文件名，不含路径)")
        path_layout.addWidget(self.pump_file_input)
        
        # 添加获取文件名按钮
        self.get_filename_btn = QPushButton("📁")
        self.get_filename_btn.setFixedSize(32, 32)
        self.get_filename_btn.setToolTip("从本地选择参考文件名（仅提取文件名）")
        self.get_filename_btn.clicked.connect(self._on_select_dmp_filename)
        path_layout.addWidget(self.get_filename_btn)
        
        pump_layout.addLayout(path_layout)
        
        # 操作按钮
        pump_btn_layout = QHBoxLayout()
        
        self.expdp_btn = QPushButton("📤 Expdp 导出")
        self.expdp_btn.setFixedHeight(38)
        self.expdp_btn.setToolTip("执行 Oracle 数据泵导出 (expdp)")
        self.expdp_btn.clicked.connect(self._on_expdp_click)
        pump_btn_layout.addWidget(self.expdp_btn)
        
        self.impdp_btn = QPushButton("📥 Impdp 导入")
        self.impdp_btn.setFixedHeight(38)
        self.impdp_btn.setToolTip("执行 Oracle 数据泵导入 (impdp)⚠️ 将覆盖现有数据")
        self.impdp_btn.setStyleSheet("""
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
        self.impdp_btn.clicked.connect(self._on_impdp_click)
        pump_btn_layout.addWidget(self.impdp_btn)
        
        pump_btn_layout.addStretch()
        pump_layout.addLayout(pump_btn_layout)
        
        ops_layout.addWidget(self.pump_group)
        ops_layout.addStretch()
        
        self.splitter.addWidget(ops_widget)
        
        # ---------- 结果区 (使用 QStackedWidget) ----------
        result_widget = QWidget()
        result_layout = QVBoxLayout(result_widget)
        result_layout.setContentsMargins(0, 0, 0, 0)
        result_layout.setSpacing(10)
        
        # 结果标签和工具栏
        result_header = QHBoxLayout()
        self.result_label = QLabel("操作结果")
        self.result_label.setStyleSheet("color: #969696; font-weight: bold;")
        result_header.addWidget(self.result_label)
        
        # 显示模式标签
        self.result_mode_label = QLabel("[文本模式]")
        self.result_mode_label.setStyleSheet("color: #569cd6; font-size: 11px;")
        self.result_mode_label.setVisible(False)
        result_header.addWidget(self.result_mode_label)
        
        result_header.addStretch()
        
        # 清除结果按钮
        self.clear_result_btn = QPushButton("🗑 清除")
        self.clear_result_btn.setFixedHeight(28)
        self.clear_result_btn.clicked.connect(self._clear_results)
        result_header.addWidget(self.clear_result_btn)
        
        result_layout.addLayout(result_header)
        
        # QStackedWidget 用于切换文本/表格显示
        self.result_stack = QStackedWidget()
        
        # Page 0: 文本结果显示
        self.result_text = QTextEdit()
        self.result_text.setReadOnly(True)
        self.result_text.setPlaceholderText("操作结果将在此显示...\n\n点击上方运维按钮执行查询")
        self.result_stack.addWidget(self.result_text)
        
        # Page 1: 表格结果显示
        self.result_table = QTableWidget()
        self.result_table.setColumnCount(0)
        self.result_table.setRowCount(0)
        self.result_table.setAlternatingRowColors(True)
        self.result_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.result_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.result_table.horizontalHeader().setStretchLastSection(True)
        self.result_table.horizontalHeader().setDefaultSectionSize(120)
        self.result_table.verticalHeader().setDefaultSectionSize(25)
        self.result_stack.addWidget(self.result_table)
        
        result_layout.addWidget(self.result_stack)
        
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
        self.get_filename_btn.setStyleSheet(op_btn_style)
        self.expdp_btn.setStyleSheet(op_btn_style)
        self.impdp_btn.setStyleSheet(op_btn_style)
        self.clear_result_btn.setStyleSheet(op_btn_style)
        
        # 数据泵输入框
        self.pump_file_input.setStyleSheet("""
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
        
        # QStackedWidget 无特殊样式
        
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
        """操作按钮点击 - 执行实际 SQL 查询"""
        if not self.current_profile:
            QMessageBox.warning(self, "未选择连接", "请先选择并连接数据库")
            return
        
        # 从 SQL 注册表获取 SQL
        sql_def = SQLRegistry.get_sql(self.current_db_type, operation_id)
        
        if not sql_def:
            QMessageBox.warning(
                self, 
                "不支持的操作", 
                f"数据库类型 '{self.current_db_type}' 不支持操作 '{operation_id}'"
            )
            return
        
        sql_text = sql_def.get("sql", "")
        result_type = sql_def.get("result_type", "table")
        timeout = sql_def.get("timeout", 10)
        description = sql_def.get("description", "")
        
        # 停止之前的查询
        if self.db_worker and self.db_worker.is_running():
            self.db_worker.stop()
        
        # 更新状态
        self._set_executing_state(True)
        self.status_label.setText(f"正在执行: {description}...")
        self.status_label.setStyleSheet("color: #569cd6;")
        
        # 显示执行信息
        self._switch_result_mode(result_type)
        self._log_message(f"[{self.current_db_type}] {description}")
        self._log_message(f"操作: {operation_id}")
        
        # 创建并启动工作线程
        self.db_worker = DBOpsWorker(
            db_profile=self.current_profile,
            operation=operation_id,
            sql_text=sql_text,
            result_type=result_type,
            timeout=timeout,
            parent=self
        )
        
        self.db_worker.result_signal.connect(
            lambda status, data_type, content, meta: self._on_query_success(
                status, data_type, content, meta, description
            )
        )
        self.db_worker.error_signal.connect(self._on_query_error)
        self.db_worker.finished_signal.connect(lambda: self._set_executing_state(False))
        
        self.db_worker.start()
    
    def _on_select_dmp_filename(self) -> None:
        """选择 DMP 文件名（仅从本地路径提取文件名作为参考）"""
        file_path, _ = QFileDialog.getSaveFileName(
            self,
            "选择 DMP 文件名参考",
            "",
            "Oracle Dump Files (*.dmp);;All Files (*)",
            options=QFileDialog.DontConfirmOverwrite
        )
        
        if file_path:
            # 仅提取文件名，不包含路径
            filename = Path(file_path).name
            # 确保扩展名为 .dmp
            if not filename.lower().endswith('.dmp'):
                filename += '.dmp'
            self.pump_file_input.setText(filename)
    
    def _on_expdp_click(self) -> None:
        """Expdp 导出按钮点击"""
        self._execute_datapump("expdp", confirm=False)
    
    def _on_impdp_click(self) -> None:
        """Impdp 导入按钮点击"""
        # 二次确认
        reply = QMessageBox.warning(
            self,
            "⚠️ 危险操作确认",
            "导入操作 (Impdp) 可能会:\n"
            "- 覆盖现有表数据\n"
            "- 删除已有对象并重建\n"
            "- 导入大量数据导致性能下降\n\n"
            "建议在非业务高峰期执行，并确保已有备份。\n\n"
            "确定要继续吗？",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            self._execute_datapump("impdp", confirm=True)
    
    def _execute_datapump(self, operation: str, confirm: bool = False) -> None:
        """
        执行数据泵操作
        
        Args:
            operation: 'expdp' 或 'impdp'
            confirm: 是否需要确认（已在外层处理）
        """
        # 检查连接
        if not self.current_profile or self.current_db_type != "oracle":
            QMessageBox.warning(self, "不支持", "数据泵仅支持 Oracle 数据库")
            return
        
        # 获取参数
        directory = self.pump_dir_input.text().strip()
        filename = self.pump_file_input.text().strip()
        
        if not directory:
            QMessageBox.warning(self, "参数错误", "请输入 Directory 名称")
            return
        
        if not filename:
            QMessageBox.warning(self, "参数错误", "请输入 DMP 文件名")
            return
        
        # 检查 Oracle 客户端环境
        available, msg = DataPumpWorker.check_oracle_client()
        if not available:
            QMessageBox.critical(
                self,
                "环境检查失败",
                f"{msg}\n\n"
                f"请确保已安装 Oracle Instant Client 或完整客户端，\n"
                f"并将 bin 目录添加到系统 PATH 环境变量。"
            )
            return
        
        # 停止之前的任务
        if self.datapump_worker and self.datapump_worker.is_running():
            self.datapump_worker.stop()
        
        # 切换到文本模式显示日志
        self._switch_result_mode("text")
        self._log_message(f"\n{'='*60}")
        self._log_message(f"【Oracle 数据泵 {operation.upper()}】")
        self._log_message(f"{'='*60}")
        
        # 构建数据库配置
        db_config = {
            "username": self.current_profile.get("username", ""),
            "password": self.current_profile.get("password", ""),
            "host": self.current_profile.get("host", ""),
            "port": self.current_profile.get("port", 1521),
            "service_name": self.current_profile.get("database", "ORCL"),
        }
        
        # 禁用按钮
        self._set_datapump_executing_state(True)
        self.status_label.setText(f"正在执行 {operation.upper()}...")
        self.status_label.setStyleSheet("color: #569cd6;")
        
        # 创建并启动工作线程
        self.datapump_worker = DataPumpWorker(
            db_config=db_config,
            operation=operation,
            dmp_filename=filename,
            directory_name=directory,
            parent=self
        )
        
        self.datapump_worker.output_signal.connect(self._on_datapump_output)
        self.datapump_worker.error_signal.connect(self._on_datapump_error)
        self.datapump_worker.finished_signal.connect(
            lambda exit_code, success: self._on_datapump_finished(exit_code, success, operation)
        )
        
        self.datapump_worker.start()
    
    def _on_datapump_output(self, line: str) -> None:
        """数据泵实时输出"""
        self.result_text.append(line)
        # 滚动到底部
        scrollbar = self.result_text.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
    
    def _on_datapump_error(self, error_msg: str) -> None:
        """数据泵错误"""
        self.result_text.append(f"\n[错误] {error_msg}")
        self.status_label.setText("✗ 数据泵执行失败")
        self.status_label.setStyleSheet("color: #f48771;")
    
    def _on_datapump_finished(self, exit_code: int, success: bool, operation: str) -> None:
        """数据泵执行完成"""
        self._set_datapump_executing_state(False)
        
        if success:
            self.status_label.setText(f"✓ {operation.upper()} 完成")
            self.status_label.setStyleSheet("color: #4ec9b0;")
        else:
            self.status_label.setText(f"✗ {operation.upper()} 失败 (码: {exit_code})")
            self.status_label.setStyleSheet("color: #f48771;")
    
    def _set_datapump_executing_state(self, executing: bool) -> None:
        """设置数据泵执行状态"""
        self.expdp_btn.setEnabled(not executing)
        self.impdp_btn.setEnabled(not executing)
        self.pump_dir_input.setEnabled(not executing)
        self.pump_file_input.setEnabled(not executing)
        self.get_filename_btn.setEnabled(not executing)
    
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
    
    def _switch_result_mode(self, mode: str) -> None:
        """
        切换结果显示模式
        
        Args:
            mode: 'text' 或 'table'
        """
        if mode == "text":
            self.result_stack.setCurrentIndex(0)
            self.result_mode_label.setText("[文本模式]")
        else:
            self.result_stack.setCurrentIndex(1)
            self.result_mode_label.setText("[表格模式]")
        self.result_mode_label.setVisible(True)
    
    def _on_query_success(self, status: str, data_type: str, content, metadata: dict, description: str) -> None:
        """
        查询成功回调
        
        Args:
            status: 'success'
            data_type: 'table' 或 'text'
            content: 实际数据
            metadata: 元信息（行数、列数等）
            description: 操作描述
        """
        if data_type == "text":
            # 文本结果显示
            self._switch_result_mode("text")
            self.result_text.append(f"\n{'='*60}")
            self.result_text.append(f"【{description}】")
            self.result_text.append(f"{'='*60}\n")
            self.result_text.append(str(content))
            self.result_text.append(f"\n{'='*60}")
            
            row_count = metadata.get("row_count", 0)
            elapsed_ms = metadata.get("elapsed_ms", 0)
            self.result_text.append(f"行数: {row_count} | 耗时: {elapsed_ms}ms")
            
        else:  # table
            # 表格结果显示
            self._switch_result_mode("table")
            
            headers = metadata.get("columns", [])
            rows = content if isinstance(content, list) else []
            
            # 设置表格
            self.result_table.setColumnCount(len(headers))
            self.result_table.setRowCount(len(rows))
            self.result_table.setHorizontalHeaderLabels(headers)
            
            # 填充数据
            for row_idx, row_data in enumerate(rows):
                for col_idx, cell_value in enumerate(row_data):
                    item = QTableWidgetItem(str(cell_value))
                    item.setFlags(item.flags() & ~Qt.ItemIsEditable)
                    self.result_table.setItem(row_idx, col_idx, item)
            
            # 调整列宽
            self.result_table.resizeColumnsToContents()
            
            # 更新标签
            row_count = metadata.get("row_count", 0)
            col_count = metadata.get("column_count", 0)
            elapsed_ms = metadata.get("elapsed_ms", 0)
            self.result_label.setText(f"{description} - {row_count} 行数据")
        
        # 更新状态栏
        elapsed_ms = metadata.get("elapsed_ms", 0)
        self.status_label.setText(f"✓ {description} 完成 ({elapsed_ms}ms)")
        self.status_label.setStyleSheet("color: #4ec9b0;")
    
    def _on_query_error(self, error_msg: str, sql_text: str) -> None:
        """
        查询错误回调
        
        Args:
            error_msg: 错误信息
            sql_text: 执行的 SQL
        """
        self._switch_result_mode("text")
        self.result_text.append(f"\n{'='*60}")
        self.result_text.append("【执行错误】")
        self.result_text.append(f"{'='*60}\n")
        self.result_text.append(error_msg)
        self.result_text.append(f"\n{'='*60}")
        
        # 限制 SQL 显示长度
        sql_display = sql_text[:500] + "..." if len(sql_text) > 500 else sql_text
        self.result_text.append(f"\nSQL:\n{sql_display}")
        
        self.status_label.setText("✗ 执行失败")
        self.status_label.setStyleSheet("color: #f48771;")
    
    def _set_executing_state(self, executing: bool) -> None:
        """设置执行状态"""
        # 禁用/启用操作按钮
        for i in range(self.ops_layout.count()):
            item = self.ops_layout.itemAt(i)
            if item and item.widget():
                item.widget().setEnabled(not executing)
        
        self.connect_btn.setEnabled(not executing)
        self.conn_combo.setEnabled(not executing)
    
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
        self.result_label.setText("操作结果")
        self.result_mode_label.setVisible(False)
    
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
    
    def closeEvent(self, event) -> None:
        """关闭时确保线程停止"""
        if self.db_worker and self.db_worker.is_running():
            self.db_worker.stop()
        if self.datapump_worker and self.datapump_worker.is_running():
            self.datapump_worker.stop()
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
    
    dashboard = DatabaseOpsWidget(title="数据库运维仪表盘")
    dashboard.resize(900, 700)
    dashboard.show()
    
    sys.exit(app.exec())
