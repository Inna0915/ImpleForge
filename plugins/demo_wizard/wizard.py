"""
数据库连接配置向导 - Phase 4 完整版

功能：
- 管理多个数据库连接配置（保存/加载/删除）
- 支持 MySQL 真实连接测试
- 异步测试避免 UI 卡顿

依赖安装:
    pip install sqlalchemy pymysql
"""

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QFormLayout,
    QLineEdit,
    QPushButton,
    QLabel,
    QMessageBox,
    QGroupBox,
    QSpinBox,
    QFrame,
    QComboBox,
    QApplication
)
from PySide6.QtCore import Qt, QThread
from PySide6.QtGui import QFont

# 导入核心模块
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.managers.connection_manager import ConnectionManager
from core.utils.db_tester import DBTestWorker


class DatabaseWizard(QWidget):
    """
    数据库连接配置向导插件
    
    Phase 4 功能：
    - 保存/加载连接配置
    - 真实数据库连接测试（异步）
    - 多配置管理
    """
    
    def __init__(
        self,
        title: str = "数据库连接配置",
        default_host: str = "localhost",
        default_port: int = 3306,
        parent=None
    ):
        super().__init__(parent)
        
        self.title_text = title
        self.default_host = default_host
        self.default_port = default_port
        
        # 初始化连接管理器
        self.connection_manager = ConnectionManager()
        
        # 测试线程
        self.test_worker: DBTestWorker = None
        
        self._setup_ui()
        self._apply_styles()
        self._load_saved_profiles()
    
    def _setup_ui(self) -> None:
        """设置界面布局"""
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(30, 30, 30, 30)
        main_layout.setSpacing(20)
        
        # ========== 标题区 ==========
        title_label = QLabel(f"🔌 {self.title_text}")
        title_font = QFont()
        title_font.setPointSize(18)
        title_font.setBold(True)
        title_label.setFont(title_font)
        title_label.setStyleSheet("color: #cccccc;")
        main_layout.addWidget(title_label)
        
        subtitle = QLabel("配置并测试数据库连接")
        subtitle.setStyleSheet("color: #969696; margin-bottom: 10px;")
        main_layout.addWidget(subtitle)
        
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setStyleSheet("background-color: #333333; max-height: 1px;")
        main_layout.addWidget(line)
        
        # ========== 已保存配置区 ==========
        profiles_group = QGroupBox("已保存的配置")
        profiles_group.setStyleSheet("""
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
        
        profiles_layout = QHBoxLayout(profiles_group)
        profiles_layout.setSpacing(10)
        profiles_layout.setContentsMargins(15, 15, 15, 15)
        
        # 配置选择下拉框
        self.profile_combo = QComboBox()
        self.profile_combo.setMinimumWidth(250)
        self.profile_combo.setPlaceholderText("-- 选择已保存的配置 --")
        self.profile_combo.currentIndexChanged.connect(self._on_profile_selected)
        profiles_layout.addWidget(self.profile_combo)
        
        # 刷新按钮
        self.refresh_btn = QPushButton("🔄")
        self.refresh_btn.setFixedSize(32, 32)
        self.refresh_btn.setToolTip("刷新配置列表")
        self.refresh_btn.clicked.connect(self._load_saved_profiles)
        profiles_layout.addWidget(self.refresh_btn)
        
        # 删除按钮
        self.delete_btn = QPushButton("🗑️")
        self.delete_btn.setFixedSize(32, 32)
        self.delete_btn.setToolTip("删除当前选中的配置")
        self.delete_btn.clicked.connect(self._on_delete_profile)
        profiles_layout.addWidget(self.delete_btn)
        
        profiles_layout.addStretch()
        main_layout.addWidget(profiles_group)
        
        # ========== 表单区 ==========
        form_group = QGroupBox("连接信息")
        form_group.setStyleSheet(profiles_group.styleSheet())
        
        form_layout = QFormLayout(form_group)
        form_layout.setSpacing(12)
        form_layout.setContentsMargins(20, 20, 20, 20)
        
        # 配置名称
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("为此配置命名，如：生产环境 MySQL")
        form_layout.addRow("配置名称:", self.name_input)
        
        # 数据库类型
        self.db_type_combo = QComboBox()
        self.db_type_combo.addItems(["mysql", "postgresql", "sqlite", "mssql", "oracle"])
        self.db_type_combo.setCurrentText("mysql")
        form_layout.addRow("数据库类型:", self.db_type_combo)
        
        # 主机和端口（水平布局）
        host_port_layout = QHBoxLayout()
        host_port_layout.setSpacing(10)
        
        self.host_input = QLineEdit(self.default_host)
        self.host_input.setPlaceholderText("例如: localhost 或 192.168.1.100")
        host_port_layout.addWidget(self.host_input, stretch=3)
        
        self.port_input = QSpinBox()
        self.port_input.setRange(1, 65535)
        self.port_input.setValue(self.default_port)
        self.port_input.setSuffix(" 端口")
        host_port_layout.addWidget(self.port_input, stretch=1)
        
        form_layout.addRow("主机地址:", host_port_layout)
        
        # 数据库名
        self.database_input = QLineEdit()
        self.database_input.setPlaceholderText("数据库名称（可选）")
        form_layout.addRow("数据库名:", self.database_input)
        
        # 用户名
        self.username_input = QLineEdit()
        self.username_input.setPlaceholderText("请输入用户名")
        form_layout.addRow("用户名:", self.username_input)
        
        # 密码
        self.password_input = QLineEdit()
        self.password_input.setPlaceholderText("请输入密码")
        self.password_input.setEchoMode(QLineEdit.Password)
        form_layout.addRow("密码:", self.password_input)
        
        main_layout.addWidget(form_group)
        
        # ========== 按钮区 ==========
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(15)
        
        # 测试连接按钮
        self.test_btn = QPushButton("🚀 测试连接")
        self.test_btn.setFixedHeight(42)
        self.test_btn.setCursor(Qt.PointingHandCursor)
        self.test_btn.clicked.connect(self._on_test_connection)
        btn_layout.addWidget(self.test_btn)
        
        # 保存配置按钮
        self.save_btn = QPushButton("💾 保存配置")
        self.save_btn.setFixedHeight(42)
        self.save_btn.setCursor(Qt.PointingHandCursor)
        self.save_btn.clicked.connect(self._on_save_config)
        btn_layout.addWidget(self.save_btn)
        
        # 新建配置按钮
        self.new_btn = QPushButton("➕ 新建")
        self.new_btn.setFixedHeight(42)
        self.new_btn.setCursor(Qt.PointingHandCursor)
        self.new_btn.clicked.connect(self._on_new_config)
        btn_layout.addWidget(self.new_btn)
        
        btn_layout.addStretch()
        main_layout.addLayout(btn_layout)
        
        # ========== 状态区 ==========
        self.status_label = QLabel("就绪 - 请填写连接信息或选择已保存的配置")
        self.status_label.setStyleSheet("color: #969696; margin-top: 10px; padding: 10px; background-color: #252526; border-radius: 4px;")
        self.status_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(self.status_label)
        
        main_layout.addStretch()
    
    def _apply_styles(self) -> None:
        """应用深色主题样式"""
        # 输入框样式
        input_style = """
            QLineEdit {
                background-color: #3c3c3c;
                color: #cccccc;
                border: 1px solid #3c3c3c;
                border-radius: 4px;
                padding: 8px 12px;
                font-size: 13px;
                min-height: 20px;
            }
            QLineEdit:focus {
                border: 1px solid #007acc;
            }
            QLineEdit::placeholder {
                color: #6e6e6e;
            }
        """
        self.name_input.setStyleSheet(input_style)
        self.host_input.setStyleSheet(input_style)
        self.database_input.setStyleSheet(input_style)
        self.username_input.setStyleSheet(input_style)
        self.password_input.setStyleSheet(input_style)
        
        # ComboBox 样式
        combo_style = """
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
        """
        self.profile_combo.setStyleSheet(combo_style)
        self.db_type_combo.setStyleSheet(combo_style)
        
        # SpinBox 样式
        self.port_input.setStyleSheet("""
            QSpinBox {
                background-color: #3c3c3c;
                color: #cccccc;
                border: 1px solid #3c3c3c;
                border-radius: 4px;
                padding: 8px 12px;
                font-size: 13px;
                min-height: 20px;
            }
            QSpinBox:focus {
                border: 1px solid #007acc;
            }
            QSpinBox::up-button, QSpinBox::down-button {
                width: 20px;
                background-color: #454545;
                border: none;
            }
            QSpinBox::up-button:hover, QSpinBox::down-button:hover {
                background-color: #505050;
            }
        """)
        
        # 按钮样式
        self.test_btn.setStyleSheet("""
            QPushButton {
                background-color: #0e639c;
                color: #ffffff;
                border: none;
                border-radius: 4px;
                padding: 0 24px;
                font-size: 14px;
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
        
        self.save_btn.setStyleSheet("""
            QPushButton {
                background-color: #238636;
                color: #ffffff;
                border: none;
                border-radius: 4px;
                padding: 0 24px;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #2ea043;
            }
            QPushButton:pressed {
                background-color: #1a6329;
            }
        """)
        
        self.new_btn.setStyleSheet("""
            QPushButton {
                background-color: #3c3c3c;
                color: #cccccc;
                border: 1px solid #454545;
                border-radius: 4px;
                padding: 0 20px;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #454545;
            }
            QPushButton:pressed {
                background-color: #333333;
            }
        """)
    
    def _load_saved_profiles(self) -> None:
        """加载已保存的配置到下拉框"""
        current_text = self.profile_combo.currentText()
        
        self.profile_combo.clear()
        self.profile_combo.addItem("-- 选择已保存的配置 --", None)
        
        profiles = self.connection_manager.load_profiles()
        
        for profile in profiles:
            name = profile.get("name", "未命名")
            db_type = profile.get("db_type", "unknown")
            display = f"{name} ({db_type})"
            self.profile_combo.addItem(display, profile)
        
        # 恢复之前的选择
        if current_text:
            index = self.profile_combo.findText(current_text)
            if index >= 0:
                self.profile_combo.setCurrentIndex(index)
        
        count = len(profiles)
        self.status_label.setText(f"已加载 {count} 个配置 - 请选择或新建配置")
        self.status_label.setStyleSheet("color: #969696; margin-top: 10px; padding: 10px; background-color: #252526; border-radius: 4px;")
    
    def _on_profile_selected(self, index: int) -> None:
        """选择已保存配置时的处理"""
        if index <= 0:  # 第一项是提示文本
            return
        
        profile = self.profile_combo.itemData(index)
        if not profile:
            return
        
        # 填充表单
        self.name_input.setText(profile.get("name", ""))
        self.db_type_combo.setCurrentText(profile.get("db_type", "mysql"))
        self.host_input.setText(profile.get("host", ""))
        self.port_input.setValue(profile.get("port", 3306))
        self.database_input.setText(profile.get("database", ""))
        self.username_input.setText(profile.get("username", ""))
        self.password_input.setText(profile.get("password", ""))
        
        self.status_label.setText(f"已加载配置: {profile.get('name', '')}")
        self.status_label.setStyleSheet("color: #4ec9b0; margin-top: 10px; padding: 10px; background-color: #252526; border-radius: 4px;")
    
    def _on_delete_profile(self) -> None:
        """删除选中的配置"""
        index = self.profile_combo.currentIndex()
        if index <= 0:
            QMessageBox.warning(self, "删除失败", "请先选择一个要删除的配置")
            return
        
        profile = self.profile_combo.itemData(index)
        name = profile.get("name", "未命名")
        
        # 确认对话框
        reply = QMessageBox.question(
            self,
            "确认删除",
            f"确定要删除配置 \"{name}\" 吗？\n此操作不可恢复。",
            QMessageBox.Yes | QMessageBox.No,
            QMessageBox.No
        )
        
        if reply == QMessageBox.Yes:
            if self.connection_manager.delete_profile(name):
                QMessageBox.information(self, "删除成功", f"配置 \"{name}\" 已删除")
                self._on_new_config()
                self._load_saved_profiles()
            else:
                QMessageBox.warning(self, "删除失败", "无法删除配置，请检查文件权限")
    
    def _on_test_connection(self) -> None:
        """测试连接按钮点击 - 异步执行"""
        # 获取表单数据
        profile = self._get_profile_from_form()
        
        if not profile["host"]:
            QMessageBox.warning(self, "输入错误", "请输入主机地址！")
            return
        
        if not profile["username"] and profile["db_type"] != "sqlite":
            QMessageBox.warning(self, "输入错误", "请输入用户名！")
            return
        
        # 如果有正在运行的测试，先停止
        if self.test_worker and self.test_worker.is_running():
            self.test_worker.stop()
        
        # 更新 UI 状态
        self._set_testing_state(True)
        self.status_label.setText("正在测试连接...")
        self.status_label.setStyleSheet("color: #569cd6; margin-top: 10px; padding: 10px; background-color: #252526; border-radius: 4px;")
        
        # 创建并启动测试线程
        self.test_worker = DBTestWorker(profile, parent=self)
        self.test_worker.success_signal.connect(self._on_test_success)
        self.test_worker.error_signal.connect(self._on_test_error)
        self.test_worker.finished_signal.connect(lambda: self._set_testing_state(False))
        
        self.test_worker.start()
    
    def _on_test_success(self, message: str) -> None:
        """连接测试成功回调"""
        self.status_label.setText("连接测试通过 ✓")
        self.status_label.setStyleSheet("color: #4ec9b0; margin-top: 10px; padding: 10px; background-color: #252526; border-radius: 4px;")
        
        QMessageBox.information(
            self,
            "连接成功",
            f"✅ 数据库连接成功！\n\n{message}",
            QMessageBox.Ok
        )
    
    def _on_test_error(self, message: str) -> None:
        """连接测试失败回调"""
        self.status_label.setText("连接测试失败 ✗")
        self.status_label.setStyleSheet("color: #f48771; margin-top: 10px; padding: 10px; background-color: #252526; border-radius: 4px;")
        
        QMessageBox.warning(
            self,
            "连接失败",
            f"❌ 无法连接到数据库\n\n{message}",
            QMessageBox.Ok
        )
    
    def _set_testing_state(self, testing: bool) -> None:
        """设置测试状态，更新 UI"""
        self.test_btn.setEnabled(not testing)
        self.save_btn.setEnabled(not testing)
        
        if testing:
            self.test_btn.setText("⏳ 测试中...")
        else:
            self.test_btn.setText("🚀 测试连接")
        
        QApplication.processEvents()
    
    def _on_save_config(self) -> None:
        """保存配置按钮点击"""
        name = self.name_input.text().strip()
        
        if not name:
            QMessageBox.warning(self, "输入错误", "请输入配置名称！")
            return
        
        profile = self._get_profile_from_form()
        
        # 保存到文件
        success = self.connection_manager.save_profile(
            name=name,
            host=profile["host"],
            port=profile["port"],
            username=profile["username"],
            password=profile["password"],
            db_type=profile["db_type"],
            database=profile["database"]
        )
        
        if success:
            QMessageBox.information(self, "保存成功", f"配置 \"{name}\" 已保存")
            self._load_saved_profiles()
            # 选中新保存的配置
            for i in range(self.profile_combo.count()):
                if name in self.profile_combo.itemText(i):
                    self.profile_combo.setCurrentIndex(i)
                    break
        else:
            QMessageBox.warning(self, "保存失败", "无法保存配置，请检查文件权限")
    
    def _on_new_config(self) -> None:
        """新建配置 - 清空表单"""
        self.profile_combo.setCurrentIndex(0)
        self.name_input.clear()
        self.db_type_combo.setCurrentText("mysql")
        self.host_input.setText(self.default_host)
        self.port_input.setValue(self.default_port)
        self.database_input.clear()
        self.username_input.clear()
        self.password_input.clear()
        
        self.status_label.setText("新建配置 - 请填写信息")
        self.status_label.setStyleSheet("color: #969696; margin-top: 10px; padding: 10px; background-color: #252526; border-radius: 4px;")
    
    def _get_profile_from_form(self) -> dict:
        """从表单获取配置数据"""
        return {
            "name": self.name_input.text().strip(),
            "db_type": self.db_type_combo.currentText(),
            "host": self.host_input.text().strip(),
            "port": self.port_input.value(),
            "database": self.database_input.text().strip(),
            "username": self.username_input.text().strip(),
            "password": self.password_input.text()
        }
    
    def closeEvent(self, event) -> None:
        """关闭时确保测试线程停止"""
        if self.test_worker and self.test_worker.is_running():
            self.test_worker.stop()
        event.accept()


# 插件入口点
if __name__ == "__main__":
    import sys
    from PySide6.QtWidgets import QApplication
    
    app = QApplication(sys.argv)
    
    # 应用暗色主题
    app.setStyleSheet("""
        QWidget {
            background-color: #1e1e1e;
            color: #cccccc;
        }
    """)
    
    wizard = DatabaseWizard(title="MySQL 配置向导")
    wizard.resize(550, 600)
    wizard.show()
    
    sys.exit(app.exec())
