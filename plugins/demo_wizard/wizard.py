"""
示例插件：数据库连接配置向导

演示如何创建一个自定义 QWidget 插件，包含表单输入和交互逻辑。
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
)
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont


class DatabaseWizard(QWidget):
    """
    数据库连接配置向导插件
    
    功能：
    - 收集数据库连接信息（Host, Port, Username, Password）
    - 提供测试连接按钮
    - 展示插件开发的最佳实践
    """
    
    def __init__(
        self,
        title: str = "数据库连接配置",
        default_host: str = "localhost",
        default_port: int = 3306,
        parent=None
    ):
        """
        初始化数据库配置向导
        
        Args:
            title: 向导标题
            default_host: 默认主机地址
            default_port: 默认端口
            parent: 父窗口部件
        """
        super().__init__(parent)
        
        self.title = title
        self.default_host = default_host
        self.default_port = default_port
        
        self._setup_ui()
        self._apply_styles()
    
    def _setup_ui(self) -> None:
        """设置界面布局"""
        # 主布局
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(30, 30, 30, 30)
        main_layout.setSpacing(20)
        
        # ========== 标题区 ==========
        title_label = QLabel(f"🔌 {self.title}")
        title_font = QFont()
        title_font.setPointSize(18)
        title_font.setBold(True)
        title_label.setFont(title_font)
        title_label.setStyleSheet("color: #cccccc;")
        main_layout.addWidget(title_label)
        
        # 副标题
        subtitle = QLabel("配置数据库连接参数")
        subtitle.setStyleSheet("color: #969696; margin-bottom: 10px;")
        main_layout.addWidget(subtitle)
        
        # 分隔线
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setStyleSheet("background-color: #333333; max-height: 1px;")
        main_layout.addWidget(line)
        
        # ========== 表单区 ==========
        form_group = QGroupBox("连接信息")
        form_group.setStyleSheet("""
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
        
        form_layout = QFormLayout(form_group)
        form_layout.setSpacing(15)
        form_layout.setContentsMargins(20, 20, 20, 20)
        
        # Host 输入
        self.host_input = QLineEdit(self.default_host)
        self.host_input.setPlaceholderText("例如: localhost 或 192.168.1.100")
        form_layout.addRow("主机地址 (Host):", self.host_input)
        
        # Port 输入
        self.port_input = QSpinBox()
        self.port_input.setRange(1, 65535)
        self.port_input.setValue(self.default_port)
        self.port_input.setSuffix(" 端口")
        form_layout.addRow("端口 (Port):", self.port_input)
        
        # Username 输入
        self.username_input = QLineEdit()
        self.username_input.setPlaceholderText("请输入用户名")
        form_layout.addRow("用户名 (Username):", self.username_input)
        
        # Password 输入
        self.password_input = QLineEdit()
        self.password_input.setPlaceholderText("请输入密码")
        self.password_input.setEchoMode(QLineEdit.Password)
        form_layout.addRow("密码 (Password):", self.password_input)
        
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
        
        # 重置按钮
        self.reset_btn = QPushButton("🔄 重置")
        self.reset_btn.setFixedHeight(42)
        self.reset_btn.setCursor(Qt.PointingHandCursor)
        self.reset_btn.clicked.connect(self._on_reset)
        btn_layout.addWidget(self.reset_btn)
        
        btn_layout.addStretch()
        main_layout.addLayout(btn_layout)
        
        # ========== 状态区 ==========
        self.status_label = QLabel("就绪 - 请填写连接信息")
        self.status_label.setStyleSheet("color: #969696; margin-top: 10px;")
        self.status_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(self.status_label)
        
        # 添加弹性空间
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
        self.host_input.setStyleSheet(input_style)
        self.username_input.setStyleSheet(input_style)
        self.password_input.setStyleSheet(input_style)
        
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
        
        self.reset_btn.setStyleSheet("""
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
    
    def _on_test_connection(self) -> None:
        """测试连接按钮点击事件"""
        host = self.host_input.text().strip()
        port = self.port_input.value()
        username = self.username_input.text().strip()
        password = self.password_input.text()
        
        # 简单验证
        if not host:
            QMessageBox.warning(self, "输入错误", "请输入主机地址！")
            return
        
        if not username:
            QMessageBox.warning(self, "输入错误", "请输入用户名！")
            return
        
        # 模拟连接测试
        self.status_label.setText("正在测试连接...")
        self.status_label.setStyleSheet("color: #569cd6;")
        
        # 显示成功消息
        QMessageBox.information(
            self,
            "连接测试",
            f"✅ 连接模拟成功！\n\n"
            f"用户: {username}@{host}:{port}\n"
            f"认证: {'✓ 密码已提供' if password else '✗ 无密码'}\n\n"
            f"（这是一个演示插件，实际连接将在 Phase 4 实现）"
        )
        
        self.status_label.setText("连接测试通过 - 配置可用")
        self.status_label.setStyleSheet("color: #4ec9b0;")
    
    def _on_save_config(self) -> None:
        """保存配置按钮点击事件"""
        host = self.host_input.text().strip()
        port = self.port_input.value()
        username = self.username_input.text().strip()
        
        if not host or not username:
            QMessageBox.warning(self, "输入错误", "请填写完整的主机地址和用户名！")
            return
        
        # 显示保存成功
        QMessageBox.information(
            self,
            "保存成功",
            f"配置已保存！\n\n"
            f"主机: {host}:{port}\n"
            f"用户: {username}\n\n"
            f"（配置实际保存功能将在后续版本实现）"
        )
        
        self.status_label.setText("配置已保存")
        self.status_label.setStyleSheet("color: #4ec9b0;")
    
    def _on_reset(self) -> None:
        """重置按钮点击事件"""
        self.host_input.setText(self.default_host)
        self.port_input.setValue(self.default_port)
        self.username_input.clear()
        self.password_input.clear()
        self.status_label.setText("已重置 - 请重新填写")
        self.status_label.setStyleSheet("color: #969696;")
    
    def get_config(self) -> dict:
        """
        获取当前配置
        
        Returns:
            配置字典
        """
        return {
            "host": self.host_input.text().strip(),
            "port": self.port_input.value(),
            "username": self.username_input.text().strip(),
            "password": self.password_input.text(),
        }
    
    def set_config(self, config: dict) -> None:
        """
        设置配置
        
        Args:
            config: 配置字典
        """
        if "host" in config:
            self.host_input.setText(config["host"])
        if "port" in config:
            self.port_input.setValue(config["port"])
        if "username" in config:
            self.username_input.setText(config["username"])
        if "password" in config:
            self.password_input.setText(config["password"])


# 插件入口点：用于验证插件是否有效
if __name__ == "__main__":
    # 独立测试插件
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
    wizard.resize(500, 450)
    wizard.show()
    
    sys.exit(app.exec())
