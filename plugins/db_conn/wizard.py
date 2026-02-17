"""
数据库连接配置向导 - Phase 7 (Navicat Style)

重构为 Navicat 风格的差异化配置界面
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QFormLayout,
    QLineEdit, QPushButton, QLabel, QMessageBox,
    QGroupBox, QFrame, QComboBox, QRadioButton,
    QButtonGroup, QSpinBox, QStackedWidget, QApplication
)
from PySide6.QtCore import Qt
from PySide6.QtGui import QFont

from core.managers.connection_manager import ConnectionManager
from core.utils.db_tester import test_db_connection, DBTestWorker


class ConnectionWizard(QWidget):
    """Navicat 风格的数据库连接配置向导"""
    
    DEFAULT_PORTS = {
        "mysql": 3306, "mariadb": 3306,
        "sqlserver": 1433, "oracle": 1521, "mongodb": 27017
    }
    
    DB_TYPE_LABELS = {
        "mysql": "MySQL", "mariadb": "MariaDB",
        "sqlserver": "SQL Server", "oracle": "Oracle", "mongodb": "MongoDB"
    }
    
    def __init__(self, title: str = "数据库连接配置", parent=None):
        super().__init__(parent)
        self.title_text = title
        self.connection_manager = ConnectionManager()
        self.test_worker = None
        self._setup_ui()
        self._apply_styles()
        self._load_profiles()
    
    def _setup_ui(self):
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(30, 30, 30, 30)
        main_layout.setSpacing(20)
        
        # 标题
        title = QLabel(f"🔌 {self.title_text}")
        font = QFont()
        font.setPointSize(18)
        font.setBold(True)
        title.setFont(font)
        title.setStyleSheet("color: #cccccc;")
        main_layout.addWidget(title)
        
        subtitle = QLabel("Navicat 风格配置")
        subtitle.setStyleSheet("color: #969696;")
        main_layout.addWidget(subtitle)
        
        # 已保存配置
        profiles_group = QGroupBox("已保存的配置")
        profiles_layout = QHBoxLayout(profiles_group)
        
        self.profile_combo = QComboBox()
        self.profile_combo.setMinimumWidth(300)
        self.profile_combo.currentIndexChanged.connect(self._on_profile_selected)
        profiles_layout.addWidget(self.profile_combo)
        
        self.refresh_btn = QPushButton("🔄")
        self.refresh_btn.setFixedSize(32, 32)
        self.refresh_btn.clicked.connect(self._load_profiles)
        profiles_layout.addWidget(self.refresh_btn)
        
        self.delete_btn = QPushButton("🗑️")
        self.delete_btn.setFixedSize(32, 32)
        self.delete_btn.clicked.connect(self._on_delete_profile)
        profiles_layout.addWidget(self.delete_btn)
        
        main_layout.addWidget(profiles_group)
        
        # 主配置区
        config_group = QGroupBox("连接配置")
        config_layout = QVBoxLayout(config_group)
        
        # 基本配置
        basic_form = QFormLayout()
        basic_form.setSpacing(12)
        
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("配置名称")
        basic_form.addRow("配置名称:", self.name_input)
        
        self.type_combo = QComboBox()
        for key, label in self.DB_TYPE_LABELS.items():
            self.type_combo.addItem(label, key)
        self.type_combo.currentIndexChanged.connect(self._on_db_type_changed)
        basic_form.addRow("数据库类型:", self.type_combo)
        
        # 主机和端口
        host_port = QHBoxLayout()
        self.host_input = QLineEdit("localhost")
        self.port_input = QSpinBox()
        self.port_input.setRange(1, 65535)
        self.port_input.setValue(3306)
        host_port.addWidget(self.host_input, 3)
        host_port.addWidget(self.port_input, 1)
        basic_form.addRow("主机:端口", host_port)
        
        self.username_input = QLineEdit()
        self.password_input = QLineEdit()
        self.password_input.setEchoMode(QLineEdit.Password)
        basic_form.addRow("用户名:", self.username_input)
        basic_form.addRow("密码:", self.password_input)
        
        config_layout.addLayout(basic_form)
        
        # 动态特定配置
        self.specific_stack = QStackedWidget()
        
        # Page 0: MySQL/SQL Server (Database)
        self.page_db = QWidget()
        form_db = QFormLayout(self.page_db)
        self.dbname_input = QLineEdit()
        form_db.addRow("数据库名:", self.dbname_input)
        
        # Page 1: MongoDB (Auth Source)
        self.page_mongo = QWidget()
        form_mongo = QFormLayout(self.page_mongo)
        self.auth_source_input = QLineEdit("admin")
        form_mongo.addRow("Auth Source:", self.auth_source_input)
        
        # Page 2: Oracle (Service Name / SID)
        self.page_oracle = QWidget()
        oracle_layout = QVBoxLayout(self.page_oracle)
        
        # 模式选择
        mode_group = QGroupBox("连接模式")
        mode_layout = QHBoxLayout(mode_group)
        
        self.service_radio = QRadioButton("Service Name")
        self.service_radio.setChecked(True)
        self.sid_radio = QRadioButton("SID")
        self.service_radio.toggled.connect(self._on_oracle_mode_changed)
        
        mode_layout.addWidget(self.service_radio)
        mode_layout.addWidget(self.sid_radio)
        mode_layout.addStretch()
        
        oracle_layout.addWidget(mode_group)
        
        # 值输入
        form_oracle = QFormLayout()
        self.oracle_value_input = QLineEdit("ORCL")
        self.oracle_label = QLabel("服务名称:")
        form_oracle.addRow(self.oracle_label, self.oracle_value_input)
        oracle_layout.addLayout(form_oracle)
        oracle_layout.addStretch()
        
        self.specific_stack.addWidget(self.page_db)
        self.specific_stack.addWidget(self.page_mongo)
        self.specific_stack.addWidget(self.page_oracle)
        
        config_layout.addWidget(self.specific_stack)
        main_layout.addWidget(config_group)
        
        # 按钮
        btn_layout = QHBoxLayout()
        self.test_btn = QPushButton("🚀 测试连接")
        self.test_btn.clicked.connect(self._on_test)
        self.save_btn = QPushButton("💾 保存")
        self.save_btn.clicked.connect(self._on_save)
        self.new_btn = QPushButton("➕ 新建")
        self.new_btn.clicked.connect(self._on_new)
        
        btn_layout.addWidget(self.test_btn)
        btn_layout.addWidget(self.save_btn)
        btn_layout.addWidget(self.new_btn)
        btn_layout.addStretch()
        main_layout.addLayout(btn_layout)
        
        # 状态
        self.status_label = QLabel("就绪")
        self.status_label.setAlignment(Qt.AlignCenter)
        main_layout.addWidget(self.status_label)
        main_layout.addStretch()
    
    def _on_oracle_mode_changed(self):
        if self.service_radio.isChecked():
            self.oracle_label.setText("服务名称:")
            self.oracle_value_input.setPlaceholderText("ORCL")
        else:
            self.oracle_label.setText("SID:")
            self.oracle_value_input.setPlaceholderText("orcl")
    
    def _on_db_type_changed(self, index):
        db_type = self.type_combo.currentData()
        if not db_type:
            return
        
        # 更新端口
        self.port_input.setValue(self.DEFAULT_PORTS.get(db_type, 3306))
        
        # 切换页面
        if db_type in ["mysql", "mariadb", "sqlserver"]:
            self.specific_stack.setCurrentIndex(0)
        elif db_type == "mongodb":
            self.specific_stack.setCurrentIndex(1)
        elif db_type == "oracle":
            self.specific_stack.setCurrentIndex(2)
    
    def _apply_styles(self):
        self.setStyleSheet("""
            QWidget { background-color: #1e1e1e; color: #cccccc; }
            QLineEdit, QComboBox {
                background-color: #3c3c3c;
                border: 1px solid #3c3c3c;
                padding: 8px;
                border-radius: 4px;
            }
            QPushButton {
                background-color: #0e639c;
                color: white;
                padding: 10px 20px;
                border: none;
                border-radius: 4px;
            }
            QPushButton:hover { background-color: #1177bb; }
            QGroupBox {
                border: 1px solid #333;
                margin-top: 10px;
                padding-top: 10px;
                font-weight: bold;
            }
        """)
    
    def _load_profiles(self):
        self.profile_combo.clear()
        self.profile_combo.addItem("-- 选择配置 --", None)
        profiles = self.connection_manager.load_profiles()
        for p in profiles:
            name = p.get("name", "")
            db_type = p.get("db_type", "")
            self.profile_combo.addItem(f"{name} ({db_type})", p)
    
    def _on_profile_selected(self, index):
        if index <= 0:
            return
        p = self.profile_combo.itemData(index)
        if not p:
            return
        
        self.name_input.setText(p.get("name", ""))
        
        db_type = p.get("db_type", "mysql")
        idx = self.type_combo.findData(db_type)
        if idx >= 0:
            self.type_combo.setCurrentIndex(idx)
        
        self.host_input.setText(p.get("host", ""))
        self.port_input.setValue(p.get("port", 3306))
        self.username_input.setText(p.get("username", ""))
        self.password_input.setText(p.get("password", ""))
        
        # 特定配置
        if db_type in ["mysql", "mariadb", "sqlserver"]:
            self.dbname_input.setText(p.get("database", ""))
        elif db_type == "mongodb":
            self.auth_source_input.setText(p.get("auth_source", "admin"))
        elif db_type == "oracle":
            mode = p.get("oracle_mode", "service_name")
            if mode == "sid":
                self.sid_radio.setChecked(True)
            else:
                self.service_radio.setChecked(True)
            self.oracle_value_input.setText(p.get("oracle_value", "ORCL"))
    
    def _on_delete_profile(self):
        index = self.profile_combo.currentIndex()
        if index <= 0:
            return
        p = self.profile_combo.itemData(index)
        name = p.get("name", "")
        
        if QMessageBox.question(self, "确认", f"删除 \"{name}\"?") == QMessageBox.Yes:
            self.connection_manager.delete_profile(name)
            self._load_profiles()
            self._on_new()
    
    def _get_form_data(self):
        db_type = self.type_combo.currentData()
        
        data = {
            "name": self.name_input.text().strip(),
            "db_type": db_type,
            "host": self.host_input.text().strip(),
            "port": self.port_input.value(),
            "username": self.username_input.text().strip(),
            "password": self.password_input.text(),
            "database": "",
            "auth_source": "",
            "oracle_mode": "",
            "oracle_value": ""
        }
        
        if db_type in ["mysql", "mariadb", "sqlserver"]:
            data["database"] = self.dbname_input.text().strip()
        elif db_type == "mongodb":
            data["auth_source"] = self.auth_source_input.text().strip() or "admin"
        elif db_type == "oracle":
            data["oracle_mode"] = "sid" if self.sid_radio.isChecked() else "service_name"
            data["oracle_value"] = self.oracle_value_input.text().strip() or "ORCL"
            data["database"] = data["oracle_value"]
        
        return data
    
    def _on_test(self):
        profile = self._get_form_data()
        if not profile["name"]:
            QMessageBox.warning(self, "提示", "请输入配置名称")
            return
        
        if self.test_worker and self.test_worker.is_running():
            self.test_worker.stop()
        
        self.test_btn.setEnabled(False)
        self.test_btn.setText("测试中...")
        self.status_label.setText("连接中...")
        
        self.test_worker = DBTestWorker(profile, parent=self)
        self.test_worker.success_signal.connect(
            lambda msg: (self.status_label.setText("✓ 成功"), 
                        QMessageBox.information(self, "成功", msg),
                        self.test_btn.setEnabled(True),
                        self.test_btn.setText("🚀 测试连接"))
        )
        self.test_worker.error_signal.connect(
            lambda msg: (self.status_label.setText("✗ 失败"),
                        QMessageBox.warning(self, "失败", msg),
                        self.test_btn.setEnabled(True),
                        self.test_btn.setText("🚀 测试连接"))
        )
        self.test_worker.start()
    
    def _on_save(self):
        profile = self._get_form_data()
        if not profile["name"]:
            QMessageBox.warning(self, "提示", "请输入配置名称")
            return
        
        if self.connection_manager.save_profile(**profile):
            QMessageBox.information(self, "成功", "配置已保存")
            self._load_profiles()
        else:
            QMessageBox.warning(self, "失败", "保存失败")
    
    def _on_new(self):
        self.profile_combo.setCurrentIndex(0)
        self.name_input.clear()
        self.type_combo.setCurrentIndex(0)
        self.host_input.setText("localhost")
        self.port_input.setValue(3306)
        self.username_input.clear()
        self.password_input.clear()
        self.dbname_input.clear()
        self.auth_source_input.setText("admin")
        self.service_radio.setChecked(True)
        self.oracle_value_input.setText("ORCL")
        self.status_label.setText("新建配置")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    wizard = ConnectionWizard()
    wizard.resize(500, 600)
    wizard.show()
    sys.exit(app.exec())
