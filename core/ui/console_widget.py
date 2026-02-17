"""
控制台组件 - 提供命令执行界面和实时输出显示
"""

from pathlib import Path
from typing import Optional, Dict, Any

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QTextEdit,
    QLabel,
    QFrame,
    QApplication
)
from PySide6.QtCore import Qt, QDateTime
from PySide6.QtGui import QFont, QTextCursor, QColor, QPalette

from ..executor import CommandWorker


class ConsoleWidget(QWidget):
    """
    控制台界面组件
    
    布局：
    +-------------------------------+
    | [标题]                        |
    | [描述信息]                     |
    +-------------------------------+
    | [开始执行] [停止]             |
    +-------------------------------+
    |                               |
    |      黑色控制台区域            |
    |      (QTextEdit)              |
    |                               |
    +-------------------------------+
    | 状态: 就绪 / 运行中 / 已完成   |
    +-------------------------------+
    """
    
    def __init__(
        self,
        command_data: Dict[str, Any],
        parent: Optional[QWidget] = None
    ):
        """
        初始化控制台组件
        
        Args:
            command_data: 命令配置数据，包含 cmd/script_path, description 等
            parent: 父部件
        """
        super().__init__(parent)
        
        self.command_data = command_data
        self.worker: Optional[CommandWorker] = None
        self.start_time: Optional[QDateTime] = None
        
        # 提取命令信息
        action = command_data.get("action", {})
        self.command = action.get("cmd", "")
        self.script_path = action.get("script_path", "")
        self.cwd = action.get("cwd", str(Path.cwd()))
        
        self._setup_ui()
        self._apply_terminal_style()
    
    def _setup_ui(self) -> None:
        """设置界面布局"""
        # 主布局
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(20, 20, 20, 20)
        main_layout.setSpacing(15)
        
        # ========== 顶部信息区 ==========
        info_frame = QFrame()
        info_frame.setStyleSheet("""
            QFrame {
                background-color: #252526;
                border: 1px solid #333333;
                border-radius: 4px;
                padding: 10px;
            }
        """)
        info_layout = QVBoxLayout(info_frame)
        info_layout.setContentsMargins(15, 15, 15, 15)
        
        # 标题
        name = self.command_data.get("name", "未命名任务")
        self.title_label = QLabel(f"▶ {name}")
        title_font = QFont()
        title_font.setPointSize(14)
        title_font.setBold(True)
        self.title_label.setFont(title_font)
        self.title_label.setStyleSheet("color: #cccccc;")
        info_layout.addWidget(self.title_label)
        
        # 描述
        description = self.command_data.get("description", "")
        if description:
            self.desc_label = QLabel(description)
            self.desc_label.setStyleSheet("color: #969696; margin-top: 5px;")
            info_layout.addWidget(self.desc_label)
        
        # 命令预览
        cmd_text = self.command or self.script_path
        self.cmd_preview = QLabel(f"命令: {cmd_text}")
        self.cmd_preview.setStyleSheet("""
            color: #6e6e6e; 
            margin-top: 10px;
            font-family: 'Consolas', 'Monaco', monospace;
            font-size: 12px;
        """)
        self.cmd_preview.setWordWrap(True)
        info_layout.addWidget(self.cmd_preview)
        
        main_layout.addWidget(info_frame)
        
        # ========== 控制按钮区 ==========
        btn_layout = QHBoxLayout()
        btn_layout.setSpacing(10)
        
        # 开始执行按钮
        self.run_btn = QPushButton("▶ 开始执行")
        self.run_btn.setFixedHeight(36)
        self.run_btn.setCursor(Qt.PointingHandCursor)
        self.run_btn.setStyleSheet("""
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
        self.run_btn.clicked.connect(self._on_run_clicked)
        btn_layout.addWidget(self.run_btn)
        
        # 停止按钮
        self.stop_btn = QPushButton("⏹ 停止")
        self.stop_btn.setFixedHeight(36)
        self.stop_btn.setEnabled(False)
        self.stop_btn.setCursor(Qt.PointingHandCursor)
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
        self.stop_btn.clicked.connect(self._on_stop_clicked)
        btn_layout.addWidget(self.stop_btn)
        
        # 清空按钮
        self.clear_btn = QPushButton("🗑 清空")
        self.clear_btn.setFixedHeight(36)
        self.clear_btn.setCursor(Qt.PointingHandCursor)
        self.clear_btn.setStyleSheet("""
            QPushButton {
                background-color: #3c3c3c;
                color: #cccccc;
                border: none;
                border-radius: 4px;
                padding: 0 16px;
                font-size: 13px;
            }
            QPushButton:hover {
                background-color: #454545;
            }
        """)
        self.clear_btn.clicked.connect(self._on_clear_clicked)
        btn_layout.addWidget(self.clear_btn)
        
        btn_layout.addStretch()
        main_layout.addLayout(btn_layout)
        
        # ========== 控制台输出区 ==========
        self.console = QTextEdit()
        self.console.setReadOnly(True)
        self.console.setLineWrapMode(QTextEdit.WidgetWidth)
        self.console.setFont(QFont("Consolas", 10))
        main_layout.addWidget(self.console, stretch=1)
        
        # ========== 底部状态栏 ==========
        status_frame = QFrame()
        status_frame.setStyleSheet("""
            QFrame {
                background-color: #252526;
                border-top: 1px solid #333333;
            }
        """)
        status_layout = QHBoxLayout(status_frame)
        status_layout.setContentsMargins(15, 8, 15, 8)
        
        self.status_label = QLabel("状态: 就绪")
        self.status_label.setStyleSheet("color: #969696; font-size: 12px;")
        status_layout.addWidget(self.status_label)
        
        status_layout.addStretch()
        
        self.time_label = QLabel("")
        self.time_label.setStyleSheet("color: #6e6e6e; font-size: 12px;")
        status_layout.addWidget(self.time_label)
        
        main_layout.addWidget(status_frame)
    
    def _apply_terminal_style(self) -> None:
        """应用终端样式（黑底绿字）"""
        self.console.setStyleSheet("""
            QTextEdit {
                background-color: #1e1e1e;
                color: #00ff00;
                border: 1px solid #333333;
                border-radius: 4px;
                padding: 10px;
                selection-background-color: #264f78;
            }
            QScrollBar:vertical {
                background-color: #1e1e1e;
                width: 12px;
                border-radius: 0px;
            }
            QScrollBar::handle:vertical {
                background-color: #424242;
                min-height: 30px;
                border-radius: 6px;
                margin: 2px;
            }
            QScrollBar::handle:vertical:hover {
                background-color: #4f4f4f;
            }
            QScrollBar::sub-line:vertical,
            QScrollBar::add-line:vertical {
                height: 0px;
            }
        """)
    
    def _on_run_clicked(self) -> None:
        """开始执行按钮点击事件"""
        if not self.command and not self.script_path:
            self._append_output("[错误] 未配置命令或脚本路径", "error")
            return
        
        # 清空之前的输出
        self.console.clear()
        self._append_output(f"$ {self.command or self.script_path}\n", "command")
        
        # 更新 UI 状态
        self._set_running_state(True)
        self.start_time = QDateTime.currentDateTime()
        
        # 创建并启动工作线程
        try:
            if self.script_path:
                from ..executor import ScriptWorker
                self.worker = ScriptWorker(
                    script_path=self.script_path,
                    cwd=self.cwd,
                    parent=self
                )
            else:
                self.worker = CommandWorker(
                    command=self.command,
                    cwd=self.cwd,
                    parent=self
                )
            
            # 连接信号
            self.worker.output_signal.connect(self._append_output)
            self.worker.error_signal.connect(self._on_error)
            self.worker.finished_signal.connect(self._on_finished)
            
            # 启动线程
            self.worker.start()
            
        except Exception as e:
            self._append_output(f"[错误] 启动失败: {e}", "error")
            self._set_running_state(False)
    
    def _on_stop_clicked(self) -> None:
        """停止按钮点击事件"""
        if self.worker and self.worker.is_running():
            self._append_output("\n[用户中断] 正在终止进程...", "warning")
            self.worker.stop()
    
    def _on_clear_clicked(self) -> None:
        """清空按钮点击事件"""
        self.console.clear()
        self.status_label.setText("状态: 就绪")
        self.time_label.setText("")
    
    def _append_output(self, text: str, style: str = "normal") -> None:
        """
        追加输出到控制台
        
        Args:
            text: 要显示的文本
            style: 文本样式 (normal, command, error, warning, success)
        """
        # 根据样式设置颜色
        color_map = {
            "normal": "#00ff00",    # 绿色
            "command": "#569cd6",   # 蓝色
            "error": "#f48771",     # 红色
            "warning": "#dcdcaa",   # 黄色
            "success": "#4ec9b0",   # 青色
        }
        color = color_map.get(style, "#00ff00")
        
        # 带颜色的 HTML 格式
        escaped_text = text.replace("<", "&lt;").replace(">", "&gt;")
        html = f'<span style="color: {color};">{escaped_text}</span>'
        
        self.console.append(html)
        
        # 自动滚动到底部
        scrollbar = self.console.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())
    
    def _on_error(self, error_msg: str) -> None:
        """错误处理"""
        self._append_output(error_msg, "error")
    
    def _on_finished(self, exit_code: int) -> None:
        """命令执行完成处理"""
        # 计算执行时间
        time_str = ""
        if self.start_time:
            elapsed = self.start_time.secsTo(QDateTime.currentDateTime())
            if elapsed < 60:
                time_str = f" ({elapsed}秒)"
            else:
                mins = elapsed // 60
                secs = elapsed % 60
                time_str = f" ({mins}分{secs}秒)"
        
        # 显示完成状态
        if exit_code == 0:
            self._append_output(f"\n[完成] 进程退出码: 0{time_str}", "success")
            self.status_label.setText("状态: 执行成功")
            self.status_label.setStyleSheet("color: #4ec9b0; font-size: 12px;")
        elif exit_code == -1:
            # 用户中断或启动失败
            self._append_output(f"\n[中断] 执行被终止", "warning")
            self.status_label.setText("状态: 已中断")
            self.status_label.setStyleSheet("color: #dcdcaa; font-size: 12px;")
        else:
            self._append_output(f"\n[错误] 进程退出码: {exit_code}{time_str}", "error")
            self.status_label.setText(f"状态: 执行失败 (码: {exit_code})")
            self.status_label.setStyleSheet("color: #f48771; font-size: 12px;")
        
        self.time_label.setText(time_str)
        
        # 恢复 UI 状态
        self._set_running_state(False)
        self.worker = None
    
    def _set_running_state(self, running: bool) -> None:
        """
        设置运行状态，更新 UI
        
        Args:
            running: 是否正在运行
        """
        self.run_btn.setEnabled(not running)
        self.stop_btn.setEnabled(running)
        self.clear_btn.setEnabled(not running)
        
        if running:
            self.status_label.setText("状态: 运行中...")
            self.status_label.setStyleSheet("color: #569cd6; font-size: 12px;")
        
        # 强制刷新 UI
        QApplication.processEvents()
