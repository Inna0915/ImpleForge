"""
ImpleForge - Windows 实施工具箱
===============================

项目目录结构：
ImpleForge/
├── main.py                    # 程序入口
├── config/
│   └── menu.json             # 菜单配置文件
├── core/
│   ├── ui/
│   │   ├── __init__.py
│   │   └── main_window.py    # 主窗口逻辑
│   └── utils/
│       ├── __init__.py
│       └── config_loader.py  # 配置加载器
├── styles/
│   └── dark_theme.qss        # QSS 样式文件
└── requirements.txt          # 依赖文件

启动方式：
    python main.py

依赖安装：
    pip install PySide6
"""

import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication
from PySide6.QtCore import Qt

# 确保能导入 core 模块
sys.path.insert(0, str(Path(__file__).parent))

from core.ui.main_window import MainWindow


def load_stylesheet(app: QApplication, style_path: str) -> None:
    """加载 QSS 样式文件"""
    style_file = Path(__file__).parent / style_path
    if style_file.exists():
        with open(style_file, "r", encoding="utf-8") as f:
            app.setStyleSheet(f.read())
    else:
        print(f"[Warning] 样式文件不存在: {style_path}")


def main():
    # 启用高 DPI 支持
    if hasattr(Qt, "AA_EnableHighDpiScaling"):
        QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    if hasattr(Qt, "AA_UseHighDpiPixmaps"):
        QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)

    app = QApplication(sys.argv)
    app.setApplicationName("ImpleForge")
    app.setApplicationDisplayName("ImpleForge - Windows 实施工具箱")

    # 加载样式
    load_stylesheet(app, "styles/dark_theme.qss")

    # 创建并显示主窗口
    window = MainWindow()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
