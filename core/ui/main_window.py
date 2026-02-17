"""
主窗口 - ImpleForge 的核心界面

功能：
- 左右分割布局：左侧树形菜单，右侧内容区
- 基于 JSON 动态生成菜单
- 点击菜单项切换内容区
"""

import json
from typing import Any, Dict, List, Optional

from PySide6.QtWidgets import (
    QMainWindow,
    QWidget,
    QHBoxLayout,
    QVBoxLayout,
    QSplitter,
    QTreeWidget,
    QTreeWidgetItem,
    QStackedWidget,
    QLabel,
    QTextEdit,
    QFrame,
)
from PySide6.QtCore import Qt, QSize
from PySide6.QtGui import QFont

from ..utils.config_loader import ConfigLoader


class MainWindow(QMainWindow):
    """
    ImpleForge 主窗口
    
    布局结构：
    +------------------------------------------+
    |  MainWindow                               |
    |  +--------------------------------------+ |
    |  |  QSplitter (水平)                     | |
    |  |  +----------------+----------------+ | |
    |  |  |  QTreeWidget   | QStackedWidget | | |
    |  |  |  (左侧菜单)     | (右侧内容区)    | | |
    |  |  |                |                | | |
    |  |  +----------------+----------------+ | |
    |  +--------------------------------------+ |
    +------------------------------------------+
    """

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)
        
        # 窗口基本设置
        self.setWindowTitle("ImpleForge - Windows 实施工具箱")
        self.setMinimumSize(800, 600)
        self.resize(1000, 700)
        
        # 存储菜单项数据，用于点击时检索
        # key: item_id, value: 完整的菜单项数据字典
        self._menu_data_map: Dict[str, Dict[str, Any]] = {}
        
        # 初始化 UI
        self._setup_ui()
        
        # 加载菜单配置
        self._load_menu()

    def _setup_ui(self) -> None:
        """设置界面布局"""
        # 创建中央部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        
        # 主布局
        main_layout = QHBoxLayout(central_widget)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # 创建水平分割器
        self.splitter = QSplitter(Qt.Horizontal)
        main_layout.addWidget(self.splitter)
        
        # ========== 左侧：树形菜单 ==========
        self.tree_widget = QTreeWidget()
        self.tree_widget.setHeaderHidden(True)  # 隐藏表头
        self.tree_widget.setColumnCount(1)
        self.tree_widget.setMaximumWidth(300)
        self.tree_widget.setMinimumWidth(200)
        self.tree_widget.itemClicked.connect(self._on_menu_item_clicked)
        
        # 设置树形控件样式增强
        self.tree_widget.setIndentation(20)
        self.tree_widget.setUniformRowHeights(True)
        
        # ========== 右侧：堆叠内容区 ==========
        self.stacked_widget = QStackedWidget()
        
        # 创建默认页面
        self._create_default_pages()
        
        # 添加到分割器
        self.splitter.addWidget(self.tree_widget)
        self.splitter.addWidget(self.stacked_widget)
        
        # 设置分割比例 (左侧:右侧 = 1:3)
        self.splitter.setSizes([250, 750])
        
        # 设置分割器拉伸因子，让右侧随窗口拉伸
        self.splitter.setStretchFactor(0, 0)  # 左侧固定
        self.splitter.setStretchFactor(1, 1)  # 右侧拉伸

    def _create_default_pages(self) -> None:
        """创建默认的内容页面"""
        # 1. 欢迎页面
        welcome_page = self._create_welcome_page()
        self.stacked_widget.addWidget(welcome_page)
        
        # 2. 功能详情页面（动态创建，这里先占位）
        self.page_placeholder = QLabel("请从左侧选择一个功能")
        self.page_placeholder.setAlignment(Qt.AlignCenter)
        font = QFont()
        font.setPointSize(14)
        self.page_placeholder.setFont(font)
        self.stacked_widget.addWidget(self.page_placeholder)

    def _create_welcome_page(self) -> QWidget:
        """创建欢迎页面"""
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(40, 40, 40, 40)
        
        # 标题
        title = QLabel("🛠️ ImpleForge")
        title_font = QFont()
        title_font.setPointSize(24)
        title_font.setBold(True)
        title.setFont(title_font)
        title.setAlignment(Qt.AlignCenter)
        layout.addWidget(title)
        
        # 副标题
        subtitle = QLabel("Windows 实施工具箱")
        subtitle_font = QFont()
        subtitle_font.setPointSize(14)
        subtitle.setFont(subtitle_font)
        subtitle.setAlignment(Qt.AlignCenter)
        subtitle.setStyleSheet("color: #969696; margin-top: 10px;")
        layout.addWidget(subtitle)
        
        # 分隔线
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setStyleSheet("background-color: #333333; max-height: 1px; margin: 30px 0;")
        layout.addWidget(line)
        
        # 说明文字
        desc = QTextEdit()
        desc.setReadOnly(True)
        desc.setHtml("""
        <h3 style="color: #cccccc;">功能概览</h3>
        <ul style="color: #969696; line-height: 1.8;">
            <li><b>网络工具</b> - 网络诊断、Ping 测试、端口扫描</li>
            <li><b>系统工具</b> - 系统信息、服务管理、进程管理</li>
            <li><b>磁盘工具</b> - 磁盘使用分析、垃圾清理</li>
            <li><b>安全工具</b> - 防火墙配置、审计策略查看</li>
            <li><b>部署工具</b> - 软件批量安装、环境配置</li>
        </ul>
        <p style="color: #6e6e6e; margin-top: 20px;">
            提示：从左侧菜单选择功能开始使用
        </p>
        """)
        desc.setStyleSheet("""
            QTextEdit {
                border: none;
                background-color: transparent;
            }
        """)
        layout.addWidget(desc)
        
        layout.addStretch()
        return page

    def _load_menu(self) -> None:
        """加载并渲染菜单配置"""
        try:
            # 获取项目根目录
            import sys
            from pathlib import Path
            
            project_root = Path(__file__).parent.parent.parent
            config_path = project_root / "config" / "menu.json"
            
            # 加载菜单配置
            menu_items = ConfigLoader.load_menu_config(str(config_path))
            
            # 递归构建菜单树
            self._build_menu_tree(menu_items)
            
            # 展开所有节点
            self.tree_widget.expandAll()
            
            print(f"[Info] 菜单加载成功，共 {len(menu_items)} 个分类")
            
        except Exception as e:
            print(f"[Error] 菜单加载失败: {e}")
            # 显示错误信息在树形控件中
            error_item = QTreeWidgetItem(self.tree_widget)
            error_item.setText(0, f"加载失败: {e}")
            error_item.setForeground(0, Qt.red)

    def _build_menu_tree(self, items: List[Dict[str, Any]], parent: Optional[QTreeWidgetItem] = None) -> None:
        """
        递归构建菜单树
        
        Args:
            items: 菜单项列表
            parent: 父节点，为 None 时添加到根
        """
        for item_data in items:
            if not ConfigLoader.validate_menu_item(item_data):
                print(f"[Warning] 跳过无效的菜单项: {item_data}")
                continue
            
            # 创建树节点
            if parent is None:
                tree_item = QTreeWidgetItem(self.tree_widget)
            else:
                tree_item = QTreeWidgetItem(parent)
            
            # 设置显示文本
            display_name = item_data.get("name", "未命名")
            icon = item_data.get("icon", "")
            tree_item.setText(0, f"{icon} {display_name}" if icon else display_name)
            
            # 存储节点数据，用于点击时检索
            item_id = item_data.get("id", display_name)
            self._menu_data_map[id(tree_item)] = item_data
            
            # 设置提示文本
            description = item_data.get("description", "")
            if description:
                tree_item.setToolTip(0, description)
            
            # 递归处理子节点
            children = item_data.get("children", [])
            if children:
                self._build_menu_tree(children, tree_item)
                # 分类节点加粗显示
                font = tree_item.font(0)
                font.setBold(True)
                tree_item.setFont(0, font)

    def _on_menu_item_clicked(self, item: QTreeWidgetItem, column: int) -> None:
        """
        菜单项点击事件处理
        
        Args:
            item: 被点击的树节点
            column: 点击的列
        """
        # 获取节点绑定的数据
        item_data = self._menu_data_map.get(id(item))
        
        if item_data:
            # 打印选中节点的 JSON 数据（验证数据绑定）
            print(f"\n{'='*50}")
            print(f"Selected: {json.dumps(item_data, ensure_ascii=False, indent=2)}")
            print(f"{'='*50}\n")
            
            # 判断节点类型
            node_type = item_data.get("type")
            
            if node_type == "script":
                self._handle_script_selection(item_data)
            elif node_type == "plugin":
                self._handle_plugin_selection(item_data)
            else:
                # 分类节点，仅展开/折叠
                if item.isExpanded():
                    item.setExpanded(False)
                else:
                    item.setExpanded(True)
        else:
            print(f"[Warning] 未找到节点数据: {item.text(0)}")

    def _handle_script_selection(self, item_data: Dict[str, Any]) -> None:
        """处理脚本类型节点选择"""
        action = item_data.get("action", {})
        cmd = action.get("cmd", "")
        
        # 切换到详情页面并显示信息
        page = self._create_detail_page(item_data)
        self.stacked_widget.addWidget(page)
        self.stacked_widget.setCurrentWidget(page)

    def _handle_plugin_selection(self, item_data: Dict[str, Any]) -> None:
        """处理插件类型节点选择"""
        action = item_data.get("action", {})
        plugin_id = action.get("plugin_id", "")
        
        # 切换到详情页面并显示信息
        page = self._create_detail_page(item_data)
        self.stacked_widget.addWidget(page)
        self.stacked_widget.setCurrentWidget(page)

    def _create_detail_page(self, item_data: Dict[str, Any]) -> QWidget:
        """
        根据菜单项数据创建详情页面
        
        Args:
            item_data: 菜单项数据
            
        Returns:
            详情页面部件
        """
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(30, 30, 30, 30)
        
        # 标题
        name = item_data.get("name", "未命名")
        node_type = item_data.get("type", "unknown")
        
        title = QLabel(f"{'▶' if node_type == 'script' else '🔌'} {name}")
        title_font = QFont()
        title_font.setPointSize(18)
        title_font.setBold(True)
        title.setFont(title_font)
        layout.addWidget(title)
        
        # 类型标签
        type_label = QLabel(f"类型: {node_type.upper()}")
        type_label.setStyleSheet("color: #969696; margin-top: 5px;")
        layout.addWidget(type_label)
        
        # 分隔线
        line = QFrame()
        line.setFrameShape(QFrame.HLine)
        line.setStyleSheet("background-color: #333333; max-height: 1px; margin: 20px 0;")
        layout.addWidget(line)
        
        # 详细信息
        info_text = QTextEdit()
        info_text.setReadOnly(True)
        info_text.setHtml(self._format_item_info(item_data))
        info_text.setStyleSheet("""
            QTextEdit {
                border: 1px solid #333333;
                background-color: #252526;
                padding: 15px;
                font-family: 'Consolas', 'Monaco', monospace;
            }
        """)
        layout.addWidget(info_text)
        
        # 占位：这里将来会放置实际的功能界面
        placeholder = QLabel("[功能界面将在 Phase 2 实现]")
        placeholder.setAlignment(Qt.AlignCenter)
        placeholder.setStyleSheet("color: #6e6e6e; padding: 40px;")
        layout.addWidget(placeholder)
        
        layout.addStretch()
        return page

    def _format_item_info(self, item_data: Dict[str, Any]) -> str:
        """格式化菜单项信息为 HTML"""
        action = item_data.get("action", {})
        description = item_data.get("description", "暂无描述")
        
        html = f"""
        <p style="color: #cccccc;"><b>描述:</b> {description}</p>
        <p style="color: #cccccc; margin-top: 15px;"><b>配置详情:</b></p>
        <pre style="background-color: #1e1e1e; padding: 10px; border-radius: 4px; color: #d4d4d4;">
{json.dumps(action, ensure_ascii=False, indent=2)}
        </pre>
        """
        return html
