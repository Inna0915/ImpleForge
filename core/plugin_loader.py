"""
插件加载器 - 动态加载外部插件模块

提供运行时动态导入插件的能力，支持从任意模块路径加载 QWidget 子类。
"""

import importlib
import importlib.util
import sys
from pathlib import Path
from typing import Any, Dict, Optional, Type

from PySide6.QtWidgets import QWidget


def load_plugin(
    module_path: str,
    class_name: str,
    plugin_params: Optional[Dict[str, Any]] = None,
    parent: Optional[QWidget] = None
) -> QWidget:
    """
    动态加载插件模块并实例化 QWidget
    
    加载策略：
    1. 优先尝试从项目 plugins 目录加载（相对路径）
    2. 尝试作为绝对模块路径加载
    3. 尝试从文件路径加载（以 .py 结尾时）
    
    Args:
        module_path: 模块路径，例如 "demo_wizard.wizard" 或 "plugins.demo_wizard.wizard"
        class_name: 要实例化的类名，例如 "DatabaseWizard"
        plugin_params: 传递给插件构造函数的参数字典
        parent: 父窗口部件，将传递给插件构造函数
        
    Returns:
        实例化后的 QWidget 对象
        
    Raises:
        ImportError: 模块导入失败
        AttributeError: 类不存在或不是 QWidget 子类
        TypeError: 实例化参数错误
        
    Example:
        >>> widget = load_plugin("demo_wizard.wizard", "DatabaseWizard", 
        ...                      {"title": "MySQL 配置"}, parent=main_window)
    """
    plugin_params = plugin_params or {}
    
    # 获取项目根目录
    project_root = Path(__file__).parent.parent
    plugins_dir = project_root / "plugins"
    
    # 标准化模块路径
    module_full_path = _resolve_module_path(module_path, plugins_dir)
    
    try:
        # 动态导入模块
        module = _import_module_dynamically(module_full_path, plugins_dir)
        
        # 获取类对象
        if not hasattr(module, class_name):
            raise AttributeError(
                f"模块 '{module_path}' 中不存在类 '{class_name}'，"
                f"可用属性: {dir(module)}"
            )
        
        plugin_class = getattr(module, class_name)
        
        # 验证是否为 QWidget 子类
        if not isinstance(plugin_class, type) or not issubclass(plugin_class, QWidget):
            raise TypeError(
                f"'{class_name}' 必须是 QWidget 的子类，"
                f"实际类型: {type(plugin_class)}"
            )
        
        # 准备构造函数参数
        init_params = dict(plugin_params)
        if parent is not None:
            init_params['parent'] = parent
        
        # 实例化插件
        instance = plugin_class(**init_params)
        
        return instance
        
    except ImportError as e:
        raise ImportError(f"加载插件失败 '{module_path}.{class_name}': {e}")
    except Exception as e:
        raise RuntimeError(f"实例化插件失败 '{module_path}.{class_name}': {e}")


def _resolve_module_path(module_path: str, plugins_dir: Path) -> str:
    """
    解析并标准化模块路径
    
    处理逻辑：
    - 如果路径以 .py 结尾，转换为模块路径
    - 如果 plugins.xxx 格式，保持原样
    - 如果 xxx.yyy 格式，尝试添加 plugins 前缀
    
    Args:
        module_path: 原始模块路径
        plugins_dir: 插件目录路径
        
    Returns:
        标准化的模块路径
    """
    # 如果以 .py 结尾，转换为模块路径
    if module_path.endswith('.py'):
        path_obj = Path(module_path)
        # 移除 .py 后缀
        module_path = str(path_obj.with_suffix(''))
        # 转换路径分隔符为点
        module_path = module_path.replace('/', '.').replace('\\', '.')
    
    # 如果已经是 plugins.xxx 开头，保持原样
    if module_path.startswith('plugins.'):
        return module_path
    
    # 如果路径包含 plugins/ 前缀，移除它
    if module_path.startswith('plugins/'):
        module_path = module_path[8:]  # len('plugins/') == 8
    
    return module_path


def _import_module_dynamically(module_path: str, plugins_dir: Path) -> Any:
    """
    动态导入模块
    
    尝试顺序：
    1. 作为已安装的包导入
    2. 从 plugins 目录导入
    3. 从文件路径导入
    
    Args:
        module_path: 模块路径，如 "demo_wizard.wizard"
        plugins_dir: 插件目录
        
    Returns:
        导入的模块对象
    """
    # 确保 plugins 目录在 sys.path 中
    if str(plugins_dir) not in sys.path:
        sys.path.insert(0, str(plugins_dir))
    
    # 尝试直接导入
    try:
        return importlib.import_module(module_path)
    except ModuleNotFoundError:
        pass
    
    # 尝试添加 plugins. 前缀后导入
    if not module_path.startswith('plugins.'):
        try:
            return importlib.import_module(f"plugins.{module_path}")
        except ModuleNotFoundError:
            pass
    
    # 尝试从文件路径导入
    file_path = plugins_dir / module_path.replace('.', '/') / "__init__.py"
    if not file_path.exists():
        file_path = plugins_dir / f"{module_path.replace('.', '/')}.py"
    
    if file_path.exists():
        return _import_from_file(file_path, module_path)
    
    raise ModuleNotFoundError(f"无法找到模块: {module_path}")


def _import_from_file(file_path: Path, module_name: str) -> Any:
    """
    从文件路径导入模块
    
    Args:
        file_path: Python 文件路径
        module_name: 模块名称
        
    Returns:
        导入的模块对象
    """
    # 确保父目录在 sys.path 中
    parent_dir = str(file_path.parent)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    
    # 创建模块规范
    spec = importlib.util.spec_from_file_location(
        module_name.replace('/', '.').replace('\\', '.'),
        file_path
    )
    
    if spec is None or spec.loader is None:
        raise ImportError(f"无法从文件加载模块: {file_path}")
    
    # 加载模块
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    
    return module


class PluginInfo:
    """
    插件信息封装类
    
    用于描述插件的元数据信息
    """
    
    def __init__(
        self,
        name: str,
        module_path: str,
        class_name: str,
        version: str = "1.0.0",
        description: str = "",
        author: str = "",
        dependencies: Optional[list] = None
    ):
        self.name = name
        self.module_path = module_path
        self.class_name = class_name
        self.version = version
        self.description = description
        self.author = author
        self.dependencies = dependencies or []
    
    def load(self, parent: Optional[QWidget] = None, **kwargs) -> QWidget:
        """加载并实例化插件"""
        return load_plugin(
            self.module_path,
            self.class_name,
            plugin_params=kwargs,
            parent=parent
        )
    
    def __repr__(self) -> str:
        return f"<PluginInfo '{self.name}' v{self.version} ({self.module_path}.{self.class_name})>"
