"""
配置加载器 - 负责读取和解析 JSON 配置文件
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional


class ConfigLoader:
    """JSON 配置文件加载器"""

    @staticmethod
    def load_json(config_path: str) -> Dict[str, Any]:
        """
        加载 JSON 配置文件
        
        Args:
            config_path: JSON 文件路径
            
        Returns:
            解析后的字典数据
            
        Raises:
            FileNotFoundError: 文件不存在
            json.JSONDecodeError: JSON 格式错误
        """
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"配置文件不存在: {config_path}")

        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def load_menu_config(config_path: str = "config/menu.json") -> List[Dict[str, Any]]:
        """
        加载菜单配置
        
        Args:
            config_path: 菜单配置文件路径，默认为 config/menu.json
            
        Returns:
            菜单项列表
        """
        data = ConfigLoader.load_json(config_path)
        
        # 支持两种格式：直接数组或 {"menu": [...]} 对象
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "menu" in data:
            return data["menu"]
        else:
            raise ValueError("菜单配置格式错误：应为数组或包含 'menu' 键的对象")

    @staticmethod
    def validate_menu_item(item: Dict[str, Any]) -> bool:
        """
        验证菜单项格式
        
        Args:
            item: 菜单项字典
            
        Returns:
            是否有效
        """
        if not isinstance(item, dict):
            return False
        
        # 必须包含 name 字段
        if "name" not in item:
            return False
            
        return True
