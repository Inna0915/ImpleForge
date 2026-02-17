"""
连接配置管理器 - 负责加载和保存数据库连接配置

使用方法:
    from core.managers.connection_manager import ConnectionManager
    
    manager = ConnectionManager()
    profiles = manager.load_profiles()
    manager.save_profile("prod_mysql", "192.168.1.100", 3306, "admin", "password", "mysql")

依赖安装 (Phase 5 扩展):
    # 基础依赖
    pip install sqlalchemy pymysql
    
    # Oracle 支持 (Oracle 12c+ 使用 thin mode，无需 instant client)
    pip install oracledb
    
    # SQL Server 支持
    pip install pymssql
    
    # MongoDB 支持
    pip install pymongo

注意:
    当前版本密码使用明文存储，生产环境建议使用加密存储 (如 keyring 库或系统密钥管理)
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime


class ConnectionManager:
    """
    数据库连接配置管理器
    
    功能:
    - 加载和保存连接配置到 JSON 文件
    - 支持多配置文件管理
    - 自动创建默认配置文件
    
    存储格式:
    {
      "version": "1.0",
      "last_updated": "2024-01-15T10:30:00",
      "profiles": [
        {
          "id": "uuid-string",
          "name": "本地MySQL",
          "host": "localhost",
          "port": 3306,
          "username": "root",
          "password": "plaintext_password",
          "db_type": "mysql",
          "database": "test",
          "created_at": "2024-01-15T10:30:00",
          "updated_at": "2024-01-15T10:30:00"
        }
      ]
    }
    """
    
    # 默认配置文件路径
    DEFAULT_CONFIG_PATH = "config/connections.json"
    
    # 支持的数据库类型 (Phase 5 扩展)
    # mysql: MySQL 5.7+ / 8.0+
    # mariadb: MariaDB 10.3+
    # sqlserver: SQL Server 2016+ (使用 pymssql)
    # oracle: Oracle 12c+ (使用 oracledb thin mode)
    # mongodb: MongoDB 4.0+ (使用 pymongo)
    SUPPORTED_DB_TYPES = [
        "mysql",
        "mariadb", 
        "sqlserver",
        "oracle",
        "mongodb"
    ]
    
    # 数据库类型显示名称映射
    DB_TYPE_DISPLAY_NAMES = {
        "mysql": "MySQL",
        "mariadb": "MariaDB",
        "sqlserver": "SQL Server",
        "oracle": "Oracle",
        "mongodb": "MongoDB"
    }
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化连接管理器
        
        Args:
            config_path: 配置文件路径，默认使用 config/connections.json
        """
        if config_path:
            self.config_file = Path(config_path)
        else:
            # 获取项目根目录
            project_root = Path(__file__).parent.parent.parent
            self.config_file = project_root / self.DEFAULT_CONFIG_PATH
        
        # 确保配置目录存在
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        
        # 缓存数据
        self._profiles: List[Dict[str, Any]] = []
        self._config: Dict[str, Any] = {"version": "1.0", "profiles": []}
        
        # 加载现有配置
        self._load_config()
    
    def _load_config(self) -> None:
        """从文件加载配置"""
        if not self.config_file.exists():
            # 创建默认空配置
            self._save_to_file()
            return
        
        try:
            with open(self.config_file, "r", encoding="utf-8") as f:
                self._config = json.load(f)
            
            self._profiles = self._config.get("profiles", [])
            
        except json.JSONDecodeError as e:
            print(f"[Warning] 配置文件格式错误: {e}，将创建新配置")
            self._config = {"version": "1.0", "profiles": []}
            self._profiles = []
            self._save_to_file()
            
        except Exception as e:
            print(f"[Error] 加载配置失败: {e}")
            self._profiles = []
    
    def _save_to_file(self) -> bool:
        """
        保存配置到文件
        
        Returns:
            是否保存成功
        """
        try:
            self._config["last_updated"] = datetime.now().isoformat()
            self._config["profiles"] = self._profiles
            
            with open(self.config_file, "w", encoding="utf-8") as f:
                json.dump(self._config, f, ensure_ascii=False, indent=2)
            
            return True
            
        except Exception as e:
            print(f"[Error] 保存配置失败: {e}")
            return False
    
    def load_profiles(self) -> List[Dict[str, Any]]:
        """
        加载所有连接配置
        
        Returns:
            连接配置列表
        """
        # 重新加载以获取最新数据
        self._load_config()
        return self._profiles.copy()
    
    def get_profile(self, name_or_id: str) -> Optional[Dict[str, Any]]:
        """
        根据名称或 ID 获取特定配置
        
        Args:
            name_or_id: 配置名称或 ID
            
        Returns:
            配置字典，未找到返回 None
        """
        for profile in self._profiles:
            if profile.get("name") == name_or_id or profile.get("id") == name_or_id:
                return profile.copy()
        return None
    
    def save_profile(
        self,
        name: str,
        host: str,
        port: int,
        username: str,
        password: str,
        db_type: str = "mysql",
        database: str = "",
        profile_id: Optional[str] = None,
        # Phase 7 新增字段
        auth_source: str = "",
        oracle_mode: str = "",
        oracle_value: str = ""
    ) -> bool:
        """
        保存连接配置 - Phase 7 更新版
        
        Args:
            name: 配置名称
            host: 主机地址
            port: 端口号
            username: 用户名
            password: 密码
            db_type: 数据库类型
            database: 数据库名
            profile_id: 配置 ID
            auth_source: MongoDB 认证源
            oracle_mode: Oracle 连接模式 ('service_name' 或 'sid')
            oracle_value: Oracle Service Name 或 SID 值
            
        Returns:
            是否保存成功
            - 系统密钥管理服务 (Windows Credential / macOS Keychain)
            - 或至少使用对称加密 + 环境变量存储密钥
        """
        import uuid
        
        # 验证数据库类型
        if db_type not in self.SUPPORTED_DB_TYPES:
            print(f"[Warning] 不支持的数据库类型: {db_type}，将使用 mysql")
            db_type = "mysql"
        
        now = datetime.now().isoformat()
        
        # 检查是否已存在同名配置
        existing_idx = None
        for idx, profile in enumerate(self._profiles):
            if profile.get("name") == name:
                existing_idx = idx
                break
            if profile_id and profile.get("id") == profile_id:
                existing_idx = idx
                break
        
        new_profile = {
            "id": profile_id or str(uuid.uuid4())[:8],
            "name": name,
            "host": host,
            "port": port,
            "username": username,
            "password": password,  # TODO: 生产环境应加密存储
            "db_type": db_type,
            "database": database,
            "updated_at": now,
            # Phase 7 新增字段
            "auth_source": auth_source,
            "oracle_mode": oracle_mode,
            "oracle_value": oracle_value
        }
        
        if existing_idx is not None:
            # 更新现有配置，保留创建时间
            new_profile["created_at"] = self._profiles[existing_idx].get("created_at", now)
            self._profiles[existing_idx] = new_profile
            print(f"[Info] 更新连接配置: {name}")
        else:
            # 创建新配置
            new_profile["created_at"] = now
            self._profiles.append(new_profile)
            print(f"[Info] 新建连接配置: {name}")
        
        return self._save_to_file()
    
    def delete_profile(self, name_or_id: str) -> bool:
        """
        删除连接配置
        
        Args:
            name_or_id: 配置名称或 ID
            
        Returns:
            是否删除成功
        """
        original_len = len(self._profiles)
        self._profiles = [
            p for p in self._profiles 
            if p.get("name") != name_or_id and p.get("id") != name_or_id
        ]
        
        if len(self._profiles) < original_len:
            print(f"[Info] 删除连接配置: {name_or_id}")
            return self._save_to_file()
        
        return False
    
    def get_profile_names(self) -> List[str]:
        """
        获取所有配置名称列表
        
        Returns:
            配置名称列表
        """
        return [p.get("name", "未命名") for p in self._profiles]
    
    def export_to_dict(self, include_passwords: bool = False) -> Dict[str, Any]:
        """
        导出配置为字典
        
        Args:
            include_passwords: 是否包含密码（导出分享时建议设为 False）
            
        Returns:
            配置字典
        """
        export_config = self._config.copy()
        
        if not include_passwords:
            # 移除密码字段
            export_profiles = []
            for profile in export_config.get("profiles", []):
                profile_copy = profile.copy()
                profile_copy.pop("password", None)
                export_profiles.append(profile_copy)
            export_config["profiles"] = export_profiles
        
        return export_config
    
    def import_from_dict(self, config_dict: Dict[str, Any], merge: bool = False) -> int:
        """
        从字典导入配置
        
        Args:
            config_dict: 配置字典
            merge: 是否合并（True=合并，False=覆盖）
            
        Returns:
            导入的配置数量
        """
        imported_profiles = config_dict.get("profiles", [])
        
        if not merge:
            self._profiles = []
        
        count = 0
        for profile in imported_profiles:
            # 生成新 ID 避免冲突
            profile["id"] = None  # 让 save_profile 生成新 ID
            success = self.save_profile(
                name=profile.get("name", "未命名"),
                host=profile.get("host", "localhost"),
                port=profile.get("port", 3306),
                username=profile.get("username", ""),
                password=profile.get("password", ""),
                db_type=profile.get("db_type", "mysql"),
                database=profile.get("database", "")
            )
            if success:
                count += 1
        
        return count
