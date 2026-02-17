"""
YAML 配置导入器 - 智能解析数据库连接信息

支持从 Spring Boot application.yml 或通用 YAML 配置文件中
自动提取数据库连接参数。

依赖安装:
    pip install PyYAML

使用示例:
    from core.importers.yaml_importer import YamlConfigImporter
    
    importer = YamlConfigImporter()
    config = importer.parse("application.yml")
    # config = {'type': 'mysql', 'host': 'localhost', 'port': 3306, ...}
"""

import re
from pathlib import Path
from typing import Any, Dict, Optional, List, Tuple


class YamlConfigImporter:
    """
    YAML 数据库配置导入器
    
    支持解析：
    - Spring Boot application.yml
    - Django settings.yaml
    - 通用数据库配置文件
    
    解析策略（按优先级）：
    1. JDBC URL 解析（最准确）
    2. 标准键名匹配（host, port, username, password）
    3. 环境变量引用处理
    """
    
    # JDBC URL 正则表达式模式
    JDBC_PATTERNS = {
        'mysql': {
            'pattern': re.compile(
                r'jdbc:mysql://(?P<host>[^:/]+)(:(?P<port>\d+))?/(?P<database>[^?]+)',
                re.IGNORECASE
            ),
            'default_port': 3306
        },
        'mariadb': {
            'pattern': re.compile(
                r'jdbc:mariadb://(?P<host>[^:/]+)(:(?P<port>\d+))?/(?P<database>[^?]+)',
                re.IGNORECASE
            ),
            'default_port': 3306
        },
        'oracle_service': {
            'pattern': re.compile(
                r'jdbc:oracle:thin:@//(?P<host>[^:/]+)(:(?P<port>\d+))?/(?P<service_name>[^\s:]+)',
                re.IGNORECASE
            ),
            'default_port': 1521,
            'mode': 'service_name'
        },
        'oracle_sid': {
            'pattern': re.compile(
                r'jdbc:oracle:thin:@(?P<host>[^:/]+)(:(?P<port>\d+))?:(?P<sid>[^\s:]+)',
                re.IGNORECASE
            ),
            'default_port': 1521,
            'mode': 'sid'
        },
        'sqlserver': {
            'pattern': re.compile(
                r'jdbc:sqlserver://(?P<host>[^:/;]+)(:(?P<port>\d+))?(;.*)?',
                re.IGNORECASE
            ),
            'default_port': 1433,
            'db_pattern': re.compile(r'databaseName=(?P<database>[^;]+)', re.IGNORECASE)
        },
        'postgresql': {
            'pattern': re.compile(
                r'jdbc:postgresql://(?P<host>[^:/]+)(:(?P<port>\d+))?/(?P<database>[^?]+)',
                re.IGNORECASE
            ),
            'default_port': 5432
        }
    }
    
    # 键名映射：可能的键名 -> 标准键名
    KEY_MAPPINGS = {
        'host': ['host', 'hostname', 'server', 'db_host', 'database_host', 'host_name'],
        'port': ['port', 'db_port'],
        'username': ['username', 'user', 'db_user', 'database_user', 'db_username'],
        'password': ['password', 'pass', 'db_pass', 'db_password', 'database_password'],
        'database': ['database', 'dbname', 'db_name', 'schema', 'database_name'],
        'auth_source': ['auth_source', 'authsource', 'authentication_database']
    }
    
    def __init__(self):
        self.raw_data: Dict[str, Any] = {}
        self.flat_data: Dict[str, str] = {}
    
    def parse(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        解析 YAML 文件并提取数据库配置
        
        Args:
            file_path: YAML 文件路径
            
        Returns:
            标准配置字典，解析失败返回 None
            
        Example:
            >>> config = importer.parse("application.yml")
            >>> print(config)
            {
                'type': 'mysql',
                'host': 'localhost',
                'port': 3306,
                'username': 'root',
                'password': 'password',
                'database': 'test_db'
            }
        """
        try:
            import yaml
        except ImportError:
            raise ImportError("请先安装 PyYAML: pip install PyYAML")
        
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        # 读取 YAML
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                self.raw_data = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise ValueError(f"YAML 格式错误: {e}")
        
        if not self.raw_data:
            raise ValueError("YAML 文件为空")
        
        # 扁平化数据
        self.flat_data = self._flatten_dict(self.raw_data)
        
        # 尝试 JDBC URL 解析（最准确）
        config = self._parse_jdbc_url()
        if config:
            return config
        
        # 回退到关键字匹配
        config = self._parse_by_keywords()
        if config:
            return config
        
        raise ValueError("未找到有效的数据库连接信息\n请确保文件包含 JDBC URL 或 host/port/user 等字段")
    
    def _flatten_dict(self, data: Dict, parent_key: str = '', sep: str = '.') -> Dict[str, str]:
        """
        将嵌套字典扁平化
        
        例如: {'spring': {'datasource': {'url': 'xxx'}}} 
              -> {'spring.datasource.url': 'xxx'}
        """
        items = []
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep).items())
            else:
                # 将所有值转为字符串
                items.append((new_key, str(v) if v is not None else ''))
        return dict(items)
    
    def _parse_jdbc_url(self) -> Optional[Dict[str, Any]]:
        """
        从扁平化数据中查找并解析 JDBC URL
        
        Returns:
            解析后的配置字典，未找到返回 None
        """
        # 查找包含 jdbc: 的字段
        jdbc_urls = []
        for key, value in self.flat_data.items():
            if 'jdbc:' in str(value).lower():
                jdbc_urls.append((key, value))
        
        if not jdbc_urls:
            return None
        
        # 优先使用 url/jdbc-url 字段，其次使用第一个找到的
        selected_url = None
        for key, value in jdbc_urls:
            if 'url' in key.lower() and 'jdbc' in str(value).lower():
                selected_url = value
                break
        
        if not selected_url:
            selected_url = jdbc_urls[0][1]
        
        return self._extract_from_jdbc_url(selected_url)
    
    def _extract_from_jdbc_url(self, jdbc_url: str) -> Optional[Dict[str, Any]]:
        """
        使用正则解析 JDBC URL
        
        Args:
            jdbc_url: JDBC 连接字符串
            
        Returns:
            配置字典
        """
        jdbc_url = str(jdbc_url).strip()
        
        # 移除环境变量占位符（如 ${DB_HOST:localhost}）
        jdbc_url_clean = re.sub(r'\$\{[^}]+\}', '', jdbc_url)
        
        for db_type, config in self.JDBC_PATTERNS.items():
            match = config['pattern'].search(jdbc_url_clean)
            if match:
                result = match.groupdict()
                
                # 构建标准配置
                parsed = {
                    'type': 'oracle' if 'oracle' in db_type else 
                            'sqlserver' if 'sqlserver' in db_type else
                            db_type.replace('_service', '').replace('_sid', ''),
                    'host': result.get('host', 'localhost'),
                    'port': int(result['port']) if result.get('port') else config['default_port'],
                }
                
                # Oracle 特殊处理
                if 'oracle' in db_type:
                    parsed['oracle_mode'] = config.get('mode', 'service_name')
                    if config.get('mode') == 'sid':
                        parsed['oracle_value'] = result.get('sid', 'ORCL')
                        parsed['database'] = result.get('sid', 'ORCL')
                    else:
                        parsed['oracle_value'] = result.get('service_name', 'ORCL')
                        parsed['database'] = result.get('service_name', 'ORCL')
                
                # SQL Server 数据库名处理
                elif 'sqlserver' in db_type:
                    db_match = config.get('db_pattern', re.compile('')).search(jdbc_url_clean)
                    if db_match:
                        parsed['database'] = db_match.group('database')
                    else:
                        parsed['database'] = ''
                else:
                    parsed['database'] = result.get('database', '').split('?')[0]  # 移除 URL 参数
                
                # 查找对应的用户名和密码
                parsed.update(self._find_credentials(db_type))
                
                return parsed
        
        return None
    
    def _find_credentials(self, db_type_hint: str) -> Dict[str, str]:
        """
        在扁平化数据中查找用户名和密码
        
        Args:
            db_type_hint: 数据库类型提示
            
        Returns:
            包含 username 和 password 的字典
        """
        result = {'username': '', 'password': ''}
        
        # 查找用户名
        for std_key, possible_keys in self.KEY_MAPPINGS.items():
            if std_key in ['username', 'password']:
                for key in possible_keys:
                    # 在扁平化数据中查找
                    for flat_key, value in self.flat_data.items():
                        if key.lower() in flat_key.lower() and value:
                            # 处理环境变量引用
                            clean_value = self._clean_env_var(value)
                            if std_key == 'username':
                                result['username'] = clean_value
                            elif std_key == 'password':
                                result['password'] = clean_value
                            break
        
        return result
    
    def _parse_by_keywords(self) -> Optional[Dict[str, Any]]:
        """
        通过关键字匹配解析配置（JDBC URL 解析失败后的回退）
        
        Returns:
            配置字典
        """
        config = {
            'type': 'mysql',  # 默认类型
            'host': 'localhost',
            'port': 3306,
            'username': '',
            'password': '',
            'database': ''
        }
        
        found_any = False
        
        for std_key, possible_keys in self.KEY_MAPPINGS.items():
            for key in possible_keys:
                for flat_key, value in self.flat_data.items():
                    if key.lower() in flat_key.lower() and value:
                        clean_value = self._clean_env_var(value)
                        if clean_value:  # 只记录非空值
                            config[std_key] = clean_value
                            found_any = True
                            
                            # 尝试推断数据库类型
                            if 'oracle' in flat_key.lower():
                                config['type'] = 'oracle'
                                config['port'] = 1521
                            elif 'postgres' in flat_key.lower():
                                config['type'] = 'postgresql'
                                config['port'] = 5432
                            elif 'sqlserver' in flat_key.lower() or 'mssql' in flat_key.lower():
                                config['type'] = 'sqlserver'
                                config['port'] = 1433
                            elif 'mongo' in flat_key.lower():
                                config['type'] = 'mongodb'
                                config['port'] = 27017
                        break
        
        # 尝试将 port 转为整数
        if config.get('port'):
            try:
                config['port'] = int(config['port'])
            except ValueError:
                config['port'] = 3306
        
        return config if found_any else None
    
    def _clean_env_var(self, value: str) -> str:
        """
        清理环境变量引用
        
        例如:
            ${DB_PASSWORD} -> ''
            ${DB_PASSWORD:default} -> 'default'
            ${DB_HOST:localhost} -> 'localhost'
        
        Args:
            value: 原始值
            
        Returns:
            清理后的值
        """
        if not value:
            return ''
        
        # 匹配 ${VAR} 或 ${VAR:default}
        match = re.match(r'^\$\{([^}:]+)(?::([^}]+))?\}$', str(value).strip())
        if match:
            var_name = match.group(1)
            default_value = match.group(2)
            
            # 如果有默认值，使用默认值
            if default_value:
                return default_value
            else:
                # 纯环境变量引用，返回空（需要用户手动填写）
                return ''
        
        return str(value)
    
    def get_raw_data(self) -> Dict[str, Any]:
        """获取原始解析的 YAML 数据"""
        return self.raw_data
    
    def get_flat_data(self) -> Dict[str, str]:
        """获取扁平化后的数据"""
        return self.flat_data


# 便捷函数
def import_yaml_config(file_path: str) -> Dict[str, Any]:
    """
    便捷函数：导入 YAML 配置
    
    Args:
        file_path: YAML 文件路径
        
    Returns:
        配置字典
        
    Raises:
        ImportError: 未安装 PyYAML
        FileNotFoundError: 文件不存在
        ValueError: 解析失败
    """
    importer = YamlConfigImporter()
    return importer.parse(file_path)
