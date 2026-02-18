"""
YAML 配置批量导入器 - Phase 8 (全量解析)

支持从单个 YAML 文件中自动识别并提取所有数据库连接配置：
- Redis (host, port, password, database)
- Elasticsearch (uris, username, password)
- JDBC (MySQL, Oracle, SQL Server, MariaDB, PostgreSQL)
- MongoDB (mongodb:// URI)

依赖:
    pip install PyYAML

使用示例:
    from core.importers.yaml_importer import YamlConfigImporter
    
    importer = YamlConfigImporter()
    configs = importer.parse_all("application.yml")
    # configs = [
    #   {'name': 'spring.redis', 'type': 'redis', 'host': 'localhost', ...},
    #   {'name': 'spring.datasource', 'type': 'mysql', 'host': 'localhost', ...},
    #   {'name': 'elasticsearch', 'type': 'elasticsearch', 'host': 'es.host', ...}
    # ]
"""

import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse


class YamlConfigImporter:
    """
    YAML 数据库配置批量导入器
    
    自动遍历 YAML 文件，识别并提取所有数据库连接信息。
    """
    
    # JDBC URL 正则表达式模式
    JDBC_PATTERNS = {
        'mysql': {
            'pattern': re.compile(
                r'jdbc:mysql://(?P<host>[^:/]+)(:(?P<port>\d+))?/(?P<database>[^?&]+)',
                re.IGNORECASE
            ),
            'default_port': 3306,
            'type': 'mysql'
        },
        'mariadb': {
            'pattern': re.compile(
                r'jdbc:mariadb://(?P<host>[^:/]+)(:(?P<port>\d+))?/(?P<database>[^?&]+)',
                re.IGNORECASE
            ),
            'default_port': 3306,
            'type': 'mariadb'
        },
        'postgresql': {
            'pattern': re.compile(
                r'jdbc:postgresql://(?P<host>[^:/]+)(:(?P<port>\d+))?/(?P<database>[^?&]+)',
                re.IGNORECASE
            ),
            'default_port': 5432,
            'type': 'postgresql'
        },
        'oracle_service': {
            'pattern': re.compile(
                r'jdbc:oracle:thin:@//(?P<host>[^:/]+)(:(?P<port>\d+))?/(?P<service>[^?&/]+)',
                re.IGNORECASE
            ),
            'default_port': 1521,
            'type': 'oracle',
            'mode': 'service_name'
        },
        'oracle_sid': {
            'pattern': re.compile(
                r'jdbc:oracle:thin:@(?P<host>[^:/]+)(:(?P<port>\d+))?:(?P<sid>[^?&/:]+)',
                re.IGNORECASE
            ),
            'default_port': 1521,
            'type': 'oracle',
            'mode': 'sid'
        },
        'sqlserver': {
            'pattern': re.compile(
                r'jdbc:sqlserver://(?P<host>[^:;]+)(:(?P<port>\d+))?',
                re.IGNORECASE
            ),
            'default_port': 1433,
            'type': 'sqlserver',
            'db_pattern': re.compile(r'databaseName=(?P<database>[^;]+)', re.IGNORECASE)
        }
    }
    
    # MongoDB URI 模式
    MONGODB_PATTERN = re.compile(
        r'mongodb://((?P<username>[^:]+):(?P<password>[^@]+)@)?(?P<host>[^:/]+)(:(?P<port>\d+))?(/(?P<database>[^?]+))?',
        re.IGNORECASE
    )
    
    # Redis 配置键名模式
    REDIS_KEYS = {
        'host': ['host', 'hostname'],
        'port': ['port'],
        'password': ['password', 'pass'],
        'database': ['database', 'db', 'index']
    }
    
    # Elasticsearch 配置键名模式
    ES_KEYS = {
        'uris': ['uris', 'uris', 'nodes', 'hosts'],
        'username': ['username', 'user'],
        'password': ['password', 'pass']
    }
    
    def __init__(self):
        self.raw_data: Dict[str, Any] = {}
        self.discovered_configs: List[Dict[str, Any]] = []
    
    def parse_all(self, file_path: str) -> List[Dict[str, Any]]:
        """
        解析 YAML 文件并返回所有发现的数据库连接配置
        
        Args:
            file_path: YAML 文件路径
            
        Returns:
            配置列表，每个配置是完整的 ConnectionManager 格式
            
        Raises:
            ImportError: 未安装 PyYAML
            FileNotFoundError: 文件不存在
            ValueError: 解析失败
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
        
        self.discovered_configs = []
        
        # 递归遍历 YAML 结构
        self._traverse_yaml(self.raw_data, [])
        
        if not self.discovered_configs:
            raise ValueError("未找到任何数据库连接配置\n请确保文件包含 Redis, Elasticsearch 或 JDBC URL 等配置")
        
        return self.discovered_configs
    
    def _traverse_yaml(self, data: Any, path: List[str]) -> None:
        """
        递归遍历 YAML 数据结构
        
        Args:
            data: 当前数据节点
            path: 当前路径（键名列表）
        """
        if isinstance(data, dict):
            # 检查当前层级是否是一个完整的连接配置
            config = self._try_extract_config(data, path)
            if config:
                # 生成配置名称（基于路径）
                config['name'] = self._generate_name(path)
                self.discovered_configs.append(config)
            
            # 继续递归遍历子节点
            for key, value in data.items():
                new_path = path + [key]
                self._traverse_yaml(value, new_path)
        
        elif isinstance(data, list):
            # 处理列表（如 Elasticsearch 的多个节点）
            for i, item in enumerate(data):
                if isinstance(item, dict):
                    new_path = path + [str(i)]
                    self._traverse_yaml(item, new_path)
    
    def _try_extract_config(self, data: Dict, path: List[str]) -> Optional[Dict[str, Any]]:
        """
        尝试从当前字典提取数据库配置
        
        Args:
            data: 当前字典
            path: 路径
            
        Returns:
            配置字典，不是数据库配置返回 None
        """
        if not isinstance(data, dict):
            return None
        
        # 1. 检查是否有 JDBC URL
        for key, value in data.items():
            if isinstance(value, str) and 'jdbc:' in value.lower():
                return self._parse_jdbc_config(data, path)
        
        # 2. 检查 Redis 配置
        if self._is_redis_config(data):
            return self._parse_redis_config(data)
        
        # 3. 检查 Elasticsearch 配置
        if self._is_elasticsearch_config(data):
            return self._parse_elasticsearch_config(data)
        
        # 4. 检查 MongoDB URI
        for key, value in data.items():
            if isinstance(value, str) and value.startswith('mongodb://'):
                return self._parse_mongodb_config(data)
        
        return None
    
    def _is_redis_config(self, data: Dict) -> bool:
        """检查是否是 Redis 配置"""
        has_host = any(k in data for k in self.REDIS_KEYS['host'])
        has_port = any(k in data for k in self.REDIS_KEYS['port'])
        return has_host and has_port
    
    def _is_elasticsearch_config(self, data: Dict) -> bool:
        """检查是否是 Elasticsearch 配置"""
        has_uris = any(k in data for k in self.ES_KEYS['uris'])
        return has_uris
    
    def _parse_jdbc_config(self, data: Dict, path: List[str]) -> Optional[Dict[str, Any]]:
        """解析 JDBC 配置"""
        # 查找 JDBC URL
        jdbc_url = None
        for key, value in data.items():
            if isinstance(value, str) and 'jdbc:' in value.lower():
                jdbc_url = value
                break
        
        if not jdbc_url:
            return None
        
        # 解析 URL
        parsed = self._extract_from_jdbc_url(jdbc_url)
        if not parsed:
            return None
        
        # 补充其他字段
        config = {
            'db_type': parsed['type'],
            'host': parsed['host'],
            'port': parsed['port'],
            'username': '',
            'password': '',
            'database': parsed.get('database', ''),
            'auth_source': '',
            'oracle_mode': parsed.get('oracle_mode', ''),
            'oracle_value': parsed.get('oracle_value', '')
        }
        
        # 查找用户名密码
        for key, value in data.items():
            if isinstance(value, str):
                key_lower = key.lower()
                if key_lower in ['username', 'user']:
                    config['username'] = self._clean_env_var(value)
                elif key_lower in ['password', 'pass']:
                    config['password'] = self._clean_env_var(value)
        
        return config
    
    def _extract_from_jdbc_url(self, jdbc_url: str) -> Optional[Dict[str, Any]]:
        """解析 JDBC URL"""
        jdbc_url = str(jdbc_url).strip()
        
        # 清理环境变量占位符
        jdbc_url_clean = re.sub(r'\$\{[^}]+\}', '', jdbc_url)
        
        for db_key, config in self.JDBC_PATTERNS.items():
            match = config['pattern'].search(jdbc_url_clean)
            if match:
                result = match.groupdict()
                
                parsed = {
                    'type': config['type'],
                    'host': result.get('host', 'localhost'),
                    'port': int(result['port']) if result.get('port') else config['default_port']
                }
                
                # Oracle 特殊处理
                if config['type'] == 'oracle':
                    parsed['oracle_mode'] = config.get('mode', 'service_name')
                    if config.get('mode') == 'sid':
                        parsed['oracle_value'] = result.get('sid', 'ORCL')
                        parsed['database'] = result.get('sid', 'ORCL')
                    else:
                        parsed['oracle_value'] = result.get('service', 'ORCL')
                        parsed['database'] = result.get('service', 'ORCL')
                
                # SQL Server 数据库名处理
                elif config['type'] == 'sqlserver':
                    db_match = config.get('db_pattern', re.compile('')).search(jdbc_url_clean)
                    if db_match:
                        parsed['database'] = db_match.group('database')
                    else:
                        parsed['database'] = ''
                else:
                    parsed['database'] = result.get('database', '').split('?')[0]
                
                return parsed
        
        return None
    
    def _parse_redis_config(self, data: Dict) -> Dict[str, Any]:
        """解析 Redis 配置"""
        config = {
            'db_type': 'redis',
            'host': 'localhost',
            'port': 6379,
            'username': '',
            'password': '',
            'database': '0',
            'auth_source': '',
            'oracle_mode': '',
            'oracle_value': ''
        }
        
        # 提取字段
        for key, value in data.items():
            if not isinstance(value, (str, int)):
                continue
            
            key_lower = key.lower()
            value_str = str(value)
            
            if key_lower in self.REDIS_KEYS['host']:
                config['host'] = self._clean_env_var(value_str) or 'localhost'
            elif key_lower in self.REDIS_KEYS['port']:
                try:
                    config['port'] = int(value)
                except:
                    config['port'] = 6379
            elif key_lower in self.REDIS_KEYS['password']:
                config['password'] = self._clean_env_var(value_str)
            elif key_lower in self.REDIS_KEYS['database']:
                config['database'] = str(value)
        
        return config
    
    def _parse_elasticsearch_config(self, data: Dict) -> Dict[str, Any]:
        """解析 Elasticsearch 配置"""
        config = {
            'db_type': 'elasticsearch',
            'host': 'localhost',
            'port': 9200,
            'username': '',
            'password': '',
            'database': '',
            'auth_source': '',
            'oracle_mode': '',
            'oracle_value': ''
        }
        
        # 解析 URIs
        for key, value in data.items():
            key_lower = key.lower()
            
            if key_lower in ['uris', 'nodes', 'hosts'] and isinstance(value, (str, list)):
                # 取第一个 URI
                uri = value[0] if isinstance(value, list) else value
                parsed = self._parse_es_uri(uri)
                if parsed:
                    config['host'] = parsed.get('host', 'localhost')
                    config['port'] = parsed.get('port', 9200)
                    if parsed.get('username'):
                        config['username'] = parsed['username']
                    if parsed.get('password'):
                        config['password'] = parsed['password']
            
            elif key_lower in ['username', 'user']:
                config['username'] = self._clean_env_var(str(value))
            elif key_lower in ['password', 'pass']:
                config['password'] = self._clean_env_var(str(value))
        
        return config
    
    def _parse_es_uri(self, uri: str) -> Optional[Dict[str, Any]]:
        """解析 Elasticsearch URI"""
        try:
            # http://user:pass@host:port 格式
            if uri.startswith('http'):
                parsed = urlparse(uri)
                return {
                    'host': parsed.hostname or 'localhost',
                    'port': parsed.port or 9200,
                    'username': parsed.username or '',
                    'password': parsed.password or ''
                }
            # host:port 格式
            elif ':' in uri:
                parts = uri.split(':')
                return {
                    'host': parts[0],
                    'port': int(parts[1]) if len(parts) > 1 else 9200,
                    'username': '',
                    'password': ''
                }
        except:
            pass
        return None
    
    def _parse_mongodb_config(self, data: Dict) -> Dict[str, Any]:
        """解析 MongoDB 配置"""
        config = {
            'db_type': 'mongodb',
            'host': 'localhost',
            'port': 27017,
            'username': '',
            'password': '',
            'database': '',
            'auth_source': 'admin',
            'oracle_mode': '',
            'oracle_value': ''
        }
        
        # 查找 MongoDB URI
        for key, value in data.items():
            if isinstance(value, str) and value.startswith('mongodb://'):
                match = self.MONGODB_PATTERN.match(value)
                if match:
                    result = match.groupdict()
                    config['host'] = result.get('host', 'localhost')
                    config['port'] = int(result['port']) if result.get('port') else 27017
                    config['username'] = result.get('username', '')
                    config['password'] = result.get('password', '')
                    config['database'] = result.get('database', '')
                break
            
            # 也支持独立字段
            key_lower = key.lower()
            if key_lower in ['host', 'hostname']:
                config['host'] = self._clean_env_var(str(value))
            elif key_lower == 'port':
                try:
                    config['port'] = int(value)
                except:
                    pass
            elif key_lower == 'database':
                config['database'] = str(value)
            elif key_lower == 'authsource':
                config['auth_source'] = str(value)
        
        return config
    
    def _generate_name(self, path: List[str]) -> str:
        """
        基于路径生成配置名称
        
        命名策略: 使用路径中的关键节点
        例如: ['spring', 'redis'] -> 'spring.redis'
              ['datasource', 'master'] -> 'datasource.master'
        """
        if not path:
            return f"imported_{uuid.uuid4().hex[:8]}"
        
        # 过滤掉常见的前缀（如 spring）
        skip_prefixes = ['spring']
        filtered = [p for p in path if p.lower() not in skip_prefixes]
        
        if filtered:
            return '.'.join(filtered)
        return '.'.join(path)
    
    def _clean_env_var(self, value: str) -> str:
        """清理环境变量引用"""
        if not value:
            return ''
        
        match = re.match(r'^\$\{([^}:]+)(?::([^}]+))?\}$', str(value).strip())
        if match:
            default_value = match.group(2)
            return default_value if default_value else ''
        
        return str(value)
    
    def get_summary(self) -> str:
        """获取导入摘要"""
        type_counts = {}
        for config in self.discovered_configs:
            db_type = config.get('db_type', 'unknown')
            type_counts[db_type] = type_counts.get(db_type, 0) + 1
        
        summary = []
        for db_type, count in sorted(type_counts.items()):
            summary.append(f"{db_type}: {count}")
        
        return ", ".join(summary)


# 便捷函数
def import_yaml_configs(file_path: str) -> List[Dict[str, Any]]:
    """便捷函数：批量导入 YAML 配置"""
    importer = YamlConfigImporter()
    return importer.parse_all(file_path)
