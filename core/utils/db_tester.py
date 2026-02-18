"""
数据库连接测试工具 - Phase 8 更新版

依赖安装:
    pip install sqlalchemy pymysql oracledb pymssql pymongo redis requests

支持的数据库:
    - MySQL (mysql+pymysql://)
    - MariaDB (mysql+pymysql://)
    - SQL Server (mssql+pymssql://)
    - Oracle (oracle+oracledb://) - 支持 Service Name 和 SID
    - MongoDB (pymongo)
    - Redis (redis-py)
    - Elasticsearch (requests)
"""

from typing import Dict, Any, Tuple, Optional
import traceback


def test_db_connection(profile: Dict[str, Any]) -> Tuple[bool, str]:
    """
    测试数据库连接 - Phase 7 更新版
    
    支持新的配置格式：
    - Oracle: oracle_mode ('service_name'|'sid'), oracle_value
    - MongoDB: auth_source
    - SQL Server: database
    
    Args:
        profile: 连接配置字典，包含:
            - host: 主机地址
            - port: 端口号
            - username: 用户名
            - password: 密码
            - db_type: 数据库类型 (mysql, mariadb, sqlserver, oracle, mongodb)
            - database: 数据库名称
            - auth_source: MongoDB 认证源 (默认 'admin')
            - oracle_mode: Oracle 连接模式 ('service_name' 或 'sid')
            - oracle_value: Oracle Service Name 或 SID 值
    
    Returns:
        (success: bool, message: str)
    """
    try:
        db_type = profile.get("db_type", "").lower()
        
        # 特殊类型处理（非 SQLAlchemy）
        if db_type == "mongodb":
            return _test_mongodb_connection(profile)
        elif db_type == "redis":
            return _test_redis_connection(profile)
        elif db_type == "elasticsearch":
            return _test_elasticsearch_connection(profile)
        
        # 延迟导入 SQLAlchemy
        from sqlalchemy import create_engine, text
        from sqlalchemy.exc import SQLAlchemyError
        
        # 构建连接字符串
        connection_string = _build_connection_string(profile)
        
        if not connection_string:
            return False, f"不支持的数据库类型: {db_type}"
        
        # 创建引擎
        engine = create_engine(
            connection_string,
            connect_args={"connect_timeout": 5},
            pool_pre_ping=True,
            echo=False,
            poolclass=None,
            pool_recycle=3600
        )
        
        # 尝试连接并执行简单查询
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            row = result.fetchone()
            
            if row and row[0] == 1:
                version_info = _get_db_version(connection, db_type)
                return True, f"连接成功\n{version_info}"
            else:
                return False, "连接异常：测试查询返回 unexpected result"
                
    except SQLAlchemyError as e:
        # SQLAlchemy 相关的错误
        error_msg = _parse_sqlalchemy_error(e, db_type)
        return False, f"连接失败: {error_msg}"
        
    except ImportError as e:
        # 缺少依赖库
        missing_lib = _get_required_driver(db_type)
        return False, f"缺少依赖库: {missing_lib}\n请执行: pip install {missing_lib}"
        
    except Exception as e:
        # 其他未知错误
        error_detail = traceback.format_exc()
        print(f"[Error] 数据库连接测试异常: {error_detail}")
        return False, f"连接异常: {str(e)}"


def _build_connection_string(profile: Dict[str, Any]) -> Optional[str]:
    """
    构建连接字符串 - Phase 7 更新版
    
    支持新的配置格式：
    - Oracle: oracle_mode, oracle_value
    - MongoDB: auth_source
    
    Args:
        profile: 完整配置字典
        
    Returns:
        连接字符串，不支持返回 None
    """
    from urllib.parse import quote_plus
    
    db_type = profile.get("db_type", "").lower()
    host = profile.get("host", "localhost")
    port = profile.get("port", 3306)
    username = profile.get("username", "")
    password = profile.get("password", "")
    database = profile.get("database", "")
    
    safe_username = quote_plus(username) if username else ""
    safe_password = quote_plus(password) if password else ""
    
    if db_type in ["mysql", "mariadb"]:
        # mysql+pymysql://user:pass@host:port/database
        if database:
            return f"mysql+pymysql://{safe_username}:{safe_password}@{host}:{port}/{database}"
        else:
            return f"mysql+pymysql://{safe_username}:{safe_password}@{host}:{port}"
    
    elif db_type == "sqlserver":
        # mssql+pymssql://user:pass@host:port/dbname
        driver = "mssql+pymssql"
        if database:
            return f"{driver}://{safe_username}:{safe_password}@{host}:{port}/{database}"
        else:
            return f"{driver}://{safe_username}:{safe_password}@{host}:{port}"
    
    elif db_type == "oracle":
        # oracle+oracledb://user:pass@host:port/?service_name=xxx 或 ?sid=xxx
        oracle_mode = profile.get("oracle_mode", "service_name")
        oracle_value = profile.get("oracle_value", database or "ORCL")
        
        if oracle_mode == "sid":
            # SID 模式
            return f"oracle+oracledb://{safe_username}:{safe_password}@{host}:{port}/?sid={oracle_value}"
        else:
            # Service Name 模式（默认）
            return f"oracle+oracledb://{safe_username}:{safe_password}@{host}:{port}/?service_name={oracle_value}"
    
    elif db_type == "mongodb":
        # MongoDB 不使用 SQLAlchemy，返回特殊标记
        return "mongodb"
    
    else:
        return None


def _get_db_version(connection, db_type: str) -> str:
    """
    获取数据库版本信息
    
    Args:
        connection: SQLAlchemy 连接对象
        db_type: 数据库类型
        
    Returns:
        版本信息字符串
    """
    from sqlalchemy import text
    
    try:
        if db_type in ["mysql", "mariadb"]:
            result = connection.execute(text("SELECT VERSION()"))
            version = result.fetchone()[0]
            return f"MySQL/MariaDB 版本: {version}"
        
        elif db_type == "sqlserver":
            result = connection.execute(text("SELECT @@VERSION"))
            version = result.fetchone()[0]
            return f"SQL Server: {version[:50]}..."
        
        elif db_type == "oracle":
            result = connection.execute(text("SELECT banner FROM v$version WHERE ROWNUM = 1"))
            version = result.fetchone()[0]
            return f"Oracle: {version[:50]}..."
        
        else:
            return "数据库版本: 未知"
            
    except Exception:
        return "数据库版本: 获取失败"


def _parse_sqlalchemy_error(error, db_type: str) -> str:
    """
    解析 SQLAlchemy 错误，返回友好的错误信息
    
    Args:
        error: SQLAlchemy 异常对象
        db_type: 数据库类型
        
    Returns:
        友好的错误信息
    """
    error_str = str(error).lower()
    
    # MySQL 错误解析
    if db_type == "mysql":
        if "access denied" in error_str or "1045" in error_str:
            return "认证失败：用户名或密码错误"
        elif "unknown host" in error_str or "2005" in error_str:
            return "主机不存在：请检查主机地址"
        elif "can't connect" in error_str or "2003" in error_str:
            return "连接失败：无法连接到服务器，请检查网络或端口"
        elif "unknown database" in error_str or "1049" in error_str:
            return "数据库不存在：请检查数据库名称"
    
    # PostgreSQL 错误解析
    elif db_type == "postgresql":
        if "authentication failed" in error_str:
            return "认证失败：用户名或密码错误"
        elif "could not connect" in error_str:
            return "连接失败：无法连接到服务器"
        elif "does not exist" in error_str:
            return "数据库不存在：请检查数据库名称"
    
    # 通用错误
    if "timeout" in error_str:
        return "连接超时：服务器无响应"
    elif "refused" in error_str:
        return "连接被拒绝：请检查端口和防火墙设置"
    elif "network" in error_str:
        return "网络错误：请检查网络连接"
    
    # 默认返回原始错误
    return str(error)[:200]


def _get_required_driver(db_type: str) -> str:
    """
    获取数据库所需的驱动包名
    
    Args:
        db_type: 数据库类型
        
    Returns:
        pip 安装包名
    """
    drivers = {
        "mysql": "pymysql",
        "mariadb": "pymysql",
        "sqlserver": "pymssql",
        "oracle": "oracledb",
        "mongodb": "pymongo",
        "redis": "redis",
        "elasticsearch": "requests"
    }
    return drivers.get(db_type, f"{db_type} 驱动")


def _test_mongodb_connection(profile: Dict[str, Any]) -> Tuple[bool, str]:
    """
    测试 MongoDB 连接
    
    Args:
        profile: 配置字典
        
    Returns:
        (success, message)
    """
    try:
        from pymongo import MongoClient
        from pymongo.errors import PyMongoError
        
        host = profile.get("host", "localhost")
        port = profile.get("port", 27017)
        username = profile.get("username", "")
        password = profile.get("password", "")
        auth_source = profile.get("auth_source", "admin")
        
        # 构建连接字符串
        if username and password:
            uri = f"mongodb://{username}:{password}@{host}:{port}/?authSource={auth_source}"
        else:
            uri = f"mongodb://{host}:{port}"
        
        # 连接并测试
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        
        # 执行服务器信息查询
        info = client.server_info()
        version = info.get("version", "unknown")
        
        client.close()
        
        return True, f"连接成功\nMongoDB 版本: {version}"
        
    except PyMongoError as e:
        return False, f"MongoDB 连接失败: {str(e)}"
    except ImportError:
        return False, "缺少 pymongo 库，请执行: pip install pymongo"


def _test_redis_connection(profile: Dict[str, Any]) -> Tuple[bool, str]:
    """
    测试 Redis 连接
    
    Args:
        profile: 配置字典
        
    Returns:
        (success, message)
    """
    try:
        import redis
        from redis.exceptions import RedisError
        
        host = profile.get("host", "localhost")
        port = profile.get("port", 6379)
        password = profile.get("password", "")
        # Redis 数据库索引
        try:
            db = int(profile.get("database", 0))
        except:
            db = 0
        
        # 创建连接
        r = redis.Redis(
            host=host,
            port=port,
            password=password if password else None,
            db=db,
            socket_connect_timeout=5,
            socket_timeout=5,
            decode_responses=True
        )
        
        # 测试连接
        r.ping()
        
        # 获取服务器信息
        info = r.info()
        version = info.get("redis_version", "unknown")
        mode = info.get("redis_mode", "standalone")
        
        r.close()
        
        return True, f"连接成功\nRedis 版本: {version}\n模式: {mode}"
        
    except RedisError as e:
        return False, f"Redis 连接失败: {str(e)}"
    except ImportError:
        return False, "缺少 redis 库，请执行: pip install redis"


def _test_elasticsearch_connection(profile: Dict[str, Any]) -> Tuple[bool, str]:
    """
    测试 Elasticsearch 连接
    
    Args:
        profile: 配置字典
        
    Returns:
        (success, message)
    """
    try:
        import requests
        from requests.auth import HTTPBasicAuth
        
        host = profile.get("host", "localhost")
        port = profile.get("port", 9200)
        username = profile.get("username", "")
        password = profile.get("password", "")
        
        # 构建 URL
        url = f"http://{host}:{port}"
        
        # 准备认证
        auth = None
        if username and password:
            auth = HTTPBasicAuth(username, password)
        
        # 发送请求
        response = requests.get(
            url,
            auth=auth,
            timeout=5,
            verify=False  # 忽略 SSL 验证（内网环境）
        )
        
        if response.status_code == 200:
            data = response.json()
            cluster_name = data.get("cluster_name", "unknown")
            version = data.get("version", {}).get("number", "unknown")
            return True, f"连接成功\n集群: {cluster_name}\n版本: {version}"
        elif response.status_code == 401:
            return False, "Elasticsearch 认证失败：用户名或密码错误"
        else:
            return False, f"Elasticsearch 返回错误: HTTP {response.status_code}"
            
    except requests.exceptions.ConnectionError:
        return False, "无法连接到 Elasticsearch，请检查主机和端口"
    except requests.exceptions.Timeout:
        return False, "连接超时，请检查网络状况"
    except ImportError:
        return False, "缺少 requests 库，请执行: pip install requests"
    except Exception as e:
        return False, f"Elasticsearch 连接异常: {str(e)}"


# 用于异步执行的 Worker 类
from PySide6.QtCore import QThread, Signal


class DBTestWorker(QThread):
    """
    数据库连接测试工作线程
    
    避免在主线程执行数据库连接测试导致 UI 卡顿
    """
    
    # 信号定义
    success_signal = Signal(str)    # 连接成功，附带版本信息
    error_signal = Signal(str)      # 连接失败，附带错误信息
    finished_signal = Signal()      # 测试完成（无论成功失败）
    
    def __init__(self, profile: Dict[str, Any], parent=None):
        """
        初始化测试线程
        
        Args:
            profile: 连接配置字典
            parent: 父对象
        """
        super().__init__(parent)
        self.profile = profile
        self._is_running = False
    
    def run(self) -> None:
        """执行连接测试"""
        self._is_running = True
        
        try:
            success, message = test_db_connection(self.profile)
            
            if self._is_running:
                if success:
                    self.success_signal.emit(message)
                else:
                    self.error_signal.emit(message)
                    
        except Exception as e:
            if self._is_running:
                self.error_signal.emit(f"测试线程异常: {str(e)}")
        
        finally:
            self._is_running = False
            self.finished_signal.emit()
    
    def stop(self) -> None:
        """停止测试（设置标志位，实际无法强制终止数据库连接）"""
        self._is_running = False
        self.wait(1000)  # 等待最多 1 秒
    
    def is_running(self) -> bool:
        """检查是否正在运行"""
        return self._is_running
