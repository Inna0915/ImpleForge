"""
数据库连接测试工具 - 使用 SQLAlchemy 测试数据库连接

依赖安装:
    pip install sqlalchemy pymysql

支持的数据库:
    - MySQL (mysql+pymysql://)
    - PostgreSQL (postgresql+psycopg2://) - 需要安装 psycopg2
    - SQLite (sqlite:///)
    - SQL Server (mssql+pyodbc://) - 需要安装 pyodbc
    - Oracle (oracle+cx_oracle://) - 需要安装 cx_oracle
"""

from typing import Dict, Any, Tuple, Optional
import traceback


def test_db_connection(profile: Dict[str, Any]) -> Tuple[bool, str]:
    """
    测试数据库连接
    
    使用 SQLAlchemy 创建引擎并尝试连接，执行简单查询验证。
    
    Args:
        profile: 连接配置字典，包含:
            - host: 主机地址
            - port: 端口号
            - username: 用户名
            - password: 密码
            - db_type: 数据库类型 (mysql, postgresql, sqlite, mssql, oracle)
            - database: 数据库名称 (可选)
    
    Returns:
        (success: bool, message: str)
        - success: True 表示连接成功，False 表示失败
        - message: 成功时返回 "连接成功"，失败时返回错误信息
    
    Example:
        >>> profile = {
        ...     "host": "localhost",
        ...     "port": 3306,
        ...     "username": "root",
        ...     "password": "password",
        ...     "db_type": "mysql",
        ...     "database": "test"
        ... }
        >>> success, msg = test_db_connection(profile)
        >>> print(success, msg)
        True 连接成功
    
    注意:
        连接超时设置为 3 秒，防止界面长时间无响应
    """
    try:
        # 延迟导入，避免模块加载时就要求安装依赖
        from sqlalchemy import create_engine, text
        from sqlalchemy.exc import SQLAlchemyError
        
        # 提取连接参数
        host = profile.get("host", "localhost")
        port = profile.get("port", 3306)
        username = profile.get("username", "")
        password = profile.get("password", "")
        db_type = profile.get("db_type", "mysql").lower()
        database = profile.get("database", "")
        
        # 构建连接字符串
        connection_string = _build_connection_string(
            db_type, host, port, username, password, database
        )
        
        if not connection_string:
            return False, f"不支持的数据库类型: {db_type}"
        
        # 创建引擎，设置短超时
        # connect_timeout 防止网络不可达时长时间等待
        # pool_pre_ping 确保连接有效
        engine = create_engine(
            connection_string,
            connect_args={"connect_timeout": 3},
            pool_pre_ping=True,
            echo=False,
            # 连接池配置：临时测试不需要连接池
            poolclass=None,
            pool_recycle=3600
        )
        
        # 尝试连接并执行简单查询
        with engine.connect() as connection:
            # 执行 SELECT 1 测试
            result = connection.execute(text("SELECT 1"))
            row = result.fetchone()
            
            if row and row[0] == 1:
                # 获取数据库版本信息
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


def _build_connection_string(
    db_type: str,
    host: str,
    port: int,
    username: str,
    password: str,
    database: str
) -> Optional[str]:
    """
    构建 SQLAlchemy 连接字符串
    
    Args:
        db_type: 数据库类型
        host: 主机地址
        port: 端口号
        username: 用户名
        password: 密码
        database: 数据库名
        
    Returns:
        SQLAlchemy 连接字符串，不支持的数据库返回 None
    """
    # URL 编码特殊字符
    from urllib.parse import quote_plus
    
    safe_username = quote_plus(username) if username else ""
    safe_password = quote_plus(password) if password else ""
    
    if db_type == "mysql":
        # mysql+pymysql://username:password@host:port/database
        if database:
            return f"mysql+pymysql://{safe_username}:{safe_password}@{host}:{port}/{database}"
        else:
            return f"mysql+pymysql://{safe_username}:{safe_password}@{host}:{port}"
    
    elif db_type == "postgresql":
        # postgresql+psycopg2://username:password@host:port/database
        if database:
            return f"postgresql+psycopg2://{safe_username}:{safe_password}@{host}:{port}/{database}"
        else:
            return f"postgresql+psycopg2://{safe_username}:{safe_password}@{host}:{port}"
    
    elif db_type == "sqlite":
        # sqlite:///path/to/database.db 或 sqlite:///:memory:
        if database:
            return f"sqlite:///{database}"
        else:
            return "sqlite:///:memory:"
    
    elif db_type == "mssql":
        # mssql+pyodbc://username:password@dsnname
        # 简化版，实际使用可能需要 ODBC 驱动配置
        if database:
            return f"mssql+pyodbc://{safe_username}:{safe_password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
        else:
            return f"mssql+pyodbc://{safe_username}:{safe_password}@{host}:{port}?driver=ODBC+Driver+17+for+SQL+Server"
    
    elif db_type == "oracle":
        # oracle+cx_oracle://username:password@host:port/?service_name=service
        return f"oracle+cx_oracle://{safe_username}:{safe_password}@{host}:{port}/?service_name={database or 'ORCL'}"
    
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
        if db_type == "mysql":
            result = connection.execute(text("SELECT VERSION()"))
            version = result.fetchone()[0]
            return f"MySQL 版本: {version}"
        
        elif db_type == "postgresql":
            result = connection.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            return f"PostgreSQL: {version.split()[0]}"
        
        elif db_type == "sqlite":
            result = connection.execute(text("SELECT sqlite_version()"))
            version = result.fetchone()[0]
            return f"SQLite 版本: {version}"
        
        elif db_type == "mssql":
            result = connection.execute(text("SELECT @@VERSION"))
            version = result.fetchone()[0]
            return f"SQL Server: {version[:50]}..."
        
        elif db_type == "oracle":
            result = connection.execute(text("SELECT * FROM v$version"))
            version = result.fetchone()[0]
            return f"Oracle: {version}"
        
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
        "postgresql": "psycopg2-binary",
        "sqlite": "",  # SQLite 内置支持
        "mssql": "pyodbc",
        "oracle": "cx_oracle"
    }
    return drivers.get(db_type, f"{db_type} 驱动")


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
