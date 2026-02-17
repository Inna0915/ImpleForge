"""
数据库运维工作线程 - 执行 SQL 并返回标准化结果

依赖:
    pip install sqlalchemy pymysql oracledb pymssql
"""

from typing import Any, Dict, List, Optional, Tuple, Union
from PySide6.QtCore import QThread, Signal


class DBOpsWorker(QThread):
    """
    数据库运维操作工作线程
    
    特性:
    - 支持多种数据库类型 (MySQL, Oracle, SQL Server, MongoDB)
    - 智能结果处理（表格/文本/文档）
    - 异常处理和错误信息标准化
    
    信号:
        result_signal: 执行结果 (status, data_type, content, metadata)
        error_signal: 错误信息 (error_msg, sql_text)
        finished_signal: 执行完成
    """
    
    # 信号定义
    # status: 'success' | 'error'
    # data_type: 'table' | 'text' | 'document'
    # content: 实际数据
    # metadata: 额外信息（如执行时间、行数等）
    result_signal = Signal(str, str, object, dict)
    error_signal = Signal(str, str)
    finished_signal = Signal()
    
    def __init__(
        self,
        db_profile: Dict[str, Any],
        operation: str,
        sql_text: str,
        result_type: str = "table",
        timeout: int = 10,
        parent=None
    ):
        """
        初始化运维操作线程
        
        Args:
            db_profile: 数据库连接配置
            operation: 操作名称（用于日志和错误信息）
            sql_text: SQL 语句或 MongoDB 命令
            result_type: 预期结果类型 ('table' | 'text' | 'document')
            timeout: 执行超时（秒）
            parent: 父对象
        """
        super().__init__(parent)
        
        self.db_profile = db_profile
        self.operation = operation
        self.sql_text = sql_text
        self.result_type = result_type
        self.timeout = timeout
        self._is_running = False
    
    def run(self) -> None:
        """执行运维操作"""
        self._is_running = True
        start_time = __import__('time').time()
        
        try:
            db_type = self.db_profile.get("db_type", "").lower()
            
            # 根据数据库类型选择执行方式
            if db_type == "mongodb":
                result, metadata = self._execute_mongodb()
            else:
                result, metadata = self._execute_sql()
            
            # 计算执行时间
            elapsed_ms = int((__import__('time').time() - start_time) * 1000)
            metadata['elapsed_ms'] = elapsed_ms
            
            if self._is_running:
                self.result_signal.emit("success", self.result_type, result, metadata)
                
        except Exception as e:
            elapsed_ms = int((__import__('time').time() - start_time) * 1000)
            if self._is_running:
                error_msg = self._parse_error(e, self.db_profile.get("db_type", ""))
                self.error_signal.emit(error_msg, self.sql_text)
        
        finally:
            self._is_running = False
            self.finished_signal.emit()
    
    def _execute_sql(self) -> Tuple[Any, Dict]:
        """
        执行 SQL 语句（关系型数据库）
        
        Returns:
            (result, metadata)
            - result: 根据 result_type 可能是 list(list) 或 str
            - metadata: 包含行数、列信息等
        """
        from sqlalchemy import create_engine, text
        from sqlalchemy.exc import SQLAlchemyError
        
        # 构建连接字符串
        connection_string = self._build_connection_string()
        if not connection_string:
            raise ValueError(f"不支持的数据库类型: {self.db_profile.get('db_type')}")
        
        # 创建引擎
        engine = create_engine(
            connection_string,
            connect_args={"connect_timeout": self.timeout},
            pool_pre_ping=True,
            echo=False
        )
        
        metadata = {
            "row_count": 0,
            "column_count": 0,
            "columns": []
        }
        
        with engine.connect() as connection:
            # 执行 SQL
            result = connection.execute(text(self.sql_text))
            
            # 检查是否有结果集
            if result.cursor is None:
                # 无结果集的命令（如 SET, CREATE 等）
                rowcount = result.rowcount if hasattr(result, 'rowcount') else 0
                metadata["row_count"] = rowcount
                return f"执行成功，影响 {rowcount} 行", metadata
            
            # 获取列信息
            if result.cursor.description:
                # Oracle 返回大写列名，统一转换为小写
                columns = []
                for col in result.cursor.description:
                    col_name = col[0]
                    if isinstance(col_name, str):
                        # Oracle 返回大写，转换为小写以便显示
                        columns.append(col_name.lower())
                    else:
                        columns.append(str(col_name))
                metadata["columns"] = columns
                metadata["column_count"] = len(columns)
            
            # 根据结果类型处理数据
            if self.result_type == "text":
                # 文本结果（如 SHOW ENGINE INNODB STATUS）
                # 通常这类查询返回多行，每行是一个字段
                rows = result.fetchall()
                
                if not rows:
                    return "(无数据)", metadata
                
                # 将结果拼接为文本
                text_lines = []
                for row in rows:
                    # 每行可能是一个字段或多个字段
                    if len(row) == 1:
                        text_lines.append(str(row[0]) if row[0] is not None else "")
                    else:
                        text_lines.append(" | ".join(str(c) if c is not None else "NULL" for c in row))
                
                full_text = "\n".join(text_lines)
                metadata["row_count"] = len(rows)
                
                return full_text, metadata
            
            else:  # table
                # 表格结果
                rows = result.fetchall()
                data = []
                
                for row in rows:
                    row_data = []
                    for value in row:
                        if value is None:
                            row_data.append("")
                        else:
                            # 截断过长字符串
                            str_val = str(value)
                            if len(str_val) > 1000:
                                str_val = str_val[:997] + "..."
                            row_data.append(str_val)
                    data.append(row_data)
                
                metadata["row_count"] = len(data)
                return data, metadata
    
    def _execute_mongodb(self) -> Tuple[Any, Dict]:
        """
        执行 MongoDB 命令
        
        Returns:
            (result, metadata)
        """
        from pymongo import MongoClient
        from pymongo.errors import PyMongoError
        
        host = self.db_profile.get("host", "localhost")
        port = self.db_profile.get("port", 27017)
        username = self.db_profile.get("username", "")
        password = self.db_profile.get("password", "")
        database = self.db_profile.get("database", "admin")
        
        # 构建连接字符串
        if username and password:
            uri = f"mongodb://{username}:{password}@{host}:{port}/{database}"
        else:
            uri = f"mongodb://{host}:{port}/{database}"
        
        client = MongoClient(uri, serverSelectionTimeoutMS=self.timeout * 1000)
        
        try:
            db = client[database]
            
            # 解析命令
            if isinstance(self.sql_text, dict):
                cmd = self.sql_text
            else:
                # 尝试解析 JSON 字符串
                import json
                cmd = json.loads(self.sql_text)
            
            # 执行命令
            result = db.command(cmd)
            
            # 转换为可显示的格式
            import json
            formatted = json.dumps(result, indent=2, default=str)
            
            return formatted, {"document_count": len(result) if isinstance(result, dict) else 0}
            
        finally:
            client.close()
    
    def _build_connection_string(self) -> Optional[str]:
        """构建 SQLAlchemy 连接字符串"""
        from urllib.parse import quote_plus
        
        db_type = self.db_profile.get("db_type", "").lower()
        host = self.db_profile.get("host", "localhost")
        port = self.db_profile.get("port", 3306)
        username = self.db_profile.get("username", "")
        password = self.db_profile.get("password", "")
        database = self.db_profile.get("database", "")
        
        safe_username = quote_plus(username)
        safe_password = quote_plus(password)
        
        if db_type == "mysql" or db_type == "mariadb":
            driver = "mysql+pymysql"
            if database:
                return f"{driver}://{safe_username}:{safe_password}@{host}:{port}/{database}"
            else:
                return f"{driver}://{safe_username}:{safe_password}@{host}:{port}"
        
        elif db_type == "oracle":
            # Oracle 12c+ 使用 oracledb
            driver = "oracle+oracledb"
            service_name = database or "ORCL"
            return f"{driver}://{safe_username}:{safe_password}@{host}:{port}/?service_name={service_name}"
        
        elif db_type == "sqlserver":
            # SQL Server 使用 pymssql
            driver = "mssql+pymssql"
            if database:
                return f"{driver}://{safe_username}:{safe_password}@{host}:{port}/{database}"
            else:
                return f"{driver}://{safe_username}:{safe_password}@{host}:{port}"
        
        return None
    
    def _parse_error(self, error, db_type: str) -> str:
        """
        解析错误信息为友好提示
        
        Args:
            error: 异常对象
            db_type: 数据库类型
            
        Returns:
            友好的错误信息
        """
        error_str = str(error).lower()
        original = str(error)
        
        # 通用错误
        if "timeout" in error_str:
            return f"连接超时（{self.timeout}秒），请检查网络或增加超时时间"
        
        if "access denied" in error_str or "login failed" in error_str:
            return "认证失败：用户名或密码错误"
        
        if "unknown host" in error_str or "could not connect" in error_str:
            return "无法连接到数据库服务器，请检查主机地址和端口"
        
        if "unknown database" in error_str or "database does not exist" in error_str:
            return "数据库不存在，请检查数据库名称"
        
        # Oracle 特有错误
        if db_type == "oracle":
            if "ora-00942" in error_str:
                return "表或视图不存在，或当前用户无权限访问"
            if "ora-01031" in error_str:
                return "权限不足，需要 DBA 权限才能执行此操作"
            if "ora-01555" in error_str:
                return "快照过旧，查询时间太长或 UNDO 表空间不足"
            if "ora-00054" in error_str:
                return "资源正忙，对象被其他会话锁定"
        
        # MySQL 特有错误
        if db_type in ["mysql", "mariadb"]:
            if "syntax" in error_str:
                return f"SQL 语法错误：\n{original[:200]}"
            if "performance_schema" in error_str:
                return "Performance Schema 未启用，某些查询无法执行"
        
        # SQL Server 特有错误
        if db_type == "sqlserver":
            if "invalid object name" in error_str:
                return "对象不存在，请检查表名或视图名"
        
        # 默认返回原始错误（截断）
        if len(original) > 500:
            original = original[:497] + "..."
        return f"执行错误：\n{original}"
    
    def stop(self) -> None:
        """停止执行"""
        self._is_running = False
        self.wait(1000)
    
    def is_running(self) -> bool:
        """检查是否正在运行"""
        return self._is_running
