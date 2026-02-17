"""
SQL 执行工作线程 - 在后台执行 SQL 查询并返回结果

依赖:
    pip install sqlalchemy pymysql
"""

from typing import Any, Dict, List, Optional, Tuple, Union
from PySide6.QtCore import QThread, Signal


class SQLWorker(QThread):
    """
    SQL 执行工作线程
    
    特性:
    - 在独立线程执行 SQL，不阻塞 UI
    - 自动识别 SELECT/INSERT/UPDATE/DELETE 语句类型
    - 返回结构化结果或执行影响行数
    
    信号:
        select_result_signal: SELECT 查询结果 (headers, rows)
        execute_result_signal: 非查询语句结果 (rowcount, message)
        error_signal: 错误信息
        finished_signal: 执行完成
    """
    
    # 信号定义
    # SELECT 查询结果: (表头列表, 数据行列表)
    select_result_signal = Signal(list, list)
    
    # 执行结果: (影响行数, 消息)
    execute_result_signal = Signal(int, str)
    
    # 错误信息
    error_signal = Signal(str)
    
    # 执行完成（无论成功失败）
    finished_signal = Signal()
    
    def __init__(
        self,
        db_profile: Dict[str, Any],
        sql_text: str,
        parent=None
    ):
        """
        初始化 SQL 执行线程
        
        Args:
            db_profile: 数据库连接配置字典，包含:
                - host: 主机地址
                - port: 端口号
                - username: 用户名
                - password: 密码
                - db_type: 数据库类型 (mysql, postgresql, sqlite, ...)
                - database: 数据库名称
            sql_text: 要执行的 SQL 语句
            parent: 父对象
        """
        super().__init__(parent)
        
        self.db_profile = db_profile
        self.sql_text = sql_text.strip()
        self._is_running = False
    
    def run(self) -> None:
        """执行 SQL 查询"""
        self._is_running = True
        
        try:
            # 延迟导入，避免模块加载时就需要依赖
            from sqlalchemy import create_engine, text
            from sqlalchemy.exc import SQLAlchemyError
            
            # 构建连接字符串
            connection_string = self._build_connection_string()
            if not connection_string:
                self.error_signal.emit("不支持的数据库类型")
                return
            
            # 创建引擎
            engine = create_engine(
                connection_string,
                connect_args={"connect_timeout": 5},
                pool_pre_ping=True,
                echo=False
            )
            
            # 执行 SQL
            with engine.connect() as connection:
                # 使用 text() 包装 SQL 语句
                stmt = text(self.sql_text)
                result = connection.execute(stmt)
                
                # 判断 SQL 类型
                sql_upper = self.sql_text.upper()
                is_select = sql_upper.startswith("SELECT") or \
                           sql_upper.startswith("SHOW") or \
                           sql_upper.startswith("DESCRIBE") or \
                           sql_upper.startswith("DESC") or \
                           sql_upper.startswith("EXPLAIN")
                
                if is_select:
                    # SELECT 查询：获取表头和数据
                    self._handle_select_result(result)
                else:
                    # 非查询语句：获取影响行数
                    self._handle_execute_result(result, connection)
                
                # 提交事务（对于需要的事务性操作）
                connection.commit()
                
        except SQLAlchemyError as e:
            error_msg = self._parse_error(e)
            self.error_signal.emit(error_msg)
            
        except Exception as e:
            self.error_signal.emit(f"执行异常: {str(e)}")
            
        finally:
            self._is_running = False
            self.finished_signal.emit()
    
    def _build_connection_string(self) -> Optional[str]:
        """构建 SQLAlchemy 连接字符串"""
        from urllib.parse import quote_plus
        
        db_type = self.db_profile.get("db_type", "mysql").lower()
        host = self.db_profile.get("host", "localhost")
        port = self.db_profile.get("port", 3306)
        username = self.db_profile.get("username", "")
        password = self.db_profile.get("password", "")
        database = self.db_profile.get("database", "")
        
        safe_username = quote_plus(username)
        safe_password = quote_plus(password)
        
        if db_type == "mysql":
            if database:
                return f"mysql+pymysql://{safe_username}:{safe_password}@{host}:{port}/{database}"
            else:
                return f"mysql+pymysql://{safe_username}:{safe_password}@{host}:{port}"
        
        elif db_type == "postgresql":
            if database:
                return f"postgresql+psycopg2://{safe_username}:{safe_password}@{host}:{port}/{database}"
            else:
                return f"postgresql+psycopg2://{safe_username}:{safe_password}@{host}:{port}"
        
        elif db_type == "sqlite":
            if database:
                return f"sqlite:///{database}"
            else:
                return "sqlite:///:memory:"
        
        elif db_type == "mssql":
            if database:
                return f"mssql+pyodbc://{safe_username}:{safe_password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
            else:
                return f"mssql+pyodbc://{safe_username}:{safe_password}@{host}:{port}?driver=ODBC+Driver+17+for+SQL+Server"
        
        elif db_type == "oracle":
            return f"oracle+cx_oracle://{safe_username}:{safe_password}@{host}:{port}/?service_name={database or 'ORCL'}"
        
        return None
    
    def _handle_select_result(self, result) -> None:
        """
        处理 SELECT 查询结果
        
        Args:
            result: SQLAlchemy Result 对象
        """
        try:
            # 获取表头
            if hasattr(result, 'keys'):
                headers = list(result.keys())
            else:
                headers = []
            
            # 获取所有数据行
            rows = result.fetchall()
            
            # 转换为列表格式（便于信号传递）
            # rows 是 Row 对象列表，需要转换为普通列表
            data = []
            for row in rows:
                # 处理每行数据
                row_data = []
                for value in row:
                    # 将 None 转换为空字符串，其他转为字符串
                    if value is None:
                        row_data.append("")
                    else:
                        # 截断过长的字符串
                        str_val = str(value)
                        if len(str_val) > 1000:
                            str_val = str_val[:997] + "..."
                        row_data.append(str_val)
                data.append(row_data)
            
            self.select_result_signal.emit(headers, data)
            
        except Exception as e:
            self.error_signal.emit(f"处理查询结果失败: {str(e)}")
    
    def _handle_execute_result(self, result, connection) -> None:
        """
        处理非查询语句结果
        
        Args:
            result: SQLAlchemy Result 对象
            connection: 数据库连接
        """
        try:
            # 获取影响行数
            rowcount = result.rowcount if hasattr(result, 'rowcount') else -1
            
            # 判断语句类型
            sql_upper = self.sql_text.upper()
            if sql_upper.startswith("INSERT"):
                action = "插入"
            elif sql_upper.startswith("UPDATE"):
                action = "更新"
            elif sql_upper.startswith("DELETE"):
                action = "删除"
            elif sql_upper.startswith("CREATE"):
                action = "创建"
            elif sql_upper.startswith("DROP"):
                action = "删除"
            elif sql_upper.startswith("ALTER"):
                action = "修改"
            else:
                action = "执行"
            
            if rowcount >= 0:
                message = f"{action}成功，影响 {rowcount} 行"
            else:
                message = f"{action}成功"
            
            self.execute_result_signal.emit(rowcount, message)
            
        except Exception as e:
            self.error_signal.emit(f"处理执行结果失败: {str(e)}")
    
    def _parse_error(self, error) -> str:
        """解析 SQLAlchemy 错误为友好信息"""
        error_str = str(error).lower()
        original = str(error)
        
        # 常见错误分类
        if "access denied" in error_str or "1045" in error_str:
            return "数据库认证失败：用户名或密码错误"
        
        elif "unknown database" in error_str or "1049" in error_str:
            return "数据库不存在：请检查数据库名称"
        
        elif "can't connect" in error_str or "2003" in error_str:
            return "无法连接到数据库服务器：请检查网络和端口"
        
        elif "table" in error_str and ("doesn't exist" in error_str or "does not exist" in error_str):
            return "表不存在：请检查表名是否正确"
        
        elif "column" in error_str and "unknown" in error_str:
            return "列不存在：请检查列名是否正确"
        
        elif "syntax" in error_str:
            return f"SQL 语法错误：\n{original[:200]}"
        
        elif "timeout" in error_str:
            return "连接超时：请检查网络状况"
        
        elif "permission" in error_str or "denied" in error_str:
            return "权限不足：当前用户无法执行此操作"
        
        # 默认返回原始错误（截断）
        return f"SQL 执行错误：\n{original[:300]}"
    
    def stop(self) -> None:
        """停止执行（设置标志位）"""
        self._is_running = False
        # 注意：SQLAlchemy 的数据库操作通常无法强制中断
        # 这里主要是设置标志，实际执行仍会继续
    
    def is_running(self) -> bool:
        """检查是否正在执行"""
        return self._is_running
