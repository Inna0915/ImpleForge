"""
SQL 注册表 - 存储各数据库的运维 SQL 语句

提供标准化的 SQL 语句模板，用于数据库运维操作。

依赖:
    pip install sqlalchemy pymysql oracledb pymssql
"""

from typing import Dict, List, Any, Optional


class SQLRegistry:
    """
    SQL 语句注册表
    
    为不同数据库类型和运维操作提供标准化的 SQL 语句。
    支持 MySQL、Oracle、SQL Server 等数据库。
    """
    
    # SQL 语句仓库
    # 结构: {db_type: {operation_key: {sql, description, result_type}}}
    _sql_map: Dict[str, Dict[str, Dict[str, Any]]] = {
        # ==================== MySQL / MariaDB ====================
        "mysql": {
            "deadlock": {
                "sql": "SHOW ENGINE INNODB STATUS",
                "description": "查看 InnoDB 死锁信息",
                "result_type": "text",  # 返回大段文本
                "timeout": 5
            },
            "processlist": {
                "sql": "SHOW FULL PROCESSLIST",
                "description": "查看当前活动连接",
                "result_type": "table",
                "timeout": 5
            },
            "binlog": {
                "sql": "SHOW MASTER STATUS",
                "description": "查看主库 Binlog 状态",
                "result_type": "table",
                "timeout": 5
            },
            "variables": {
                "sql": "SHOW GLOBAL VARIABLES",
                "description": "查看全局变量配置",
                "result_type": "table",
                "timeout": 10
            },
            "replication": {
                "sql": "SHOW SLAVE STATUS",
                "description": "查看从库复制状态",
                "result_type": "table",
                "timeout": 5
            },
            "slow_query": {
                "sql": """
                    SELECT 
                        DIGEST_TEXT as sql_text,
                        SCHEMA_NAME as db,
                        COUNT_STAR as exec_count,
                        ROUND(AVG_TIMER_WAIT/1000000000000, 3) as avg_latency_sec,
                        ROUND(MAX_TIMER_WAIT/1000000000000, 3) as max_latency_sec
                    FROM performance_schema.events_statements_summary_by_digest
                    WHERE SCHEMA_NAME IS NOT NULL
                    ORDER BY AVG_TIMER_WAIT DESC
                    LIMIT 20
                """,
                "description": "查看慢查询统计 (Performance Schema)",
                "result_type": "table",
                "timeout": 10
            },
            "table_stats": {
                "sql": """
                    SELECT 
                        TABLE_SCHEMA as db_name,
                        TABLE_NAME as table_name,
                        TABLE_ROWS as row_count,
                        ROUND(DATA_LENGTH/1024/1024, 2) as data_size_mb,
                        ROUND(INDEX_LENGTH/1024/1024, 2) as index_size_mb,
                        ROUND((DATA_LENGTH+INDEX_LENGTH)/1024/1024, 2) as total_size_mb
                    FROM information_schema.tables
                    WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                    ORDER BY (DATA_LENGTH+INDEX_LENGTH) DESC
                    LIMIT 50
                """,
                "description": "查看表统计信息",
                "result_type": "table",
                "timeout": 15
            }
        },
        
        # ==================== MariaDB ====================
        "mariadb": {
            "deadlock": {
                "sql": "SHOW ENGINE INNODB STATUS",
                "description": "查看 InnoDB 死锁信息",
                "result_type": "text",
                "timeout": 5
            },
            "processlist": {
                "sql": "SHOW FULL PROCESSLIST",
                "description": "查看当前活动连接",
                "result_type": "table",
                "timeout": 5
            },
            "binlog": {
                "sql": "SHOW BINLOG STATUS",
                "description": "查看 Binlog 状态",
                "result_type": "table",
                "timeout": 5
            },
            "variables": {
                "sql": "SHOW GLOBAL VARIABLES",
                "description": "查看全局变量配置",
                "result_type": "table",
                "timeout": 10
            },
            "replication": {
                "sql": "SHOW SLAVE STATUS",
                "description": "查看复制状态",
                "result_type": "table",
                "timeout": 5
            },
            "slow_query": {
                "sql": """
                    SELECT 
                        DIGEST_TEXT as sql_text,
                        SCHEMA_NAME as db,
                        COUNT_STAR as exec_count,
                        ROUND(AVG_TIMER_WAIT/1000000000000, 3) as avg_latency_sec
                    FROM performance_schema.events_statements_summary_by_digest
                    WHERE SCHEMA_NAME IS NOT NULL
                    ORDER BY AVG_TIMER_WAIT DESC
                    LIMIT 20
                """,
                "description": "查看慢查询统计",
                "result_type": "table",
                "timeout": 10
            },
            "table_stats": {
                "sql": """
                    SELECT 
                        TABLE_SCHEMA as db_name,
                        TABLE_NAME as table_name,
                        TABLE_ROWS as row_count,
                        ROUND(DATA_LENGTH/1024/1024, 2) as data_size_mb
                    FROM information_schema.tables
                    WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                    ORDER BY (DATA_LENGTH+INDEX_LENGTH) DESC
                    LIMIT 50
                """,
                "description": "查看表统计信息",
                "result_type": "table",
                "timeout": 15
            }
        },
        
        # ==================== Oracle (12c+) ====================
        "oracle": {
            "deadlock": {
                "sql": """
                    SELECT 
                        s1.sid as waiting_session_id,
                        s1.serial# as waiting_serial,
                        s1.username as oracle_user,
                        s1.osuser as os_user,
                        s2.sid as blocking_session_id,
                        s2.serial# as blocking_serial,
                        s2.username as blocking_user,
                        o.object_name as locked_object,
                        l.locked_mode
                    FROM v$locked_object l
                    JOIN dba_objects o ON l.object_id = o.object_id
                    JOIN v$session s1 ON l.session_id = s1.sid
                    LEFT JOIN v$session s2 ON s1.blocking_session = s2.sid
                    WHERE s1.blocking_session IS NOT NULL
                    ORDER BY s1.sid
                """,
                "description": "查看死锁和阻塞会话",
                "result_type": "table",
                "timeout": 10
            },
            "processlist": {
                "sql": """
                    SELECT 
                        sid as session_id,
                        serial# as serial_num,
                        username,
                        status,
                        machine,
                        program,
                        type,
                        logon_time,
                        last_call_et as idle_seconds,
                        sql_id
                    FROM v$session
                    WHERE type = 'USER'
                    ORDER BY logon_time DESC
                """,
                "description": "查看用户会话列表",
                "result_type": "table",
                "timeout": 10
            },
            "tablespaces": {
                "sql": """
                    SELECT 
                        df.tablespace_name,
                        ROUND(df.bytes/1024/1024, 2) as total_mb,
                        ROUND(NVL(fs.bytes, 0)/1024/1024, 2) as free_mb,
                        ROUND((df.bytes-NVL(fs.bytes, 0))/1024/1024, 2) as used_mb,
                        ROUND((NVL(fs.bytes, 0)/df.bytes)*100, 2) as free_pct
                    FROM (
                        SELECT tablespace_name, SUM(bytes) bytes 
                        FROM dba_data_files 
                        GROUP BY tablespace_name
                    ) df
                    LEFT JOIN (
                        SELECT tablespace_name, SUM(bytes) bytes 
                        FROM dba_free_space 
                        GROUP BY tablespace_name
                    ) fs ON df.tablespace_name = fs.tablespace_name
                    ORDER BY (NVL(fs.bytes, 0)/df.bytes) ASC
                """,
                "description": "查看表空间使用情况",
                "result_type": "table",
                "timeout": 15
            },
            "awr": {
                "sql": """
                    SELECT 
                        snap_id,
                        begin_interval_time,
                        end_interval_time,
                        ROUND((end_interval_time - begin_interval_time)*24*60, 2) as duration_min
                    FROM dba_hist_snapshot
                    WHERE begin_interval_time > SYSDATE - 1
                    ORDER BY snap_id DESC
                    FETCH FIRST 20 ROWS ONLY
                """,
                "description": "查看 AWR 快照列表",
                "result_type": "table",
                "timeout": 10
            },
            "slow_query": {
                "sql": """
                    SELECT 
                        sql_id,
                        sql_text,
                        elapsed_time/1000000 as elapsed_sec,
                        executions,
                        ROUND(elapsed_time/1000000/NULLIF(executions, 0), 3) as avg_sec_per_exec
                    FROM v$sql
                    WHERE executions > 0
                    AND elapsed_time > 1000000
                    ORDER BY elapsed_time DESC
                    FETCH FIRST 20 ROWS ONLY
                """,
                "description": "查看高耗时 SQL",
                "result_type": "table",
                "timeout": 15
            },
            "lock_wait": {
                "sql": """
                    SELECT 
                        s.sid,
                        s.serial#,
                        s.username,
                        s.osuser,
                        l.type,
                        l.id1,
                        l.id2,
                        l.request,
                        l.lmode,
                        w.event
                    FROM v$lock l
                    JOIN v$session s ON l.sid = s.sid
                    JOIN v$session_wait w ON s.sid = w.sid
                    WHERE l.request > 0
                    ORDER BY s.sid
                """,
                "description": "查看锁等待事件",
                "result_type": "table",
                "timeout": 10
            }
        },
        
        # ==================== SQL Server ====================
        "sqlserver": {
            "deadlock": {
                "sql": """
                    SELECT 
                        request_session_id as spid,
                        resource_type,
                        resource_database_id,
                        resource_description,
                        request_mode,
                        request_status,
                        request_owner_type
                    FROM sys.dm_tran_locks
                    WHERE request_status = 'WAIT'
                    ORDER BY request_session_id
                """,
                "description": "查看等待中的锁（潜在死锁）",
                "result_type": "table",
                "timeout": 10
            },
            "processlist": {
                "sql": """
                    SELECT 
                        r.session_id,
                        s.login_name,
                        r.status,
                        r.command,
                        r.database_id,
                        r.start_time,
                        r.cpu_time,
                        r.total_elapsed_time,
                        t.text as sql_text
                    FROM sys.dm_exec_requests r
                    JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
                    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
                    WHERE r.session_id > 50
                    ORDER BY r.cpu_time DESC
                """,
                "description": "查看活动请求",
                "result_type": "table",
                "timeout": 10
            },
            "dmv": {
                "sql": """
                    SELECT 
                        session_id,
                        login_name,
                        status,
                        cpu_time,
                        memory_usage,
                        total_scheduled_time,
                        total_elapsed_time,
                        reads,
                        writes,
                        logical_reads
                    FROM sys.dm_exec_sessions
                    WHERE is_user_process = 1
                    ORDER BY cpu_time DESC
                """,
                "description": "查看会话 DMV 信息",
                "result_type": "table",
                "timeout": 10
            },
            "slow_query": {
                "sql": """
                    SELECT TOP 20
                        qs.execution_count,
                        qs.total_elapsed_time/1000000 as total_elapsed_sec,
                        qs.total_elapsed_time/qs.execution_count/1000000 as avg_elapsed_sec,
                        SUBSTRING(st.text, (qs.statement_start_offset/2)+1,
                            ((CASE qs.statement_end_offset
                                WHEN -1 THEN DATALENGTH(st.text)
                                ELSE qs.statement_end_offset
                            END - qs.statement_start_offset)/2) + 1) as sql_text
                    FROM sys.dm_exec_query_stats qs
                    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) st
                    ORDER BY qs.total_elapsed_time DESC
                """,
                "description": "查看慢查询统计",
                "result_type": "table",
                "timeout": 15
            },
            "lock_info": {
                "sql": """
                    SELECT 
                        l.request_session_id,
                        DB_NAME(l.resource_database_id) as database_name,
                        l.resource_type,
                        l.request_mode,
                        l.request_status,
                        s.login_name,
                        s.host_name
                    FROM sys.dm_tran_locks l
                    JOIN sys.dm_exec_sessions s ON l.request_session_id = s.session_id
                    WHERE s.is_user_process = 1
                    ORDER BY l.request_session_id
                """,
                "description": "查看锁信息",
                "result_type": "table",
                "timeout": 10
            }
        },
        
        # ==================== MongoDB ====================
        # MongoDB 使用 JavaScript 而非 SQL，需要特殊处理
        "mongodb": {
            "processlist": {
                "command": {"currentOp": {}, "$all": True},
                "description": "查看当前操作",
                "result_type": "document",
                "timeout": 5
            },
            "oplog": {
                "command": {"collStats": "oplog.rs"},
                "description": "查看 Oplog 状态",
                "result_type": "document",
                "timeout": 5
            },
            "replica_status": {
                "command": {"replSetGetStatus": 1},
                "description": "查看副本集状态",
                "result_type": "document",
                "timeout": 10
            },
            "server_status": {
                "command": {"serverStatus": 1},
                "description": "查看服务器状态",
                "result_type": "document",
                "timeout": 10
            },
            "slow_query": {
                "command": {
                    "find": "system.profile",
                    "sort": {"ts": -1},
                    "limit": 20
                },
                "description": "查看慢查询日志",
                "result_type": "document",
                "timeout": 10
            }
        }
    }
    
    @classmethod
    def get_sql(cls, db_type: str, operation: str) -> Optional[Dict[str, Any]]:
        """
        获取指定数据库类型和操作的 SQL 语句
        
        Args:
            db_type: 数据库类型，如 'mysql', 'oracle', 'sqlserver'
            operation: 操作名称，如 'deadlock', 'processlist'
            
        Returns:
            SQL 定义字典，包含 sql, description, result_type, timeout
            未找到返回 None
            
        Example:
            >>> sql_info = SQLRegistry.get_sql('mysql', 'deadlock')
            >>> print(sql_info['sql'])
            SHOW ENGINE INNODB STATUS
        """
        db_type = db_type.lower()
        operation = operation.lower()
        
        db_ops = cls._sql_map.get(db_type, {})
        sql_def = db_ops.get(operation)
        
        if sql_def:
            return {
                "sql": sql_def.get("sql", ""),
                "command": sql_def.get("command", None),  # For MongoDB
                "description": sql_def.get("description", ""),
                "result_type": sql_def.get("result_type", "table"),
                "timeout": sql_def.get("timeout", 10)
            }
        
        return None
    
    @classmethod
    def list_operations(cls, db_type: str) -> List[str]:
        """
        列出指定数据库类型支持的所有操作
        
        Args:
            db_type: 数据库类型
            
        Returns:
            操作名称列表
        """
        db_type = db_type.lower()
        return list(cls._sql_map.get(db_type, {}).keys())
    
    @classmethod
    def add_sql(cls, db_type: str, operation: str, sql_def: Dict[str, Any]) -> None:
        """
        动态添加 SQL 定义（用于扩展或自定义）
        
        Args:
            db_type: 数据库类型
            operation: 操作名称
            sql_def: SQL 定义字典
        """
        db_type = db_type.lower()
        operation = operation.lower()
        
        if db_type not in cls._sql_map:
            cls._sql_map[db_type] = {}
        
        cls._sql_map[db_type][operation] = sql_def
    
    @classmethod
    def is_nosql(cls, db_type: str) -> bool:
        """
        判断是否为 NoSQL 数据库
        
        Args:
            db_type: 数据库类型
            
        Returns:
            是否为 NoSQL
        """
        return db_type.lower() in ["mongodb"]


# 便捷函数
def get_sql(db_type: str, operation: str) -> Optional[Dict[str, Any]]:
    """便捷函数：获取 SQL 定义"""
    return SQLRegistry.get_sql(db_type, operation)


def list_operations(db_type: str) -> List[str]:
    """便捷函数：列出支持的操作"""
    return SQLRegistry.list_operations(db_type)
