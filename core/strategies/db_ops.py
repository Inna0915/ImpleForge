"""
数据库运维策略定义

定义不同数据库类型支持的功能和操作。
用于动态渲染运维仪表盘的操作面板。

依赖安装:
    pip install oracledb pymssql pymongo
"""

from typing import Dict, List, Any
from enum import Enum


class DBType(Enum):
    """支持的数据库类型"""
    MYSQL = "mysql"
    MARIADB = "mariadb"
    SQLSERVER = "sqlserver"
    ORACLE = "oracle"
    MONGODB = "mongodb"


# 数据库能力定义
# 每个数据库类型对应支持的功能标志
db_capabilities: Dict[str, Dict[str, bool]] = {
    "mysql": {
        "deadlock": True,           # 支持查看死锁
        "binlog": True,             # 支持查看 Binlog 状态
        "processlist": True,        # 支持查看进程列表
        "performance_schema": True, # 支持性能模式查询
        "replication": True,        # 支持主从复制状态
        "slow_query": True,         # 支持慢查询分析
        "table_stats": True,        # 支持表统计信息
        "kill_session": True,       # 支持终止会话
        "oracle_pump": False,       # Oracle 特有功能
        "backup_logical": True,     # 支持逻辑备份 (mysqldump)
    },
    "mariadb": {
        "deadlock": True,
        "binlog": True,
        "processlist": True,
        "performance_schema": True,
        "replication": True,
        "slow_query": True,
        "table_stats": True,
        "kill_session": True,
        "oracle_pump": False,
        "backup_logical": True,
    },
    "sqlserver": {
        "deadlock": True,           # 支持查看死锁 (系统视图)
        "binlog": False,            # SQL Server 使用事务日志，非 Binlog
        "processlist": True,        # 支持查看活动进程 (sp_who/sp_who2)
        "performance_schema": False, # SQL Server 使用 DMV
        "replication": True,        # 支持复制状态
        "slow_query": True,         # 支持慢查询 (扩展事件)
        "table_stats": True,        # 支持表统计信息
        "kill_session": True,       # 支持终止会话 (KILL)
        "oracle_pump": False,
        "backup_logical": True,     # 支持备份 (BACKUP DATABASE)
        "dmv": True,                # SQL Server 特有：动态管理视图
    },
    "oracle": {
        "deadlock": True,           # 支持查看死锁 (v$lock, dba_blockers)
        "binlog": False,            # Oracle 使用归档日志
        "processlist": True,        # 支持查看会话 (v$session)
        "performance_schema": False, # Oracle 使用 AWR/ASH
        "replication": True,        # 支持 Data Guard 状态
        "slow_query": True,         # 支持 AWR 慢 SQL 分析
        "table_stats": True,        # 支持表统计信息
        "kill_session": True,       # 支持终止会话 (ALTER SYSTEM KILL SESSION)
        "oracle_pump": True,        # Oracle 特有：数据泵 (Data Pump)
        "backup_logical": True,     # 支持逻辑导出 (expdp/impdp)
        "awr": True,                # Oracle 特有：自动工作负载仓库
        "tablespace": True,         # Oracle 特有：表空间管理
    },
    "mongodb": {
        "deadlock": False,          # MongoDB 无传统死锁概念
        "binlog": False,            # MongoDB 使用 Oplog
        "processlist": True,        # 支持查看操作 (currentOp)
        "performance_schema": False,
        "replication": True,        # 支持副本集状态
        "slow_query": True,         # 支持慢查询分析
        "table_stats": True,        # 支持集合统计 (collStats)
        "kill_session": True,       # 支持终止操作 (killOp)
        "oracle_pump": False,
        "backup_logical": True,     # 支持 mongodump/mongorestore
        "oplog": True,              # MongoDB 特有：操作日志
        "replica_status": True,     # MongoDB 特有：副本集状态
    }
}


# 操作按钮定义
# 定义每个功能对应的按钮信息和描述
operation_definitions: Dict[str, Dict[str, Any]] = {
    "deadlock": {
        "id": "deadlock",
        "label": "🔍 查看死锁",
        "tooltip": "查看当前数据库中的死锁信息",
        "icon": "🔍",
        "shortcut": "Ctrl+D",
    },
    "binlog": {
        "id": "binlog",
        "label": "📜 Binlog 状态",
        "tooltip": "查看二进制日志状态和配置",
        "icon": "📜",
        "shortcut": "Ctrl+B",
    },
    "processlist": {
        "id": "processlist",
        "label": "👥 进程列表",
        "tooltip": "查看当前活动连接和进程",
        "icon": "👥",
        "shortcut": "Ctrl+P",
    },
    "replication": {
        "id": "replication",
        "label": "🔄 复制状态",
        "tooltip": "查看主从复制状态",
        "icon": "🔄",
        "shortcut": "Ctrl+R",
    },
    "slow_query": {
        "id": "slow_query",
        "label": "🐌 慢查询",
        "tooltip": "查看慢查询日志和分析",
        "icon": "🐌",
        "shortcut": "Ctrl+S",
    },
    "table_stats": {
        "id": "table_stats",
        "label": "📊 表统计",
        "tooltip": "查看表大小和统计信息",
        "icon": "📊",
        "shortcut": "Ctrl+T",
    },
    "kill_session": {
        "id": "kill_session",
        "label": "⚡ 终止会话",
        "tooltip": "终止指定的数据库会话",
        "icon": "⚡",
        "shortcut": "Ctrl+K",
    },
    "oracle_pump": {
        "id": "oracle_pump",
        "label": "📦 数据泵",
        "tooltip": "Oracle 数据泵导入导出 (expdp/impdp)",
        "icon": "📦",
        "shortcut": "Ctrl+O",
    },
    "awr": {
        "id": "awr",
        "label": "📈 AWR 报告",
        "tooltip": "生成 Oracle AWR 性能报告",
        "icon": "📈",
        "shortcut": "Ctrl+A",
    },
    "tablespace": {
        "id": "tablespace",
        "label": "💾 表空间",
        "tooltip": "查看表空间使用情况",
        "icon": "💾",
        "shortcut": "Ctrl+Space",
    },
    "oplog": {
        "id": "oplog",
        "label": "📋 Oplog 状态",
        "tooltip": "查看 MongoDB Oplog 状态",
        "icon": "📋",
        "shortcut": "Ctrl+L",
    },
    "replica_status": {
        "id": "replica_status",
        "label": "🔰 副本集状态",
        "tooltip": "查看 MongoDB 副本集状态",
        "icon": "🔰",
        "shortcut": "Ctrl+Shift+R",
    },
    "dmv": {
        "id": "dmv",
        "label": "📊 DMV 查询",
        "tooltip": "查询 SQL Server 动态管理视图",
        "icon": "📊",
        "shortcut": "Ctrl+M",
    },
}


def get_db_capabilities(db_type: str) -> Dict[str, bool]:
    """
    获取指定数据库类型的能力列表
    
    Args:
        db_type: 数据库类型，如 'mysql', 'oracle' 等
        
    Returns:
        能力字典，key 为功能名，value 为是否支持
        
    Example:
        >>> caps = get_db_capabilities('mysql')
        >>> caps['deadlock']
        True
        >>> caps['oracle_pump']
        False
    """
    return db_capabilities.get(db_type.lower(), {})


def get_supported_operations(db_type: str) -> List[Dict[str, Any]]:
    """
    获取指定数据库类型支持的所有操作定义
    
    Args:
        db_type: 数据库类型
        
    Returns:
        操作定义列表，每个元素包含按钮所需的信息
        
    Example:
        >>> ops = get_supported_operations('oracle')
        >>> [op['label'] for op in ops]
        ['🔍 查看死锁', '👥 进程列表', ...]
    """
    caps = get_db_capabilities(db_type)
    supported = []
    
    for op_key, supported_flag in caps.items():
        if supported_flag and op_key in operation_definitions:
            supported.append(operation_definitions[op_key])
    
    return supported


def is_capability_supported(db_type: str, capability: str) -> bool:
    """
    检查指定数据库类型是否支持某项功能
    
    Args:
        db_type: 数据库类型
        capability: 功能名称
        
    Returns:
        是否支持
        
    Example:
        >>> is_capability_supported('oracle', 'oracle_pump')
        True
        >>> is_capability_supported('mysql', 'oracle_pump')
        False
    """
    caps = get_db_capabilities(db_type)
    return caps.get(capability, False)


def get_all_db_types() -> List[str]:
    """获取所有支持的数据库类型列表"""
    return list(db_capabilities.keys())


# 预设 SQL/命令 模板（后续 Phase 实现具体执行）
# 这些模板将在后续 Phase 中用于生成实际执行的 SQL
sql_templates: Dict[str, Dict[str, str]] = {
    "mysql": {
        "deadlock": "SHOW ENGINE INNODB STATUS",
        "binlog": "SHOW MASTER STATUS; SHOW BINARY LOGS;",
        "processlist": "SHOW FULL PROCESSLIST",
        "replication": "SHOW SLAVE STATUS\\G",
        "slow_query": "SELECT * FROM mysql.slow_log ORDER BY start_time DESC LIMIT 20",
        "table_stats": """
            SELECT 
                table_name,
                table_rows,
                ROUND(data_length/1024/1024, 2) AS data_size_mb,
                ROUND(index_length/1024/1024, 2) AS index_size_mb
            FROM information_schema.tables 
            WHERE table_schema = DATABASE()
            ORDER BY data_length DESC
        """,
    },
    "mariadb": {
        "deadlock": "SHOW ENGINE INNODB STATUS",
        "binlog": "SHOW BINLOG STATUS",
        "processlist": "SHOW FULL PROCESSLIST",
        "replication": "SHOW SLAVE STATUS\\G",
        "slow_query": "SELECT * FROM mysql.slow_log ORDER BY start_time DESC LIMIT 20",
    },
    "sqlserver": {
        "deadlock": """
            SELECT 
                request_session_id AS spid,
                resource_type,
                resource_description,
                request_mode,
                request_status
            FROM sys.dm_tran_locks
            WHERE request_status = 'WAIT'
        """,
        "processlist": "EXEC sp_who2",
        "dmv": "SELECT * FROM sys.dm_exec_requests WHERE status = 'running'",
    },
    "oracle": {
        "deadlock": """
            SELECT 
                s1.username || '@' || s1.machine AS waiting_user,
                s2.username || '@' || s2.machine AS blocking_user,
                lo.object_id,
                lo.locked_mode
            FROM v$locked_object lo
            JOIN v$session s1 ON lo.session_id = s1.sid
            JOIN v$session s2 ON s1.blocking_session = s2.sid
        """,
        "processlist": "SELECT sid, serial#, username, status, machine FROM v$session WHERE type = 'USER'",
        "tablespace": """
            SELECT 
                tablespace_name,
                ROUND(used_space*8192/1024/1024, 2) AS used_mb,
                ROUND(tablespace_size*8192/1024/1024, 2) AS total_mb,
                ROUND((used_space/tablespace_size)*100, 2) AS used_pct
            FROM dba_tablespace_usage_metrics
        """,
    },
    "mongodb": {
        "processlist": "db.currentOp({})",
        "oplog": "rs.printReplicationInfo()",
        "replica_status": "rs.status()",
        "slow_query": "db.system.profile.find().sort({ts: -1}).limit(20)",
    }
}


def get_sql_template(db_type: str, operation: str) -> str:
    """
    获取指定数据库类型和操作的 SQL 模板
    
    Args:
        db_type: 数据库类型
        operation: 操作名称
        
    Returns:
        SQL 模板字符串，未找到返回空字符串
    """
    db_templates = sql_templates.get(db_type.lower(), {})
    return db_templates.get(operation, "")
