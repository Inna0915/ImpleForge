"""
Oracle 数据泵工作线程 - 执行 expdp/impdp 命令

功能：
- 构建并执行 expdp/impdp 命令
- 实时捕获输出日志
- 密码脱敏显示
- 客户端环境检查

依赖:
    - Oracle Client (instant client 或完整客户端)
    - expdp/impdp 命令在 PATH 中
"""

import subprocess
import sys
import shutil
from typing import Any, Dict, List, Optional
from pathlib import Path

from PySide6.QtCore import QThread, Signal


class DataPumpWorker(QThread):
    """
    Oracle 数据泵执行工作线程
    
    特性：
    - 支持 expdp (导出) 和 impdp (导入)
    - Easy Connect String 连接
    - 实时输出捕获
    - 密码脱敏日志
    
    信号:
        output_signal: 实时输出日志 (line_text)
        finished_signal: 执行完成 (exit_code, success)
        error_signal: 错误信息 (error_msg)
    """
    
    # 信号定义
    output_signal = Signal(str)      # 实时输出行
    finished_signal = Signal(int, bool)  # (退出码, 是否成功)
    error_signal = Signal(str)       # 错误信息
    
    def __init__(
        self,
        db_config: Dict[str, Any],
        operation: str,  # 'expdp' 或 'impdp'
        dmp_filename: str,
        directory_name: str = "DATA_PUMP_DIR",
        additional_params: Optional[List[str]] = None,
        parent=None
    ):
        """
        初始化数据泵工作线程
        
        Args:
            db_config: 数据库配置，包含:
                - username: 用户名
                - password: 密码
                - host: 主机地址
                - port: 端口号
                - service_name: 服务名 (或 SID)
            operation: 操作类型 'expdp' 或 'impdp'
            dmp_filename: DMP 文件名 (不含路径)
            directory_name: Oracle 目录对象名，默认 DATA_PUMP_DIR
            additional_params: 额外参数列表
            parent: 父对象
        """
        super().__init__(parent)
        
        self.db_config = db_config
        self.operation = operation.lower()
        self.dmp_filename = dmp_filename
        self.directory_name = directory_name
        self.additional_params = additional_params or []
        
        self._is_running = False
        self._process: Optional[subprocess.Popen] = None
        
        # 验证操作类型
        if self.operation not in ["expdp", "impdp"]:
            raise ValueError(f"不支持的操作: {operation}，必须是 'expdp' 或 'impdp'")
    
    def run(self) -> None:
        """执行数据泵命令"""
        self._is_running = True
        
        try:
            # 构建命令
            cmd_parts = self._build_command()
            
            # 发送脱敏后的命令信息
            masked_cmd = self._mask_password_in_cmd(cmd_parts)
            self.output_signal.emit(f"执行命令: {masked_cmd}")
            self.output_signal.emit("-" * 60)
            
            # Windows 下隐藏控制台窗口
            startupinfo = None
            if sys.platform == 'win32':
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                startupinfo.wShowWindow = subprocess.SW_HIDE
            
            # 执行命令
            self._process = subprocess.Popen(
                cmd_parts,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.PIPE,
                startupinfo=startupinfo,
                bufsize=1,
                universal_newlines=False
            )
            
            # 实时读取输出
            while self._is_running and self._process.poll() is None:
                line = self._process.stdout.readline()
                if line:
                    # 解码输出
                    try:
                        decoded_line = line.decode('gbk', errors='replace')
                    except:
                        decoded_line = line.decode('utf-8', errors='replace')
                    
                    decoded_line = decoded_line.rstrip('\r\n')
                    if decoded_line:
                        self.output_signal.emit(decoded_line)
                
                self.msleep(50)  # 短暂休眠避免 CPU 过高
            
            # 读取剩余输出
            remaining = self._process.stdout.read()
            if remaining:
                try:
                    decoded = remaining.decode('gbk', errors='replace')
                except:
                    decoded = remaining.decode('utf-8', errors='replace')
                
                for line in decoded.strip().split('\n'):
                    if line.strip():
                        self.output_signal.emit(line.rstrip('\r\n'))
            
            # 获取退出码
            exit_code = self._process.returncode
            success = exit_code == 0
            
            self.output_signal.emit("-" * 60)
            if success:
                self.output_signal.emit(f"✓ 命令执行成功 (退出码: {exit_code})")
            else:
                self.output_signal.emit(f"✗ 命令执行失败 (退出码: {exit_code})")
            
            self.finished_signal.emit(exit_code, success)
            
        except FileNotFoundError as e:
            error_msg = f"未找到 {self.operation} 命令，请检查 Oracle 客户端是否安装并配置 PATH"
            self.output_signal.emit(f"[错误] {error_msg}")
            self.error_signal.emit(error_msg)
            self.finished_signal.emit(-1, False)
            
        except Exception as e:
            error_msg = f"执行异常: {str(e)}"
            self.output_signal.emit(f"[错误] {error_msg}")
            self.error_signal.emit(error_msg)
            self.finished_signal.emit(-1, False)
            
        finally:
            self._is_running = False
            self._process = None
    
    def _build_command(self) -> List[str]:
        """
        构建数据泵命令
        
        Returns:
            命令参数列表
        """
        # 提取配置
        username = self.db_config.get("username", "")
        password = self.db_config.get("password", "")
        host = self.db_config.get("host", "localhost")
        port = self.db_config.get("port", 1521)
        service_name = self.db_config.get("service_name") or self.db_config.get("database", "ORCL")
        
        # 构建 Easy Connect String
        # 格式: user/password@//host:port/service_name
        conn_str = f"{username}/{password}@//{host}:{port}/{service_name}"
        
        # 构建命令
        if self.operation == "expdp":
            # 导出命令
            cmd = [
                "expdp",
                conn_str,
                f"directory={self.directory_name}",
                f"dumpfile={self.dmp_filename}",
                f"logfile={self.dmp_filename}.log",
            ]
            
            # 默认添加 full=y (全库导出)
            if "full" not in " ".join(self.additional_params):
                cmd.append("full=y")
                
        else:  # impdp
            # 导入命令
            cmd = [
                "impdp",
                conn_str,
                f"directory={self.directory_name}",
                f"dumpfile={self.dmp_filename}",
                f"logfile={self.dmp_filename}.imp.log",
            ]
            
            # 默认添加 full=y 和 table_exists_action
            if "full" not in " ".join(self.additional_params):
                cmd.append("full=y")
            if "table_exists_action" not in " ".join(self.additional_params):
                cmd.append("table_exists_action=replace")
        
        # 添加额外参数
        cmd.extend(self.additional_params)
        
        return cmd
    
    def _mask_password_in_cmd(self, cmd_parts: List[str]) -> str:
        """
        将命令中的密码脱敏显示
        
        Args:
            cmd_parts: 命令参数列表
            
        Returns:
            脱敏后的命令字符串
        """
        username = self.db_config.get("username", "")
        password = self.db_config.get("password", "")
        
        # 重建命令字符串并脱敏
        cmd_str = " ".join(cmd_parts)
        
        # 替换密码部分
        # 格式: username/password@//host...
        if password:
            pwd_pattern = f"{username}/{password}@"
            masked_pwd = f"{username}/******@"
            cmd_str = cmd_str.replace(pwd_pattern, masked_pwd)
        
        return cmd_str
    
    def stop(self) -> None:
        """停止执行"""
        self._is_running = False
        
        if self._process and self._process.poll() is None:
            try:
                self._process.terminate()
                self._process.wait(timeout=3)
            except:
                self._process.kill()
    
    def is_running(self) -> bool:
        """检查是否正在运行"""
        return self._is_running
    
    @staticmethod
    def check_oracle_client() -> tuple:
        """
        检查 Oracle 客户端环境
        
        Returns:
            (是否可用, 错误信息)
        """
        # 检查 expdp 和 impdp 是否可用
        expdp_available = shutil.which("expdp") is not None
        impdp_available = shutil.which("impdp") is not None
        
        if not expdp_available and not impdp_available:
            return False, "未检测到 Oracle 客户端 (expdp/impdp 命令未找到)\n请安装 Oracle Instant Client 并配置 PATH 环境变量"
        
        if not expdp_available:
            return False, "未找到 expdp 命令"
        
        if not impdp_available:
            return False, "未找到 impdp 命令"
        
        return True, "Oracle 客户端检测正常"


class DataPumpCommandBuilder:
    """
    数据泵命令构建器
    
    用于生成数据泵命令的辅助类
    """
    
    @staticmethod
    def build_expdp_command(
        username: str,
        password: str,
        host: str,
        port: int,
        service_name: str,
        directory: str = "DATA_PUMP_DIR",
        dumpfile: str = "export.dmp",
        schemas: Optional[List[str]] = None,
        tables: Optional[List[str]] = None,
        full: bool = False,
        parallel: int = 1
    ) -> List[str]:
        """
        构建导出命令
        
        Args:
            username: 用户名
            password: 密码
            host: 主机
            port: 端口
            service_name: 服务名
            directory: 目录对象
            dumpfile: DMP 文件名
            schemas: 指定导出的模式列表
            tables: 指定导出的表列表 (格式: schema.table)
            full: 是否全库导出
            parallel: 并行度
            
        Returns:
            命令参数列表
        """
        conn_str = f"{username}/{password}@//{host}:{port}/{service_name}"
        
        cmd = [
            "expdp",
            conn_str,
            f"directory={directory}",
            f"dumpfile={dumpfile}",
            f"logfile={dumpfile}.log",
        ]
        
        if full:
            cmd.append("full=y")
        elif schemas:
            cmd.append(f"schemas={','.join(schemas)}")
        elif tables:
            cmd.append(f"tables={','.join(tables)}")
        
        if parallel > 1:
            cmd.append(f"parallel={parallel}")
        
        return cmd
    
    @staticmethod
    def build_impdp_command(
        username: str,
        password: str,
        host: str,
        port: int,
        service_name: str,
        directory: str = "DATA_PUMP_DIR",
        dumpfile: str = "export.dmp",
        schemas: Optional[List[str]] = None,
        tables: Optional[List[str]] = None,
        full: bool = False,
        table_exists_action: str = "replace",
        remap_schema: Optional[Dict[str, str]] = None
    ) -> List[str]:
        """
        构建导入命令
        
        Args:
            username: 用户名
            password: 密码
            host: 主机
            port: 端口
            service_name: 服务名
            directory: 目录对象
            dumpfile: DMP 文件名
            schemas: 指定导入的模式列表
            tables: 指定导入的表列表
            full: 是否全库导入
            table_exists_action: 表已存在时的动作 (skip/replace/append/truncate)
            remap_schema: 模式重映射 {source: target}
            
        Returns:
            命令参数列表
        """
        conn_str = f"{username}/{password}@//{host}:{port}/{service_name}"
        
        cmd = [
            "impdp",
            conn_str,
            f"directory={directory}",
            f"dumpfile={dumpfile}",
            f"logfile={dumpfile}.imp.log",
        ]
        
        if full:
            cmd.append("full=y")
        elif schemas:
            cmd.append(f"schemas={','.join(schemas)}")
        elif tables:
            cmd.append(f"tables={','.join(tables)}")
        
        cmd.append(f"table_exists_action={table_exists_action}")
        
        if remap_schema:
            for source, target in remap_schema.items():
                cmd.append(f"remap_schema={source}:{target}")
        
        return cmd
