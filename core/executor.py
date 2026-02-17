"""
核心执行引擎 - 负责在后台线程执行命令并实时输出结果
"""

import subprocess
import sys
from pathlib import Path
from typing import Optional

from PySide6.QtCore import QThread, Signal


class CommandWorker(QThread):
    """
    命令执行工作线程
    
    特性：
    - 在独立线程中执行命令，不阻塞 UI
    - 实时读取 stdout/stderr 并通过信号传回
    - 支持 Windows GBK 编码处理
    """
    
    # 信号定义
    output_signal = Signal(str)      # 实时输出文本
    finished_signal = Signal(int)    # 进程退出码 (0=成功, 非0=失败)
    error_signal = Signal(str)       # 错误信息
    
    def __init__(
        self, 
        command: str, 
        cwd: Optional[str] = None,
        shell: bool = True,
        parent=None
    ):
        """
        初始化命令执行器
        
        Args:
            command: 要执行的命令字符串
            cwd: 工作目录，默认为当前目录
            shell: 是否使用 shell 执行
            parent: 父对象
        """
        super().__init__(parent)
        
        self.command = command
        self.cwd = cwd or str(Path.cwd())
        self.shell = shell
        self._is_running = False
        self._process: Optional[subprocess.Popen] = None
    
    def run(self) -> None:
        """
        线程主方法 - 执行命令并实时读取输出
        """
        self._is_running = True
        
        try:
            # 创建子进程
            # startupinfo 用于在 Windows 下隐藏控制台窗口
            startupinfo = None
            if sys.platform == 'win32':
                startupinfo = subprocess.STARTUPINFO()
                startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                startupinfo.wShowWindow = subprocess.SW_HIDE
            
            self._process = subprocess.Popen(
                self.command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # 合并 stderr 到 stdout
                stdin=subprocess.PIPE,
                cwd=self.cwd,
                shell=self.shell,
                startupinfo=startupinfo,
                # bufsize=1 表示行缓冲
                bufsize=1,
                universal_newlines=False  # 以二进制模式读取，便于控制解码
            )
            
            # 实时读取输出流
            while self._is_running and self._process.poll() is None:
                # 逐行读取，避免阻塞
                line = self._process.stdout.readline()
                if line:
                    # Windows CMD 默认使用 GBK 编码
                    # 使用 errors='replace' 避免解码失败导致程序崩溃
                    try:
                        decoded_line = line.decode('gbk', errors='replace')
                    except UnicodeDecodeError:
                        decoded_line = line.decode('utf-8', errors='replace')
                    
                    # 去掉行尾换行符，QTextEdit.append 会自动添加
                    decoded_line = decoded_line.rstrip('\r\n')
                    
                    if decoded_line:
                        self.output_signal.emit(decoded_line)
                
                # 短暂休眠，避免 CPU 占用过高
                self.msleep(10)
            
            # 读取剩余输出
            remaining = self._process.stdout.read()
            if remaining:
                try:
                    decoded = remaining.decode('gbk', errors='replace')
                except UnicodeDecodeError:
                    decoded = remaining.decode('utf-8', errors='replace')
                
                for line in decoded.strip().split('\n'):
                    if line.strip():
                        self.output_signal.emit(line.rstrip('\r\n'))
            
            # 发送完成信号
            exit_code = self._process.returncode
            self.finished_signal.emit(exit_code)
            
        except FileNotFoundError as e:
            self.error_signal.emit(f"[错误] 命令未找到: {e}")
            self.finished_signal.emit(-1)
            
        except PermissionError as e:
            self.error_signal.emit(f"[错误] 权限不足: {e}")
            self.finished_signal.emit(-1)
            
        except Exception as e:
            self.error_signal.emit(f"[错误] 执行异常: {e}")
            self.finished_signal.emit(-1)
            
        finally:
            self._is_running = False
            self._process = None
    
    def stop(self) -> None:
        """终止正在执行的命令"""
        self._is_running = False
        
        if self._process and self._process.poll() is None:
            try:
                # 先尝试优雅终止
                self._process.terminate()
                # 等待最多 2 秒
                self._process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                # 强制终止
                self._process.kill()
            except Exception:
                pass
    
    def is_running(self) -> bool:
        """检查是否正在执行"""
        return self._is_running


class ScriptWorker(CommandWorker):
    """
    脚本执行器（继承自 CommandWorker）
    
    用于执行脚本文件（.bat, .ps1, .py 等）
    """
    
    def __init__(
        self,
        script_path: str,
        args: Optional[list] = None,
        cwd: Optional[str] = None,
        parent=None
    ):
        """
        初始化脚本执行器
        
        Args:
            script_path: 脚本文件路径
            args: 脚本参数列表
            cwd: 工作目录
            parent: 父对象
        """
        self.script_path = Path(script_path)
        self.args = args or []
        
        # 验证脚本文件存在
        if not self.script_path.exists():
            raise FileNotFoundError(f"脚本文件不存在: {script_path}")
        
        # 根据脚本类型构建命令
        command = self._build_command()
        
        super().__init__(command, cwd, shell=True, parent=parent)
    
    def _build_command(self) -> str:
        """根据脚本类型构建执行命令"""
        suffix = self.script_path.suffix.lower()
        script_str = str(self.script_path)
        args_str = ' '.join(self.args)
        
        if suffix == '.bat' or suffix == '.cmd':
            # Windows 批处理脚本
            return f'cmd /c "{script_str}" {args_str}'
        
        elif suffix == '.ps1':
            # PowerShell 脚本
            return f'powershell -ExecutionPolicy Bypass -File "{script_str}" {args_str}'
        
        elif suffix == '.py':
            # Python 脚本
            return f'python "{script_str}" {args_str}'
        
        elif suffix == '.vbs':
            # VBScript
            return f'cscript //nologo "{script_str}" {args_str}'
        
        else:
            # 其他类型，直接执行
            return f'"{script_str}" {args_str}'
