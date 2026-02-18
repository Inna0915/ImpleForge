"""
Elasticsearch 工作线程 - 封装 ES HTTP API 操作

依赖安装:
    pip install requests

功能:
- 索引列表获取
- 文档 CRUD 操作
- 分页查询
- Basic Auth 支持
"""

import json
from typing import Any, Dict, List, Optional, Tuple
from PySide6.QtCore import QThread, Signal


class ESClient:
    """
    Elasticsearch HTTP 客户端
    
    封装 ES REST API 调用
    """
    
    def __init__(self, host: str, port: int, username: str = "", password: str = ""):
        """
        初始化 ES 客户端
        
        Args:
            host: ES 主机地址
            port: ES 端口
            username: 用户名（可选）
            password: 密码（可选）
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.base_url = f"http://{host}:{port}"
        
        # 导入 requests
        try:
            import requests
            from requests.auth import HTTPBasicAuth
            self.requests = requests
            self.auth = HTTPBasicAuth(username, password) if username and password else None
        except ImportError:
            raise ImportError("缺少 requests 库，请执行: pip install requests")
    
    def _request(self, method: str, endpoint: str, **kwargs) -> Tuple[bool, Any]:
        """
        发送 HTTP 请求
        
        Args:
            method: HTTP 方法 (GET, POST, PUT, DELETE)
            endpoint: API 端点（不含 base_url）
            **kwargs: 传递给 requests 的参数
            
        Returns:
            (success, data)
        """
        url = f"{self.base_url}{endpoint}"
        
        try:
            response = self.requests.request(
                method=method,
                url=url,
                auth=self.auth,
                timeout=30,
                verify=False,  # 忽略 SSL 验证（内网环境）
                **kwargs
            )
            
            if response.status_code in [200, 201]:
                return True, response.json()
            elif response.status_code == 404:
                return False, "资源不存在"
            elif response.status_code == 401:
                return False, "认证失败：用户名或密码错误"
            else:
                return False, f"HTTP {response.status_code}: {response.text[:200]}"
                
        except self.requests.exceptions.ConnectionError:
            return False, "连接失败：无法连接到 Elasticsearch"
        except self.requests.exceptions.Timeout:
            return False, "连接超时"
        except Exception as e:
            return False, f"请求异常: {str(e)}"
    
    def list_indices(self) -> Tuple[bool, List[Dict]]:
        """
        获取索引列表
        
        Returns:
            (success, indices_list)
        """
        success, data = self._request("GET", "/_cat/indices?format=json&bytes=b")
        if success:
            # 解析索引信息
            indices = []
            for idx in data:
                indices.append({
                    'name': idx.get('index', ''),
                    'docs_count': idx.get('docs.count', '0'),
                    'store_size': idx.get('store.size', '0b'),
                    'health': idx.get('health', 'unknown'),
                    'status': idx.get('status', 'unknown')
                })
            return True, indices
        return False, data
    
    def search_docs(self, index: str, page: int = 1, size: int = 20) -> Tuple[bool, Dict]:
        """
        搜索文档
        
        Args:
            index: 索引名称
            page: 页码（从1开始）
            size: 每页数量
            
        Returns:
            (success, result)
        """
        from_val = (page - 1) * size
        
        payload = {
            "from": from_val,
            "size": size,
            "query": {"match_all": {}},
            "sort": [{"_id": {"order": "asc"}}]
        }
        
        return self._request(
            "POST",
            f"/{index}/_search",
            json=payload,
            headers={"Content-Type": "application/json"}
        )
    
    def get_doc(self, index: str, doc_id: str) -> Tuple[bool, Dict]:
        """
        获取单个文档
        
        Args:
            index: 索引名称
            doc_id: 文档 ID
            
        Returns:
            (success, doc)
        """
        return self._request("GET", f"/{index}/_doc/{doc_id}")
    
    def create_doc(self, index: str, data: Dict) -> Tuple[bool, Dict]:
        """
        创建文档
        
        Args:
            index: 索引名称
            data: 文档数据
            
        Returns:
            (success, result)
        """
        return self._request(
            "POST",
            f"/{index}/_doc",
            json=data,
            headers={"Content-Type": "application/json"}
        )
    
    def update_doc(self, index: str, doc_id: str, data: Dict) -> Tuple[bool, Dict]:
        """
        更新文档（直接覆盖）
        
        Args:
            index: 索引名称
            doc_id: 文档 ID
            data: 新文档数据
            
        Returns:
            (success, result)
        """
        return self._request(
            "PUT",
            f"/{index}/_doc/{doc_id}",
            json=data,
            headers={"Content-Type": "application/json"}
        )
    
    def delete_doc(self, index: str, doc_id: str) -> Tuple[bool, str]:
        """
        删除文档
        
        Args:
            index: 索引名称
            doc_id: 文档 ID
            
        Returns:
            (success, message)
        """
        success, data = self._request("DELETE", f"/{index}/_doc/{doc_id}")
        if success:
            return True, f"文档 {doc_id} 已删除"
        return False, str(data)


class ESWorker(QThread):
    """
    Elasticsearch 异步工作线程
    
    避免 UI 卡顿
    """
    
    # 信号定义
    indices_ready = Signal(list)           # 索引列表就绪
    docs_ready = Signal(dict)              # 文档列表就绪
    doc_ready = Signal(dict)               # 单个文档就绪
    operation_finished = Signal(bool, str) # 操作完成 (success, message)
    error_occurred = Signal(str)           # 错误发生
    
    def __init__(self, profile: Dict[str, Any], parent=None):
        """
        初始化工作线程
        
        Args:
            profile: ES 连接配置
            parent: 父对象
        """
        super().__init__(parent)
        
        self.profile = profile
        self.client: Optional[ESClient] = None
        self._operation: str = ""
        self._params: Dict = {}
    
    def setup_client(self) -> bool:
        """初始化客户端"""
        try:
            self.client = ESClient(
                host=self.profile.get("host", "localhost"),
                port=self.profile.get("port", 9200),
                username=self.profile.get("username", ""),
                password=self.profile.get("password", "")
            )
            return True
        except Exception as e:
            self.error_occurred.emit(f"客户端初始化失败: {e}")
            return False
    
    def list_indices(self):
        """异步获取索引列表"""
        self._operation = "list_indices"
        self.start()
    
    def search_docs(self, index: str, page: int = 1, size: int = 20):
        """异步搜索文档"""
        self._operation = "search_docs"
        self._params = {"index": index, "page": page, "size": size}
        self.start()
    
    def get_doc(self, index: str, doc_id: str):
        """异步获取文档"""
        self._operation = "get_doc"
        self._params = {"index": index, "doc_id": doc_id}
        self.start()
    
    def create_doc(self, index: str, data: Dict):
        """异步创建文档"""
        self._operation = "create_doc"
        self._params = {"index": index, "data": data}
        self.start()
    
    def update_doc(self, index: str, doc_id: str, data: Dict):
        """异步更新文档"""
        self._operation = "update_doc"
        self._params = {"index": index, "doc_id": doc_id, "data": data}
        self.start()
    
    def delete_doc(self, index: str, doc_id: str):
        """异步删除文档"""
        self._operation = "delete_doc"
        self._params = {"index": index, "doc_id": doc_id}
        self.start()
    
    def run(self):
        """执行异步操作"""
        if not self.client and not self.setup_client():
            return
        
        try:
            if self._operation == "list_indices":
                success, data = self.client.list_indices()
                if success:
                    self.indices_ready.emit(data)
                else:
                    self.error_occurred.emit(str(data))
            
            elif self._operation == "search_docs":
                p = self._params
                success, data = self.client.search_docs(
                    p["index"], p["page"], p["size"]
                )
                if success:
                    self.docs_ready.emit(data)
                else:
                    self.error_occurred.emit(str(data))
            
            elif self._operation == "get_doc":
                p = self._params
                success, data = self.client.get_doc(p["index"], p["doc_id"])
                if success:
                    self.doc_ready.emit(data)
                else:
                    self.error_occurred.emit(str(data))
            
            elif self._operation == "create_doc":
                p = self._params
                success, data = self.client.create_doc(p["index"], p["data"])
                self.operation_finished.emit(success, 
                    "文档创建成功" if success else str(data))
            
            elif self._operation == "update_doc":
                p = self._params
                success, data = self.client.update_doc(
                    p["index"], p["doc_id"], p["data"]
                )
                self.operation_finished.emit(success,
                    "文档更新成功" if success else str(data))
            
            elif self._operation == "delete_doc":
                p = self._params
                success, msg = self.client.delete_doc(p["index"], p["doc_id"])
                self.operation_finished.emit(success, msg)
                
        except Exception as e:
            self.error_occurred.emit(f"操作异常: {e}")
