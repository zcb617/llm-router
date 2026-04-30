"""
数据捕获模块 - 请求/响应拦截，耗时计算
"""
import time
import json
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse


@dataclass
class CapturedRequest:
    """捕获的请求数据"""
    timestamp: str                    # ISO格式时间戳
    url: str                          # 原始请求URL
    method: str                       # HTTP方法
    headers: dict                     # 请求头
    body: Optional[str] = None        # 请求体
    start_time: float = 0.0           # 开始时间(秒)
    call_id: str = ""                 # 唯一调用ID (UUID)
    original_model: str = ""          # 原始模型
    overridden_model: str = ""        # 映射后的模型


@dataclass
class CapturedResponse:
    """捕获的响应数据"""
    status_code: int                  # HTTP状态码
    headers: dict                     # 响应头
    body: Optional[str] = None        # 响应体
    duration_ms: int = 0              # 总耗时(毫秒)
    first_token_ms: Optional[int] = None  # 首字耗时(毫秒)，仅流式
    end_time: float = 0.0             # 结束时间(秒)


class DataCapturer:
    """数据捕获器"""
    
    def capture_request(self, flow) -> CapturedRequest:
        """从mitmproxy flow中捕获请求数据"""
        import datetime
        
        request = flow.request
        body = None
        if request.content:
            try:
                body = request.content.decode("utf-8")
            except:
                body = request.content.decode("latin-1")
        
        return CapturedRequest(
            timestamp=datetime.datetime.now().isoformat(),
            url=request.url,
            method=request.method,
            headers=dict(request.headers),
            body=body,
            start_time=time.time()
        )
    
    def capture_response(self, flow, captured_request: CapturedRequest) -> CapturedResponse:
        """从mitmproxy flow中捕获响应数据"""
        response = flow.response
        
        body = None
        if response.content:
            try:
                body = response.content.decode("utf-8")
            except:
                body = response.content.decode("latin-1")
        
        end_time = time.time()
        duration_ms = int((end_time - captured_request.start_time) * 1000)
        
        return CapturedResponse(
            status_code=response.status_code,
            headers=dict(response.headers),
            body=body,
            duration_ms=duration_ms,
            end_time=end_time
        )
    
    def rewrite_url(self, flow, target_base_url: str, remaining_path: str):
        """重写flow的为目标URL"""
        from urllib.parse import urlparse, urlunparse

        original = flow.request
        # 解析目标基础URL
        parsed_base = urlparse(target_base_url)

        # 处理路径拼接，避免双斜杠
        base_path = parsed_base.path.rstrip("/")
        if remaining_path and not remaining_path.startswith("/"):
            remaining_path = "/" + remaining_path
        full_path = base_path + remaining_path if remaining_path else base_path
        if not full_path:
            full_path = "/"

        # 构建新URL
        new_url = f"{parsed_base.scheme}://{parsed_base.netloc}{full_path}"
        
        # 如果有query参数，附加上
        if original.query:
            from urllib.parse import urlencode
            query_str = urlencode(list(original.query.items()))
            if "?" in new_url:
                new_url += "&" + query_str
            else:
                new_url += "?" + query_str
        
        # 更新flow的请求
        original.url = new_url

        # 更新Host头
        original.headers["Host"] = parsed_base.netloc

        # 更新scheme（端口由 URL 自动解析，无需手动设置）
        original.scheme = parsed_base.scheme

        return new_url
