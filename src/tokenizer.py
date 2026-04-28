"""
Token计算模块 - API响应解析优先，tiktoken本地计算降级
"""
import json
from typing import Optional, Tuple


def count_tokens_from_api_response(response_body: str) -> Optional[Tuple[int, int]]:
    """
    从API响应中提取token数量
    返回: (input_tokens, output_tokens) 或 None
    """
    try:
        data = json.loads(response_body)
        
        # OpenAI兼容格式
        if "usage" in data:
            usage = data["usage"]
            input_tokens = usage.get("prompt_tokens", 0)
            output_tokens = usage.get("completion_tokens", 0)
            return (input_tokens, output_tokens)
        
        # 其他可能的格式
        if "usage" in data:
            usage = data["usage"]
            if "input_tokens" in usage and "output_tokens" in usage:
                return (usage["input_tokens"], usage["output_tokens"])
        
        return None
    except (json.JSONDecodeError, KeyError, TypeError):
        return None


def count_tokens_local(model: str, text: str) -> int:
    """
    使用tiktoken本地计算token数量
    """
    try:
        import tiktoken
        
        # 尝试获取编码器
        try:
            enc = tiktoken.encoding_for_model(model)
        except KeyError:
            # 如果模型不支持，使用cl100k_base作为默认
            enc = tiktoken.get_encoding("cl100k_base")
        
        tokens = enc.encode(text)
        return len(tokens)
    except ImportError:
        # tiktoken未安装，返回估算值
        # 粗略估算：英文约4字符/token，中文约1.5字符/token
        chinese_chars = sum(1 for c in text if '\u4e00' <= c <= '\u9fff')
        other_chars = len(text) - chinese_chars
        return int(chinese_chars / 1.5 + other_chars / 4)
    except Exception:
        # 其他错误，返回估算值
        return len(text) // 3


def calculate_tokens(
    model: str,
    request_body: Optional[str],
    response_body: Optional[str]
) -> Tuple[int, int, str]:
    """
    计算token数量，优先使用API响应，否则本地计算
    返回: (input_tokens, output_tokens, source)
    source: 'api' 或 'local'
    """
    # 优先尝试从API响应提取
    if response_body:
        api_result = count_tokens_from_api_response(response_body)
        if api_result:
            return (api_result[0], api_result[1], "api")
    
    # 降级到本地计算
    input_tokens = 0
    output_tokens = 0
    
    if request_body:
        input_tokens = count_tokens_local(model, request_body)
    
    if response_body:
        # 尝试从响应体提取内容部分
        try:
            data = json.loads(response_body)
            # OpenAI兼容格式
            if "choices" in data:
                content = ""
                for choice in data["choices"]:
                    if "message" in choice and "content" in choice["message"]:
                        content += choice["message"]["content"]
                    elif "delta" in choice and "content" in choice["delta"]:
                        content += choice["delta"]["content"]
                output_tokens = count_tokens_local(model, content)
            else:
                output_tokens = count_tokens_local(model, response_body)
        except:
            output_tokens = count_tokens_local(model, response_body)
    
    return (input_tokens, output_tokens, "local")
