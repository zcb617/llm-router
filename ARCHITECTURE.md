# llm-router 系统架构设计

## 1. 总体原则

llm-router 采用“先网络、后业务”的两阶段架构。网络传输和业务内容判断必须分离，适用于所有上游厂家、所有协议和所有业务场景。

```text
上游服务
    ↓
网络传输层 / relay
    ↓
完整响应数据
    ↓
业务协议解析层
    ↓
最终调用结果
```

## 2. 网络传输层

### 2.1 relay 的职责

relay 是网络传输层的中继组件，位于客户端和上游服务之间：

```text
客户端  ← HTTP 连接 →  relay  ← HTTP 连接 →  上游服务
```

relay 只负责：

- 建立和维护 HTTP 连接；
- 接收上游响应；
- 将响应数据流转发给下游；
- 处理 HTTP 状态码；
- 判断数据传输是否完成；
- 处理网络、连接、读取、写入、chunk 和关闭异常。

relay 不负责：

- 解析 `message_stop`；
- 解析 `response.completed`；
- 校验业务字段；
- 判断业务成功或失败；
- 修改业务结果。

### 2.2 网络层判断顺序

网络判断必须在网络异常处理边界内完成：

1. 检查 HTTP 状态；
2. 传输响应数据；
3. 确认全部数据已经传输完成；
4. 确认没有网络、读取、写入、chunk 或 relay 异常；
5. 网络层确认成功后，才允许进入业务层。

只要网络层发生异常，网络层标记失败并停止后续传输，业务层不进行成功或失败判断。

## 3. 业务协议层

业务层只能在网络层确认数据完整传输结束，并且已经拿到完整数据后执行。

业务层按照具体协议使用对应的业务规则：

- OpenAI Responses：检查 `response.completed` 及相关业务字段；
- Anthropic Messages：检查 `message_stop` 及相关业务字段；
- 其他厂家或协议：使用该协议定义的结束标记和业务字段。

`message_stop`、`response.completed` 等字段只能判断业务内容，不能用来判断网络传输是否成功。

业务层需要校验：

- 是否存在协议要求的结束标记；
- 必需字段是否存在；
- 字段内容是否符合协议和业务要求；
- 完整数据是否代表业务成功。

## 4. 状态模型

网络状态和业务状态必须独立记录，不得互相覆盖：

```text
transport_status
transport_error

business_status
business_error
```

状态关系如下：

```text
网络传输失败 → 记录网络失败 → 不进入业务判断
网络传输成功 → 交给业务层判断
业务校验失败 → 记录业务失败
网络传输成功 + 业务校验成功 → 调用成功
```

最终调用只有在网络传输成功且业务校验成功时才算成功，但网络层和业务层的原始状态、错误和诊断信息必须分别保留。

## 5. 不允许的实现方式

- 不得在 relay 中解析业务结束标记；
- 不得在 relay 的网络异常处理分支中判断业务成功或失败；
- 不得用 `message_stop` 或 `response.completed` 证明网络传输成功；
- 不得用网络异常直接覆盖业务状态；
- 不得用业务异常直接覆盖网络状态；
- 不得把网络传输判断和业务协议判断放入同一个职责或共享异常分支；
- 不得因为修复一个协议或厂家路径而改变其他协议、厂家和业务场景的状态判断。

## 6. 完整业务的处理模型

一次完整业务 = 从接收到客户端请求，到返回给客户端的整趟调用（流式、非流式都是）。

异常处理针对**整个业务**，不针对单个数据包。用服务层的一个大异常处理把整趟包住：

```python
# ===== service 业务层 =====
def handle_call(flow):
    # --- 网络层：只传数据，出错就抛，跳出网络层 ---
    try:
        # 流式：逐包收、逐包发客户端，同时组装完整数据包
        # 非流式：一次性收发
        # 任何网络问题（断连/读写/chunk/关闭异常）都抛网络异常
        transport_status = "success"
        transport_error = None
    except NetworkError as e:           # 网络异常，跳出网络层
        transport_status = "failed"
        transport_error = str(e)
        # 已组装的那部分数据仍保留，供业务层判断

    # --- 业务层：不管网络成没成，都走到这，拿完整数据包判一次 ---
    # 完整数据包 = 写日志用的那份拼接好的 body，直接复用，不新建变量、不新建一套逻辑
    body = 完整数据包

    # 按三种协议类型分别判定，看整个数据包完不完整
    protocol = detect_protocol(flow)    # anthropic / responses / chat
    if protocol == "anthropic":
        # 流式看 message_stop；非流式看 stop_reason
        business_status = "success" if 数据包含完整结束标记 else "failed"
    elif protocol == "responses":
        # 流式看 response.completed；非流式看 status=completed
        business_status = "success" if 数据包含完整结束标记 else "failed"
    elif protocol == "chat":
        # 流式看 finish_reason + [DONE]；非流式看 finish_reason
        business_status = "success" if 数据包含完整结束标记 else "failed"

    # 业务优先于网络：最终成败只看数据包完不完整
    call_status = business_status

    save_log(transport_status, transport_error, business_status, business_error, call_status)
```

### 6.1 核心规则

- **网络失败要抛出异常**，由业务层捕获；网络层自己不判业务成败。
- **业务判断的是"当前数据包是否完整"，完整就是成功。** llm-router 是处理 LLM 数据包的，只要数据包完整，业务就是 OK。
- **业务优先级高于网络。** 网络出错但数据包已完整，业务仍算成功；数据包不完整才算失败。
- **成败是整个业务的结果**（service 成功=成功、service 失败=失败），不是 service 里某个函数（某个数据包、某个环节）的局部成败。
- 流式接收只是业务中的一个环节，一个流式数据包正确不代表整个业务正确；判断业务必须拿**完整数据包**。
- 业务判断只在业务层（service）这一个地方做，response() 和 error() 都汇入同一处收尾判定；error() 作为网络层只记录网络失败并抛出，不调用业务判断。
- 完整数据包复用现有写日志时拼接好的那份，不新建变量、不新建一套拼接逻辑。

## 7. 扩展原则

网络传输层保持统一，所有厂家共用网络传输和 relay 规则。业务层通过协议对应的解析器和校验器扩展，各协议只负责自己的业务字段和结束标记，不改变网络层职责。
