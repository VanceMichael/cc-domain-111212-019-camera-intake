# 来源格式与业务语义

夹具文件前部是一段 HTTP Content-Type 头，后部是原始 multipart body。在线 GET /stream 将两者拆成真实 HTTP 响应头与分块返回的正文。支持 image/jpeg、application/x-heartbeat，X-Sequence 在连接内递增但不能当跨连接 ID，X-Time 可回拨。帧是合成字节，不做视觉解码；完整性指部件 framing 与声明长度完整，不承诺相机图像质量。边界只在协议规定的行位置匹配，超限部件须有界跳弃或断连，不无限缓存。

本地演练夹具由 `sampledata.py` 生成，均为虚构业务数据。`sample` 是当前可查看输入，`invalid` 是边界失败输入，其余用于对照来源变体。生成不会下载资源。

本地 HTTP 演练器采用方法与原始路径作为路由键，值为按调用次数返回的响应数组，耗尽后维持最后一项。响应可定义 status、headers、text/base64、require_headers、chunk_bytes、disconnect_after。每次请求保留方法、路径、头与正文摘要表示，便于核对实际访问；演练器只监听回环地址。

原始输入上限 16 MiB 是现有查看入口的保护值，不代表未来持续采集需一次读完整来源。格式含义和截断错误不能被 HTTP 200 覆盖。
