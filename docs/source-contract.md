# 来源格式与业务语义

夹具文件前部是一段 HTTP Content-Type 头，后部是原始 multipart body。在线 GET /stream 将两者拆成真实 HTTP 响应头与分块返回的正文。支持 image/jpeg、application/x-heartbeat，X-Sequence 在连接内递增但不能当跨连接 ID，X-Time 可回拨。帧是合成字节，不做视觉解码；完整性指部件 framing 与声明长度完整，不承诺相机图像质量。边界只在协议规定的行位置匹配，超限部件须有界跳弃或断连，不无限缓存。

本地演练夹具由 `sampledata.py` 生成，均为虚构业务数据。`sample` 是当前可查看输入，`invalid` 是边界失败输入，其余用于对照来源变体。生成不会下载资源。

本地 HTTP 演练器采用方法与原始路径作为路由键，值为按调用次数返回的响应数组，耗尽后维持最后一项。响应可定义 status、headers、text/base64、require_headers、chunk_bytes、chunk_sizes、chunk_pause、disconnect_after。每次请求保留方法、路径、头与正文摘要表示，便于核对实际访问；演练器只监听回环地址。

原始输入上限 16 MiB 是现有查看入口的保护值，不代表未来持续采集需一次读完整来源。格式含义和截断错误不能被 HTTP 200 覆盖。

## 持续接收与采样语义

`stream.py` 的 `MultipartParser` 只在协议规定的行位置识别边界：分隔线是行首的 `--boundary`（可带少量空白填充）或终止线 `--boundary--`，正文里出现的相似字节（如 `--boundaryX`、行中的 `--boundary`）不算分隔线。部件声明 Content-Length 时按声明长度取正文，正文即使含有完整分隔线字节也照长度计；未声明时扫描下一分隔线。声明长度与实际正文不符属于帧结构错误，当前连接必须放弃。单部件超过 `max_part_bytes` 时有界跳弃：声明长度已知则按长度丢弃，未知则只保留分隔线匹配窗口，解析器缓冲有硬上限。

只有完整结束的帧才进入采样与留存；连接断开时的残片记入缺口（frame_truncated）并随连接一起丢弃，新连接用全新解析器，残片不会拼到下一连接首帧。心跳部件（application/x-heartbeat）只证明来源存活并计数，不作为画面留存。

采样按单调时钟的接收时刻判定（`IntervalSampler`），保留原因记为 first / interval / reconnect_first；间隔内跳过的帧计入 frames_skipped_interval。来源声明的 X-Time 与接收时刻分别落库；帧表只追加，来源时钟回拨或同时间多帧各自成行，行 id 即证据顺序，不按时间覆盖。

落盘与读网络分离：完整帧先写入 spool 目录临时文件再排队等写库，队列满时按 spool_full_drop_new 丢弃新帧并计数（spool_drop 缺口），网络读取不因此被阻塞。来源静默以 idle_timeout 无字节判定（source_silent 缺口），与连接断开分别记录；停采（stop）会关闭阻塞中的读取，写线程排空队列后删除 spool 目录。

缺口种类：connect、connect_failed、bad_response、disconnect、source_silent、part_dropped（part_oversized / headers_too_large）、frame_truncated、framing_error、spool_drop、connection_summary、session_end。
