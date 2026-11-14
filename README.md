# camera-intake

持续图像源的分帧接收与采样所用的来源检查与原件留存工具。

## 当前能力

`source.py` 查看来源的现有结构，不把检查结果冒充完整业务采集。
`stream.py` 持续接收 multipart 图像源：按来源声明的边界与部件头增量分帧，跨块边界、可选 Content-Length 与正文相似字节都按协议位置处理；单部件超限有界跳弃，待写盘帧经 spool 目录排队，内存占用有上限。定时采样按单调时钟判定，来源时间与接收时刻分别记录；落盘慢时按容量丢弃新帧并计数，网络阻塞不会被当成相机离线；断线可重连，上一连接残片不拼到下一连接；主动停采会关闭阻塞读取并清理临时文件。不做图像识别或视频转码。
`storage.py` 将原始字节、来源标识、摘要和检查结果一起保存在本地 SQLite，重复原件保留同一收件编号；`FrameStore` 以只追加方式留存采样帧与缺口说明，时钟回拨或同时间多帧各自成行，不覆盖已有证据。
`sampledata.py` 构造可重复的本地数据；`transport.py` 提供独立的回环 HTTP 演练源与请求记录。
解释范围和来源变体见 `docs/source-contract.md`，当前检查测试与将来新增业务能力的测试应分开保留。

## 环境

Python 3.11 或以上，仅标准库，无需凭据和外部数据。使用容器时可执行 `docker build -t camera-intake .`，依赖及镜像须在正式运行前准备。
运行入口、测试与演示均不安装包，不访问公网。数据库使用临时路径，不向代码目录写入业务文件。

## 测试与构建

构建工具版本固定在 `requirements-build.txt`，在环境准备阶段执行 `python3 -m pip install -r requirements-build.txt`；容器构建已包含此步骤。应用运行仍仅使用标准库，测试及演示不安装依赖。

```sh
python3 -m unittest discover -s tests -v
python3 -m compileall -q app.py source.py sampledata.py storage.py transport.py scenario.py stream.py
python3 -m build --wheel --no-isolation
```

## 快速演示

```sh
python3 app.py demo
```

演示真实启动本地来源、领取原件、执行检查并保存临时数据库，展示检查结果与重复收件身份，退出后自动清理。正常在数秒内完成，不以预录日志代替执行。

## 持续接收演示

```sh
python3 app.py stream-demo
```

真实启动本地持续流源，依次演示任意分块与心跳部件、超大部件有界跳弃、慢写盘时的丢弃计数、断线截断与重连、来源静默识别、主动停采清理，输出实际留存的帧（来源时间、接收时刻、采样原因）与未保留原因（缺口种类与计数），数秒内完成。

对真实来源持续采样（仅 http，Ctrl-C 或 --max-seconds 结束）：

```sh
python3 app.py stream http://127.0.0.1:8000/live --database /tmp/camera-intake.sqlite3 --source cam-1 --interval 1.0
python3 app.py frames --database /tmp/camera-intake.sqlite3
python3 app.py gaps --database /tmp/camera-intake.sqlite3
```

## 单独检查与收件

```sh
python3 app.py fixtures /tmp/camera-intake-fixtures
python3 app.py inspect /tmp/camera-intake-fixtures/sample.mjpeg
python3 app.py capture /tmp/camera-intake-fixtures/sample.mjpeg --database /tmp/camera-intake.sqlite3 --source local-partner
python3 app.py list --database /tmp/camera-intake.sqlite3
```

生成夹具不会覆盖已有文件。运行错误以 JSON 输出到标准错误并返回退出码 2；原始输入无效时不生成成功收件。内部原件与任何对外展示副本应分开管理，不写入真实个人数据。
