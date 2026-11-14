"""持续 multipart 图像源的分帧接收、定时采样与留存。

只按来源声明的边界与部件头分帧，不做图像识别或视频转码。
内存占用有界：单个部件超限时有界跳弃，待写盘帧经 spool 目录排队，
读网络不会被落盘速度阻塞，来源静默与连接断开分别记录。
"""
import collections
import os
import queue
import select
import socket
import tempfile
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit

CRLF = b'\r\n'
HEADER_END = b'\r\n\r\n'
MAX_PADDING = 8
READ_CHUNK = 65536
MAX_RESPONSE_HEAD = 65536


def _utcnow():
    return datetime.now(timezone.utc).isoformat()


class FramingError(ValueError):
    """来源帧结构不可信，当前连接必须放弃，残片不得带入下一连接。"""


def parse_boundary(content_type):
    """从 HTTP Content-Type 取出 multipart 边界；非相机流类型返回 None。"""
    media, _, params = (content_type or '').partition(';')
    if media.strip().lower() != 'multipart/x-mixed-replace':
        return None
    for param in params.split(';'):
        name, _, value = param.partition('=')
        if name.strip().lower() == 'boundary':
            value = value.strip().strip('"')
            if value and len(value) <= 70:
                return value
    return None


def _connect(url, timeout):
    """建立 TCP 连接并发送最小 HTTP GET，返回 (sock, 已读到的响应字节)。

    不用 http.client 的缓冲读：其 BufferedReader 在 socket 超时后不可复用，
    会把来源静默伪装成读取异常。这里显式管理 socket，配合 select 轮询，
    让静默、断开与主动停止各自可区分。调用方应尽早持有 sock 以便 stop()。
    """
    parsed = urlsplit(url)
    if parsed.scheme not in ('http', ''):
        raise ValueError('仅支持 http 来源')
    host = parsed.hostname or '127.0.0.1'
    port = parsed.port or 80
    path = parsed.path or '/'
    if parsed.query:
        path += '?' + parsed.query
    sock = socket.create_connection((host, port), timeout=timeout)
    try:
        request = ('GET %s HTTP/1.1\r\nHost: %s\r\nAccept: multipart/x-mixed-replace\r\n'
                   'Connection: close\r\n\r\n' % (path, host))
        sock.sendall(request.encode('ascii'))
        return sock, path
    except Exception:
        sock.close()
        raise


def _read_response_head(sock):
    """读取响应状态行与头，返回 (status, headers, body_prefix)。"""
    head = b''
    while HEADER_END not in head:
        chunk = sock.recv(4096)
        if not chunk:
            raise ConnectionError('响应头不完整')
        head += chunk
        if len(head) > MAX_RESPONSE_HEAD:
            raise ValueError('响应头超过限制')
    head, prefix = head.split(HEADER_END, 1)
    lines = head.split(CRLF)
    parts = lines[0].split(b' ', 2)
    if len(parts) < 2 or not parts[1].isdigit():
        raise ValueError('响应状态行无效')
    headers = {}
    for line in lines[1:]:
        name, _, value = line.partition(b':')
        headers[name.strip().lower().decode('ascii', 'replace')] = \
            value.strip().decode('ascii', 'replace')
    return int(parts[1]), headers, prefix


class MultipartParser:
    """按边界与部件头增量分帧；feed 可任意分块，缓冲有上限。"""

    IMAGE_TYPES = ('image/jpeg',)
    HEARTBEAT_TYPES = ('application/x-heartbeat',)

    def __init__(self, boundary, max_part_bytes=262144, max_header_bytes=8192):
        if isinstance(boundary, str):
            boundary = boundary.encode('ascii')
        if not boundary or len(boundary) > 70:
            raise ValueError('边界长度无效')
        self.marker = b'--' + boundary
        self.max_part_bytes = max_part_bytes
        self.max_header_bytes = max_header_bytes
        self._tail = len(self.marker) + MAX_PADDING + 4
        self.buffer = bytearray()
        self.state = 'preamble'
        self.part_headers = None
        self.part_body = None
        self.body_remaining = 0
        self.skip_remaining = 0
        self.final_seen = False
        self._stream_start = True

    def buffer_usage(self):
        used = len(self.buffer)
        if self.part_body is not None:
            used += len(self.part_body)
        return used

    def feed(self, data):
        """喂入任意大小的块，返回本批解析出的事件列表。"""
        events = []
        if data:
            self.buffer += data
        while True:
            if self.state == 'preamble':
                if not self._preamble(events):
                    break
            elif self.state == 'headers':
                if not self._headers(events):
                    break
            elif self.state == 'body_cl':
                if not self._body_cl(events):
                    break
            elif self.state == 'body_scan':
                if not self._body_scan(events):
                    break
            elif self.state == 'skip_n':
                if not self._skip_n(events):
                    break
            elif self.state == 'body_skip':
                if not self._body_skip(events):
                    break
            elif self.state == 'expect_delimiter':
                if not self._expect_delimiter(events):
                    break
            else:
                self.buffer.clear()
                break
        return events

    def close(self):
        """连接结束时调用；未完成的部件记为截断残片，绝不拼到下一连接。"""
        if self.state == 'epilogue':
            return []
        if self.state == 'expect_delimiter':
            # 帧已完整交出；缓冲里至多是不完整的分隔线，不含帧数据
            self.state = 'epilogue'
            return []
        if self.state in ('preamble', 'headers') and not self.buffer:
            self.state = 'epilogue'
            return []
        detail = {'state': self.state, 'buffered_bytes': self.buffer_usage()}
        if self.state == 'skip_n':
            detail['skip_remaining'] = self.skip_remaining
        self.buffer.clear()
        self.part_body = None
        self.part_headers = None
        self.state = 'epilogue'
        return [('truncated', detail)]

    def _delimiter_at(self, pos):
        """buffer[pos] 起若是分隔线，返回 (kind, end)；数据不足返回 'need_more'。"""
        end = pos + len(self.marker)
        if len(self.buffer) < end:
            return 'need_more' if bytes(self.buffer[pos:]) == self.marker[:len(self.buffer) - pos] else None
        if bytes(self.buffer[pos:end]) != self.marker:
            return None
        rest = bytes(self.buffer[end:end + MAX_PADDING + 2])
        if not rest:
            return 'need_more'
        if rest[:2] == b'--':
            return ('final', end + 2)
        if rest[:2] == CRLF:
            return ('part', end + 2)
        if rest[:1] == b'-' or rest[:1] == b'\r':
            return 'need_more' if len(rest) < 2 else None
        if rest[:1] in (b' ', b'\t'):
            idx = rest.find(CRLF)
            if 0 <= idx <= MAX_PADDING and all(c in (32, 9) for c in rest[:idx]):
                return ('part', end + idx + 2)
            if idx < 0 and len(rest) < MAX_PADDING + 2 and all(c in (32, 9) for c in rest):
                return 'need_more'
            return None
        return None

    def _preamble(self, events):
        start = 0
        while True:
            idx = self.buffer.find(self.marker, start)
            if idx < 0:
                break
            line_start = (idx == 0 and self._stream_start) or \
                         (idx > 0 and self.buffer[idx - 1:idx] == b'\n')
            if line_start:
                matched = self._delimiter_at(idx)
                if matched == 'need_more':
                    return False
                if matched:
                    kind, end = matched
                    del self.buffer[:end]
                    if kind == 'final':
                        self._finish()
                    else:
                        self.state = 'headers'
                    return True
            start = idx + 1
        keep = len(self.marker) + 1
        if len(self.buffer) > keep:
            del self.buffer[:len(self.buffer) - keep]
            self._stream_start = False
        return False

    def _headers(self, events):
        end = self.buffer.find(HEADER_END)
        if end < 0:
            if len(self.buffer) > self.max_header_bytes:
                events.append(('dropped', 'headers_too_large', {'limit': self.max_header_bytes}))
                self.state = 'body_skip'
                return True
            return False
        block = bytes(self.buffer[:end])
        del self.buffer[:end + len(HEADER_END)]
        headers = {}
        for line in block.split(CRLF):
            if not line:
                continue
            name, separator, value = line.partition(b':')
            if not separator:
                raise FramingError('部件头缺少冒号')
            headers[name.strip().lower().decode('ascii', 'replace')] = \
                value.strip().decode('ascii', 'replace')
        self.part_headers = headers
        declared = headers.get('content-length')
        if declared is None:
            self.part_body = bytearray()
            self.state = 'body_scan'
            return True
        try:
            length = int(declared)
        except ValueError:
            raise FramingError('Content-Length 非数字：' + declared)
        if length < 0:
            raise FramingError('Content-Length 为负')
        if length > self.max_part_bytes:
            events.append(('dropped', 'part_oversized',
                           {'declared_bytes': length, 'limit': self.max_part_bytes}))
            self.part_headers = None
            self.skip_remaining = length
            self.state = 'skip_n'
            return True
        self.part_body = bytearray()
        self.body_remaining = length
        self.state = 'body_cl'
        return True

    def _emit_part(self, events, body):
        headers = self.part_headers or {}
        content_type = headers.get('content-type', '').split(';')[0].strip().lower()
        if content_type in self.IMAGE_TYPES:
            kind = 'frame'
        elif content_type in self.HEARTBEAT_TYPES:
            kind = 'heartbeat'
        else:
            kind = 'ignored'
        events.append((kind, headers, body))
        self.part_headers = None
        self.part_body = None

    def _body_cl(self, events):
        take = min(self.body_remaining, len(self.buffer))
        if take:
            self.part_body += self.buffer[:take]
            del self.buffer[:take]
            self.body_remaining -= take
        if self.body_remaining:
            return False
        self._emit_part(events, bytes(self.part_body))
        self.state = 'expect_delimiter'
        return True

    def _body_scan(self, events):
        start = 0
        while True:
            idx = self.buffer.find(CRLF + self.marker, start)
            if idx < 0:
                break
            matched = self._delimiter_at(idx + 2)
            if matched == 'need_more':
                return False
            if matched:
                kind, end = matched
                body = bytes(self.buffer[:idx])
                del self.buffer[:end]
                self._emit_part(events, body)
                if kind == 'final':
                    self._finish()
                else:
                    self.state = 'headers'
                return True
            start = idx + 1
        if len(self.buffer) > self.max_part_bytes + self._tail:
            events.append(('dropped', 'part_oversized',
                           {'declared_bytes': None, 'limit': self.max_part_bytes}))
            del self.buffer[:len(self.buffer) - self._tail]
            self.part_headers = None
            self.part_body = None
            self.state = 'body_skip'
            return True
        return False

    def _body_skip(self, events):
        start = 0
        while True:
            idx = self.buffer.find(CRLF + self.marker, start)
            if idx < 0:
                break
            matched = self._delimiter_at(idx + 2)
            if matched == 'need_more':
                return False
            if matched:
                kind, end = matched
                del self.buffer[:end]
                if kind == 'final':
                    self._finish()
                else:
                    self.state = 'headers'
                return True
            start = idx + 1
        if len(self.buffer) > self._tail:
            del self.buffer[:len(self.buffer) - self._tail]
        return False

    def _skip_n(self, events):
        take = min(self.skip_remaining, len(self.buffer))
        if take:
            del self.buffer[:take]
            self.skip_remaining -= take
        if self.skip_remaining:
            return False
        self.state = 'expect_delimiter'
        return True

    def _expect_delimiter(self, events):
        if not self.buffer:
            return False
        if len(self.buffer) < 2:
            if CRLF.startswith(bytes(self.buffer)):
                return False
            raise FramingError('声明长度后未出现分隔线')
        if self.buffer[:2] != CRLF:
            raise FramingError('声明长度后未出现分隔线')
        matched = self._delimiter_at(2)
        if matched == 'need_more':
            return False
        if not matched:
            raise FramingError('声明长度后未出现分隔线')
        kind, end = matched
        del self.buffer[:end]
        if kind == 'final':
            self._finish()
        else:
            self.state = 'headers'
        return True

    def _finish(self):
        self.final_seen = True
        self.state = 'epilogue'
        self.part_headers = None
        self.part_body = None


class IntervalSampler:
    """按单调时钟对完整帧做定时采样；来源时间与墙上时间只记录不参与判定。"""

    def __init__(self, interval, clock=time.monotonic):
        self.interval = interval
        self.clock = clock
        self.last = None
        self.connections = 0

    def reset(self):
        self.connections += 1
        self.last = None

    def decide(self):
        now = self.clock()
        if self.last is None:
            self.last = now
            return 'first' if self.connections <= 1 else 'reconnect_first'
        if now - self.last >= self.interval:
            self.last = now
            return 'interval'
        return None


class _SpoolWriter(threading.Thread):
    """独占落盘的写线程；帧体经 spool 文件排队，内存不随积压增长。"""

    def __init__(self, store, source, frame_queue, write_delay=0.0):
        super().__init__(daemon=True)
        self.store = store
        self.source = source
        self.queue = frame_queue
        self.write_delay = write_delay
        self.events = collections.deque()
        self.events_lock = threading.Lock()
        self.errors = []

    def add_event(self, event):
        with self.events_lock:
            self.events.append(event)

    def run(self):
        while True:
            try:
                item = self.queue.get(timeout=0.1)
            except queue.Empty:
                self._drain_events()
                continue
            if item is None:
                break
            self._drain_events()
            if self.write_delay:
                time.sleep(self.write_delay)
            try:
                with open(item['path'], 'rb') as handle:
                    raw = handle.read()
                self.store.save_frame(self.source, item['connection'], item['sequence'],
                                      item['source_time'], item['received_at'],
                                      item['received_mono'], item['reason'], raw)
            except Exception as error:
                self.errors.append(repr(error))
            finally:
                try:
                    os.unlink(item['path'])
                except OSError:
                    pass
        self._drain_events()

    def _drain_events(self):
        while True:
            with self.events_lock:
                if not self.events:
                    return
                event = self.events.popleft()
            try:
                self.store.save_gap(self.source, event['connection'], event['kind'],
                                    event['detail'], at=event['at'])
            except Exception as error:
                self.errors.append(repr(error))

    def finish(self):
        self.queue.put(None)
        self.join(timeout=10)


class IntakeSession:
    """单来源持续接收：连接、分帧、采样、有界 spool、落盘与缺口记录。

    读网络与写盘分离：落盘慢时按 spool_capacity 丢弃新帧并计数，
    不会反压网络读取；来源静默以 idle_timeout 判定并单独记录，
    不会把下游阻塞误当作相机离线。stop() 关闭阻塞读取并清理临时文件。
    """

    def __init__(self, url, source, store, *, sample_interval=1.0,
                 max_part_bytes=262144, spool_capacity=8, spool_root=None,
                 idle_timeout=2.5, read_timeout=0.25, connect_timeout=2.0,
                 max_reconnects=2, retry_pause=0.1, run_seconds=None,
                 write_delay=0.0, clock=time.monotonic):
        if not source.strip():
            raise ValueError('来源标识不能为空')
        self.url = url
        self.source = source
        self.store = store
        self.sampler = IntervalSampler(sample_interval, clock)
        self.max_part_bytes = max_part_bytes
        self.spool_capacity = spool_capacity
        self.spool_root = spool_root
        self.idle_timeout = idle_timeout
        self.read_timeout = read_timeout
        self.connect_timeout = connect_timeout
        self.max_reconnects = max_reconnects
        self.retry_pause = retry_pause
        self.run_seconds = run_seconds
        self.write_delay = write_delay
        self.clock = clock
        self.counters = collections.Counter()
        self.spool_dir = None
        self.writer = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._sock = None
        self._spool_seq = 0
        self._pending_drops = 0

    def run(self):
        """阻塞运行直到流正常结束、重连耗尽、时间预算用完或 stop() 被调用。"""
        self.spool_dir = tempfile.mkdtemp(prefix='camera-intake-spool-', dir=self.spool_root)
        frame_queue = queue.Queue(maxsize=self.spool_capacity)
        self.writer = _SpoolWriter(self.store, self.source, frame_queue, self.write_delay)
        self.writer.start()
        deadline = None if self.run_seconds is None else self.clock() + self.run_seconds
        attempts = 0
        end_reason = 'stop_requested'
        try:
            while not self._stop.is_set():
                attempts += 1
                self._count('connections')
                self._emit(attempts, 'connect', {'url': self.url})
                start = dict(self.counters)
                outcome = self._run_connection(attempts, deadline)
                self._flush_drops(attempts)
                summary = {key: self.counters[key] - start.get(key, 0)
                           for key in set(self.counters) | set(start)}
                self._emit(attempts, 'connection_summary', summary)
                if outcome == 'final':
                    end_reason = 'stream_final'
                    break
                if outcome == 'time_budget':
                    end_reason = 'time_budget'
                    break
                if self._stop.is_set():
                    break
                if attempts > self.max_reconnects:
                    end_reason = 'reconnects_exhausted'
                    break
                if deadline is not None and self.clock() >= deadline:
                    end_reason = 'time_budget'
                    break
                self._stop.wait(self.retry_pause)
            self._emit(attempts, 'session_end',
                       {'reason': end_reason, 'connections': attempts})
        finally:
            self.writer.finish()
            cleaned = 0
            if self.spool_dir and os.path.isdir(self.spool_dir):
                for name in os.listdir(self.spool_dir):
                    try:
                        os.unlink(os.path.join(self.spool_dir, name))
                        cleaned += 1
                    except OSError:
                        pass
                try:
                    os.rmdir(self.spool_dir)
                except OSError:
                    pass
            self._count('spool_files_cleaned', cleaned)
        return end_reason

    def stop(self):
        """主动停采：关闭阻塞中的读取，run() 随后清理临时文件并返回。"""
        self._stop.set()
        sock = self._sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass

    def stats(self):
        with self._lock:
            result = dict(self.counters)
        result['writer_errors'] = list(self.writer.errors) if self.writer else []
        result['spool_dir'] = self.spool_dir
        return result

    def _count(self, name, amount=1):
        with self._lock:
            self.counters[name] += amount

    def _emit(self, connection, kind, detail):
        self.writer.add_event({'connection': connection, 'kind': kind,
                               'detail': detail, 'at': _utcnow()})

    def _flush_drops(self, connection):
        if self._pending_drops:
            self._emit(connection, 'spool_drop',
                       {'count': self._pending_drops, 'policy': 'spool_full_drop_new'})
            self._pending_drops = 0

    def _run_connection(self, connection_no, deadline):
        sock = None
        try:
            sock, _ = _connect(self.url, self.connect_timeout)
            self._sock = sock
            status, headers, prefix = _read_response_head(sock)
        except (OSError, ValueError) as error:
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
            self._sock = None
            if self._stop.is_set():
                return 'stopped'
            self._emit(connection_no, 'connect_failed',
                       {'error': type(error).__name__ + ': ' + str(error)})
            return 'retry'
        try:
            if status != 200:
                self._emit(connection_no, 'bad_response', {'status': status})
                return 'retry'
            if 'chunked' in headers.get('transfer-encoding', '').lower():
                self._emit(connection_no, 'bad_response',
                           {'transfer_encoding': 'chunked'})
                return 'retry'
            boundary = parse_boundary(headers.get('content-type', ''))
            if boundary is None:
                self._emit(connection_no, 'bad_response',
                           {'content_type': headers.get('content-type', '')})
                return 'retry'
            parser = MultipartParser(boundary, max_part_bytes=self.max_part_bytes)
            self.sampler.reset()
            sock.setblocking(False)
            pending = bytearray(prefix)
            last_byte = self.clock()
            try:
                while not self._stop.is_set():
                    if deadline is not None and self.clock() >= deadline:
                        return 'time_budget'
                    if pending:
                        chunk = bytes(pending)
                        pending.clear()
                    else:
                        try:
                            readable = select.select([sock], [], [], self.read_timeout)[0]
                        except (OSError, ValueError) as error:
                            if self._stop.is_set():
                                return 'stopped'
                            self._count('disconnects')
                            self._emit(connection_no, 'disconnect',
                                       {'error': type(error).__name__ + ': ' + str(error)})
                            return 'retry'
                        if not readable:
                            if self.clock() - last_byte > self.idle_timeout:
                                self._count('source_silent')
                                self._emit(connection_no, 'source_silent',
                                           {'idle_seconds': self.idle_timeout})
                                return 'retry'
                            continue
                        try:
                            chunk = sock.recv(READ_CHUNK)
                        except BlockingIOError:
                            continue
                        except OSError as error:
                            if self._stop.is_set():
                                return 'stopped'
                            self._count('disconnects')
                            self._emit(connection_no, 'disconnect',
                                       {'error': type(error).__name__ + ': ' + str(error)})
                            return 'retry'
                    if not chunk:
                        if self._stop.is_set():
                            return 'stopped'
                        self._count('disconnects')
                        self._emit(connection_no, 'disconnect', {'error': 'eof'})
                        return 'retry'
                    last_byte = self.clock()
                    self._count('bytes_received', len(chunk))
                    try:
                        events = parser.feed(chunk)
                    except FramingError as error:
                        self._count('framing_errors')
                        self._emit(connection_no, 'framing_error', {'error': str(error)})
                        return 'retry'
                    for event in events:
                        self._handle_event(event, connection_no)
                    if parser.final_seen:
                        return 'final'
                return 'stopped'
            finally:
                # 除正常结束外，连接残片一律记为截断，绝不带入下一连接
                self._close_parser(parser, connection_no)
        finally:
            try:
                sock.close()
            except OSError:
                pass
            self._sock = None

    def _close_parser(self, parser, connection_no):
        for event in parser.close():
            if event[0] == 'truncated':
                self._count('frames_truncated')
                self._emit(connection_no, 'frame_truncated', event[1])

    def _handle_event(self, event, connection_no):
        kind = event[0]
        if kind == 'frame':
            _, headers, body = event
            self._count('frames_parsed')
            reason = self.sampler.decide()
            if reason is None:
                self._count('frames_skipped_interval')
                return
            meta = {'connection': connection_no,
                    'sequence': _int_or_none(headers.get('x-sequence')),
                    'source_time': headers.get('x-time'),
                    'received_at': _utcnow(),
                    'received_mono': self.clock(),
                    'reason': reason}
            self._spool_or_drop(meta, body, connection_no)
        elif kind == 'heartbeat':
            self._count('heartbeats')
        elif kind == 'ignored':
            self._count('parts_ignored')
        elif kind == 'dropped':
            _, reason, detail = event
            counter = {'part_oversized': 'parts_dropped_oversized',
                       'headers_too_large': 'parts_dropped_headers_too_large'}.get(
                           reason, 'parts_dropped_other')
            self._count(counter)
            self._emit(connection_no, 'part_dropped', dict(detail, reason=reason))

    def _spool_or_drop(self, meta, body, connection_no):
        if self.writer.queue.full():
            self._count('frames_dropped_spool_full')
            self._pending_drops += 1
            return
        self._spool_seq += 1
        path = os.path.join(self.spool_dir,
                            'frame-%03d-%06d.part' % (connection_no, self._spool_seq))
        with open(path, 'wb') as handle:
            handle.write(body)
        try:
            self.writer.queue.put_nowait(dict(meta, path=path))
        except queue.Full:
            try:
                os.unlink(path)
            except OSError:
                pass
            self._count('frames_dropped_spool_full')
            self._pending_drops += 1
            return
        self._count('frames_retained')


def _int_or_none(value):
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None
