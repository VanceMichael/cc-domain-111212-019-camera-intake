def samples():
    head = b'Content-Type: multipart/x-mixed-replace; boundary=local-frame\r\n\r\n'
    frame = b'\xff\xd8synthetic-frame\xff\xd9'
    parts = []
    for sequence in [1,2]:
        parts.append(b'--local-frame\r\nContent-Type: image/jpeg\r\nContent-Length: ' + str(len(frame)).encode() + b'\r\nX-Sequence: ' + str(sequence).encode() + b'\r\nX-Time: 2026-11-12T09:00:00Z\r\n\r\n' + frame + b'\r\n')
    document = head + b''.join(parts) + b'--local-frame--\r\n'
    return {'sample.mjpeg':document, 'partial.mjpeg':document[:-25],
            'invalid.mjpeg':b'Content-Type: image/jpeg\r\n\r\nnot a stream'}

def _jpeg(tag):
    return b'\xff\xd8' + tag + b'\xff\xd9'

def _part(boundary, headers, body):
    lines = [b'--' + boundary]
    for name, value in headers:
        lines.append(name + b': ' + value)
    return b'\r\n'.join(lines) + b'\r\n\r\n' + body + b'\r\n'

def _jpeg_part(boundary, sequence, moment, body, final=False):
    headers = [(b'Content-Type', b'image/jpeg'),
               (b'Content-Length', str(len(body)).encode()),
               (b'X-Sequence', str(sequence).encode()),
               (b'X-Time', moment.encode())]
    return _part(boundary, headers, body)

def live_stream():
    """持续图像源的两个连接正文。

    覆盖：心跳部件、可选 Content-Length、正文内相似边界字节、超大部件、
    来源时钟回拨、同时间多帧、连接中断截断与重连后全新首帧。
    均为虚构合成字节，不做视觉解码。
    """
    boundary = b'livecam'
    heartbeat = lambda tag: _part(boundary, [(b'Content-Type', b'application/x-heartbeat')], tag)
    expected = []

    def frame(connection, sequence, moment, body):
        expected.append(dict(connection=connection, sequence=sequence,
                             source_time=moment, body=body))
        return _jpeg_part(boundary, sequence, moment, body)

    conn1 = [frame(1, 1, '2026-11-12T09:00:00Z', _jpeg(b'conn1-frame-1'))]
    conn1.append(heartbeat(b'ping-1'))
    lookalikes = _jpeg(b'conn1-frame-2') + b'\r\n--livecamX-similar\r\nmid-line --livecam bytes'
    conn1.append(_part(boundary,
                       [(b'Content-Type', b'image/jpeg'),
                        (b'X-Sequence', b'2'), (b'X-Time', b'2026-11-12T09:00:01Z')],
                       lookalikes))
    expected.append(dict(connection=1, sequence=2,
                         source_time='2026-11-12T09:00:01Z', body=lookalikes))
    oversized = b'x' * 20000
    conn1.append(_jpeg_part(boundary, 3, '2026-11-12T09:00:01Z', oversized))
    conn1.append(frame(1, 4, '2026-11-12T08:30:00Z', _jpeg(b'conn1-frame-4-rollback')))
    conn1.append(frame(1, 5, '2026-11-12T09:00:01Z', _jpeg(b'conn1-frame-5-sametime')))
    truncated_head = _jpeg_part(boundary, 6, '2026-11-12T09:00:02Z',
                                _jpeg(b'conn1-frame-6-partial'))
    conn1_body = b''.join(conn1)
    cutoff = len(conn1_body) + len(truncated_head) - 7
    conn1_body += truncated_head

    conn2 = [heartbeat(b'ping-2'),
             frame(2, 1, '2026-11-12T09:00:02Z', _jpeg(b'conn2-frame-6')),
             frame(2, 2, '2026-11-12T09:00:03Z', _jpeg(b'conn2-frame-7'))]
    conn2_body = b''.join(conn2) + b'--' + boundary + b'--\r\n'

    return dict(boundary='livecam', conn1=conn1_body, cutoff=cutoff, conn2=conn2_body,
                expected_frames=expected, oversized_declared=len(oversized),
                heartbeats=2)

def trickle_stream():
    """慢速来源正文：首帧完整，第二帧只有残片，用于静默与停采演练。"""
    boundary = b'livecam'
    first = _jpeg_part(boundary, 1, '2026-11-12T09:10:00Z', _jpeg(b'trickle-frame-1'))
    partial = b'--livecam\r\nContent-Type: image/jpeg\r\nContent-Length: 4096\r\n' \
              b'X-Sequence: 2\r\n\r\n' + _jpeg(b'trickle-frame-2-part') + b'z' * 600
    return first + partial
