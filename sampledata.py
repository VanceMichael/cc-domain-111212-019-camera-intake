def samples():
    head = b'Content-Type: multipart/x-mixed-replace; boundary=local-frame\r\n\r\n'
    frame = b'\xff\xd8synthetic-frame\xff\xd9'
    parts = []
    for sequence in [1,2]:
        parts.append(b'--local-frame\r\nContent-Type: image/jpeg\r\nContent-Length: ' + str(len(frame)).encode() + b'\r\nX-Sequence: ' + str(sequence).encode() + b'\r\nX-Time: 2026-11-12T09:00:00Z\r\n\r\n' + frame + b'\r\n')
    document = head + b''.join(parts) + b'--local-frame--\r\n'
    return {'sample.mjpeg':document, 'partial.mjpeg':document[:-25],
            'invalid.mjpeg':b'Content-Type: image/jpeg\r\n\r\nnot a stream'}
