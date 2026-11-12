def inspect_bytes(data):
    if not data.startswith(b'Content-Type: multipart/x-mixed-replace; boundary='):
        raise ValueError('缺少相机流媒体类型与边界')
    header, separator, body = data.partition(b'\r\n\r\n')
    if not separator:
        raise ValueError('流头不完整')
    boundary = header.split(b'boundary=', 1)[1].strip().strip(b'"')
    if not boundary or len(boundary) > 70:
        raise ValueError('边界长度无效')
    return [{'boundary': boundary.decode('ascii'), 'body_bytes': len(body),
             'delimiter_occurrences': body.count(b'--' + boundary)}]
