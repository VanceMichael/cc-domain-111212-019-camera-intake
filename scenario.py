import base64
import json
from sampledata import live_stream, samples, trickle_stream

def response(raw, status=200, headers=None, **options):
    options.setdefault('chunk_bytes', 3)
    return dict(base64=base64.b64encode(raw).decode(),status=status,
                headers=headers or {},**options)

def scenario():
    fixtures = samples()
    routes = {}

    raw = fixtures['sample.mjpeg']
    header, separator, body = raw.partition(b'\r\n\r\n')
    media = header.split(b':',1)[1].strip().decode()
    routes['GET /stream'] = [response(body,headers={'Content-Type':media}),
                            response(body,headers={'Content-Type':media},disconnect_after=70)]
    return routes

def live_routes():
    """持续图像源演练：任意分块、超大部件、慢写盘对照、断线重连与慢速源。"""
    stream = live_stream()
    media = 'multipart/x-mixed-replace; boundary=' + stream['boundary']
    headers = {'Content-Type': media}
    routes = {
        'GET /live': [
            response(stream['conn1'], headers=headers,
                     chunk_sizes=[1, 7, 3, 64, 2, 256, 5, 1024],
                     disconnect_after=stream['cutoff']),
            response(stream['conn2'], headers=headers,
                     chunk_sizes=[9, 4, 128, 1, 33]),
        ],
        'GET /trickle': [
            response(trickle_stream(), headers=headers,
                     chunk_bytes=512, chunk_pause=2.0),
        ],
    }
    return routes

def main():
    import argparse
    from transport import ReplaySource
    parser = argparse.ArgumentParser(description='来源协议本地演练入口')
    parser.add_argument('--export', action='store_true')
    args = parser.parse_args()
    routes = scenario()
    if args.export:
        print(json.dumps(routes,ensure_ascii=False,sort_keys=True))
        return
    with ReplaySource(routes) as server:
        print(json.dumps({'url':server.url,'routes':list(routes)},ensure_ascii=False),flush=True)
        try:
            server.thread.join()
        except KeyboardInterrupt:
            pass

if __name__ == '__main__':
    main()
