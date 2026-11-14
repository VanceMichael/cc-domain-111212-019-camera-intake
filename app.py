import argparse
import base64
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import time

from scenario import live_routes
from source import inspect_bytes
from sampledata import samples
from storage import Archive, FrameStore
from stream import IntakeSession
from transport import ReplaySource, fetch

MAX_INPUT = 16*1024*1024

def read_input(path):
    with Path(path).open('rb') as handle:
        data = handle.read(MAX_INPUT+1)
    if len(data) > MAX_INPUT:
        raise ValueError('输入超过接收限制')
    return data

def demo():
    fixtures = samples()
    name = next(key for key in fixtures if key.startswith('sample.'))
    data = fixtures[name]
    routes = {'GET /sample':[{'base64':base64.b64encode(data).decode()}]}
    with tempfile.TemporaryDirectory() as directory, ReplaySource(routes) as server:
        status, headers, raw = fetch(server.url + '/sample')
        if status != 200:
            raise ValueError('来源请求失败')
        rows = inspect_bytes(raw)
        with Archive(Path(directory)/'archive.sqlite3') as archive:
            first = archive.save('local-demo', raw, rows)
            repeated = archive.save('local-demo', raw, rows)
            return dict(input=name, bytes=len(raw), captures=len(archive.list()),
                        same_capture=first==repeated, rows=rows, requests=len(server.requests))

def _phase_report(report, name, session, end_reason):
    stats = session.stats()
    stats['spool_dir_exists'] = os.path.isdir(stats.pop('spool_dir'))
    stats['end_reason'] = end_reason
    report['phases'][name] = stats

def stream_demo():
    """真实启动本地持续流源，演示分帧、采样、丢弃计数、断线重连与停采。"""
    report = {'phases':{}}
    with tempfile.TemporaryDirectory() as directory, ReplaySource(live_routes()) as server:
        store = FrameStore(Path(directory)/'frames.sqlite3')
        session = IntakeSession(server.url + '/live', 'cam-main', store,
                                sample_interval=0.0, max_part_bytes=4096,
                                spool_capacity=2, write_delay=0.2,
                                idle_timeout=1.0, read_timeout=0.1,
                                max_reconnects=3, retry_pause=0.6)
        _phase_report(report, 'stream_and_slow_disk', session, session.run())
        session = IntakeSession(server.url + '/live', 'cam-interval', store,
                                sample_interval=3600.0, max_part_bytes=4096,
                                spool_capacity=4, read_timeout=0.1,
                                idle_timeout=1.0, max_reconnects=0)
        _phase_report(report, 'interval_sampling', session, session.run())
        session = IntakeSession(server.url + '/trickle', 'cam-silence', store,
                                sample_interval=0.0, max_part_bytes=4096,
                                spool_capacity=4, read_timeout=0.1,
                                idle_timeout=0.4, max_reconnects=1, retry_pause=0.05)
        _phase_report(report, 'source_silence', session, session.run())
        stopping = IntakeSession(server.url + '/trickle', 'cam-stop', store,
                                 sample_interval=0.0, max_part_bytes=4096,
                                 spool_capacity=4, read_timeout=0.1,
                                 idle_timeout=30.0, max_reconnects=0)
        worker = threading.Thread(target=stopping.run)
        worker.start()
        time.sleep(0.5)
        stopping.stop()
        worker.join(timeout=5)
        _phase_report(report, 'manual_stop', stopping, 'stop_requested')
        report['phases']['manual_stop']['stopped_promptly'] = not worker.is_alive()
        report['frames'] = store.list_frames()
        report['gaps'] = store.list_gaps()
        report['requests'] = len(server.requests)
        store.close()
    return report

def main(argv=None):
    parser = argparse.ArgumentParser(description='来源检查与原件留存')
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('inspect', 'capture'):
        command = commands.add_parser(name)
        command.add_argument('path')
        if name == 'capture':
            command.add_argument('--database', required=True)
            command.add_argument('--source', required=True)
    listing = commands.add_parser('list')
    listing.add_argument('--database', required=True)
    fixtures = commands.add_parser('fixtures')
    fixtures.add_argument('directory')
    commands.add_parser('demo')
    stream = commands.add_parser('stream')
    stream.add_argument('url')
    stream.add_argument('--database', required=True)
    stream.add_argument('--source', required=True)
    stream.add_argument('--interval', type=float, default=1.0)
    stream.add_argument('--max-part-bytes', type=int, default=262144)
    stream.add_argument('--spool-capacity', type=int, default=8)
    stream.add_argument('--max-seconds', type=float, default=None)
    stream.add_argument('--reconnects', type=int, default=2)
    for name in ('frames', 'gaps'):
        command = commands.add_parser(name)
        command.add_argument('--database', required=True)
    commands.add_parser('stream-demo')
    args = parser.parse_args(argv)
    try:
        if args.command == 'demo':
            result = demo()
        elif args.command == 'stream-demo':
            result = stream_demo()
        elif args.command == 'stream':
            with FrameStore(args.database) as store:
                session = IntakeSession(args.url, args.source, store,
                                        sample_interval=args.interval,
                                        max_part_bytes=args.max_part_bytes,
                                        spool_capacity=args.spool_capacity,
                                        run_seconds=args.max_seconds,
                                        max_reconnects=args.reconnects)
                try:
                    end_reason = session.run()
                except KeyboardInterrupt:
                    session.stop()
                    end_reason = 'interrupted'
                result = dict(end_reason=end_reason, stats=session.stats())
        elif args.command == 'frames':
            with FrameStore(args.database) as store:
                result = store.list_frames()
        elif args.command == 'gaps':
            with FrameStore(args.database) as store:
                result = store.list_gaps()
        elif args.command == 'fixtures':
            destination = Path(args.directory)
            destination.mkdir(parents=True, exist_ok=True)
            for name, raw in samples().items():
                path = destination/name
                if path.exists():
                    raise ValueError('不覆盖已有夹具文件：'+name)
                path.write_bytes(raw)
            result = dict(files=sorted(samples()))
        elif args.command == 'list':
            with Archive(args.database) as archive:
                result = archive.list()
        else:
            raw = read_input(args.path)
            rows = inspect_bytes(raw)
            if args.command == 'inspect':
                result = rows
            else:
                with Archive(args.database) as archive:
                    result = dict(capture_id=archive.save(args.source,raw,rows), rows=len(rows))
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except Exception as error:
        print(json.dumps(dict(error=type(error).__name__, reason=str(error)), ensure_ascii=False), file=sys.stderr)
        return 2

if __name__ == '__main__':
    raise SystemExit(main())
