import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest

import app
from sampledata import live_stream, samples, trickle_stream
from scenario import live_routes
from storage import FrameStore
from stream import (FramingError, IntervalSampler, IntakeSession, MultipartParser,
                    parse_boundary)
from transport import ReplaySource

BOUNDARY = 'livecam'


def jpeg(tag):
    return b'\xff\xd8' + tag + b'\xff\xd9'


def part(headers, body, boundary=BOUNDARY):
    lines = [b'--' + boundary.encode()]
    for name, value in headers:
        lines.append(name + b': ' + value)
    return b'\r\n'.join(lines) + b'\r\n\r\n' + body + b'\r\n'


def jpeg_part(sequence, moment, body, boundary=BOUNDARY):
    return part([(b'Content-Type', b'image/jpeg'),
                 (b'Content-Length', str(len(body)).encode()),
                 (b'X-Sequence', str(sequence).encode()),
                 (b'X-Time', moment.encode())], body, boundary)


def feed_in_chunks(parser, data, size):
    events = []
    for position in range(0, len(data), size):
        events += parser.feed(data[position:position + size])
    return events


class ParserTests(unittest.TestCase):
    def test_frames_across_single_byte_chunks(self):
        body = samples()['sample.mjpeg'].partition(b'\r\n\r\n')[2]
        parser = MultipartParser('local-frame')
        events = feed_in_chunks(parser, body, 1)
        events += parser.close()
        frames = [event for event in events if event[0] == 'frame']
        self.assertEqual(len(frames), 2)
        self.assertTrue(all(frame[2] == jpeg(b'synthetic-frame') for frame in frames))
        self.assertEqual([frame[1]['x-sequence'] for frame in frames], ['1', '2'])
        self.assertTrue(parser.final_seen)
        self.assertEqual([event for event in events if event[0] == 'truncated'], [])

    def test_boundary_lookalike_bytes_stay_in_body(self):
        lookalikes = jpeg(b'frame') + b'\r\n--livecamX-similar\r\nmid --livecam bytes'
        stream = part([(b'Content-Type', b'image/jpeg')], lookalikes) + \
            jpeg_part(2, '2026-11-12T09:00:01Z', jpeg(b'next')) + b'--livecam--\r\n'
        events = feed_in_chunks(MultipartParser(BOUNDARY), stream, 5)
        frames = [event for event in events if event[0] == 'frame']
        self.assertEqual(len(frames), 2)
        self.assertEqual(frames[0][2], lookalikes)
        self.assertEqual(frames[1][2], jpeg(b'next'))

    def test_content_length_is_authoritative_over_delimiter_bytes(self):
        tricky = b'AA\r\n--livecam\r\nBB'
        stream = jpeg_part(1, '2026-11-12T09:00:00Z', tricky) + \
            jpeg_part(2, '2026-11-12T09:00:01Z', jpeg(b'second')) + b'--livecam--\r\n'
        events = feed_in_chunks(MultipartParser(BOUNDARY), stream, 3)
        frames = [event for event in events if event[0] == 'frame']
        self.assertEqual([frame[2] for frame in frames], [tricky, jpeg(b'second')])

    def test_oversized_declared_part_skipped_with_bounded_memory(self):
        huge = b'x' * 1000000
        stream = jpeg_part(1, '2026-11-12T09:00:00Z', huge) + \
            jpeg_part(2, '2026-11-12T09:00:01Z', jpeg(b'after')) + b'--livecam--\r\n'
        parser = MultipartParser(BOUNDARY, max_part_bytes=1024)
        events = []
        for position in range(0, len(stream), 4096):
            events += parser.feed(stream[position:position + 4096])
            self.assertLessEqual(parser.buffer_usage(), 4096)
        dropped = [event for event in events if event[0] == 'dropped']
        self.assertEqual(dropped[0][1], 'part_oversized')
        self.assertEqual(dropped[0][2]['declared_bytes'], len(huge))
        frames = [event for event in events if event[0] == 'frame']
        self.assertEqual([frame[2] for frame in frames], [jpeg(b'after')])

    def test_oversized_undeclared_part_skipped_with_bounded_memory(self):
        huge = b'y' * 100000
        stream = part([(b'Content-Type', b'image/jpeg')], huge) + \
            jpeg_part(2, '2026-11-12T09:00:01Z', jpeg(b'after')) + b'--livecam--\r\n'
        parser = MultipartParser(BOUNDARY, max_part_bytes=1024)
        events = []
        for position in range(0, len(stream), 4096):
            events += parser.feed(stream[position:position + 4096])
            self.assertLessEqual(parser.buffer_usage(), 1024 + 4096 + 64)
        dropped = [event for event in events if event[0] == 'dropped']
        self.assertEqual(dropped[0][1], 'part_oversized')
        self.assertIsNone(dropped[0][2]['declared_bytes'])
        frames = [event for event in events if event[0] == 'frame']
        self.assertEqual([frame[2] for frame in frames], [jpeg(b'after')])

    def test_truncated_part_is_reported_never_emitted(self):
        stream = jpeg_part(1, '2026-11-12T09:00:00Z', jpeg(b'complete')) + \
            jpeg_part(2, '2026-11-12T09:00:01Z', jpeg(b'partial-frame'))[:-9]
        parser = MultipartParser(BOUNDARY)
        events = parser.feed(stream) + parser.close()
        frames = [event for event in events if event[0] == 'frame']
        self.assertEqual([frame[2] for frame in frames], [jpeg(b'complete')])
        truncated = [event for event in events if event[0] == 'truncated']
        self.assertEqual(len(truncated), 1)
        self.assertGreater(truncated[0][1]['buffered_bytes'], 0)

    def test_bad_content_length_is_framing_error(self):
        stream = part([(b'Content-Type', b'image/jpeg'), (b'Content-Length', b'abc')], b'x')
        with self.assertRaises(FramingError):
            MultipartParser(BOUNDARY).feed(stream)

    def test_content_length_shorter_than_body_is_framing_error(self):
        stream = part([(b'Content-Type', b'image/jpeg'), (b'Content-Length', b'2')],
                      b'hello') + b'--livecam--\r\n'
        with self.assertRaises(FramingError):
            MultipartParser(BOUNDARY).feed(stream)

    def test_oversized_headers_dropped_then_resynced(self):
        filler = b'f' * 10000
        stream = part([(b'Content-Type', b'image/jpeg'), (b'X-Filler', filler)],
                      jpeg(b'lost')) + jpeg_part(2, '2026-11-12T09:00:01Z', jpeg(b'kept'))
        parser = MultipartParser(BOUNDARY, max_header_bytes=1024)
        events = feed_in_chunks(parser, stream, 2048)
        dropped = [event for event in events if event[0] == 'dropped']
        self.assertEqual(dropped[0][1], 'headers_too_large')
        frames = [event for event in events if event[0] == 'frame']
        self.assertEqual([frame[2] for frame in frames], [jpeg(b'kept')])

    def test_close_at_part_boundary_is_clean(self):
        stream = jpeg_part(1, '2026-11-12T09:00:00Z', jpeg(b'done'))
        parser = MultipartParser(BOUNDARY)
        events = parser.feed(stream)
        self.assertEqual([event[0] for event in events], ['frame'])
        self.assertEqual(parser.close(), [])

    def test_parse_boundary_variants(self):
        media = 'multipart/x-mixed-replace'
        self.assertEqual(parse_boundary(media + '; boundary="livecam"'), 'livecam')
        self.assertEqual(parse_boundary(media + ';boundary=livecam'), 'livecam')
        self.assertIsNone(parse_boundary(media))
        self.assertIsNone(parse_boundary('image/jpeg'))
        self.assertIsNone(parse_boundary(media + '; boundary=' + 'x' * 71))


class SamplerTests(unittest.TestCase):
    def test_interval_and_reconnect_reasons(self):
        moments = iter([10.0, 10.1, 11.1, 20.0, 20.2])
        sampler = IntervalSampler(1.0, clock=lambda: next(moments))
        sampler.reset()
        self.assertEqual(sampler.decide(), 'first')
        self.assertIsNone(sampler.decide())
        self.assertEqual(sampler.decide(), 'interval')
        sampler.reset()
        self.assertEqual(sampler.decide(), 'reconnect_first')
        self.assertIsNone(sampler.decide())


class FrameStoreTests(unittest.TestCase):
    def test_frames_append_only_under_rollback_and_same_time(self):
        with FrameStore(':memory:') as store:
            first = store.save_frame('cam', 1, 1, '2026-11-12T09:00:01Z',
                                     '2026-11-14T00:00:01Z', 1.0, 'interval', b'a')
            store.save_frame('cam', 1, 2, '2026-11-12T08:30:00Z',
                             '2026-11-14T00:00:02Z', 2.0, 'interval', b'b')
            store.save_frame('cam', 1, 3, '2026-11-12T09:00:01Z',
                             '2026-11-14T00:00:03Z', 3.0, 'interval', b'c')
            frames = store.list_frames()
            self.assertEqual([frame['id'] for frame in frames], [first, first + 1, first + 2])
            self.assertEqual([frame['source_time'] for frame in frames],
                             ['2026-11-12T09:00:01Z', '2026-11-12T08:30:00Z',
                              '2026-11-12T09:00:01Z'])
            self.assertEqual(store.frame_raw(first + 1), b'b')
            store.save_gap('cam', 1, 'disconnect', {'error': 'eof'})
            self.assertEqual(store.list_gaps()[0]['kind'], 'disconnect')


class SessionTests(unittest.TestCase):
    def run_session(self, server, path, **options):
        defaults = dict(sample_interval=0.0, max_part_bytes=4096, spool_capacity=16,
                        read_timeout=0.05, idle_timeout=0.4, max_reconnects=3,
                        retry_pause=0.02)
        defaults.update(options)
        self.store = FrameStore(Path(self.directory) / 'frames.sqlite3')
        self.session = IntakeSession(server.url + path, 'cam-test', self.store, **defaults)
        return self.session.run()

    def setUp(self):
        self.directory_obj = tempfile.TemporaryDirectory()
        self.directory = self.directory_obj.name

    def tearDown(self):
        self.directory_obj.cleanup()

    def test_end_to_end_live_routes(self):
        stream = live_stream()
        with ReplaySource(live_routes()) as server:
            end = self.run_session(server, '/live')
            self.assertEqual(end, 'stream_final')
            stats = self.session.stats()
            self.assertEqual(stats['connections'], 2)
            self.assertEqual(stats['frames_parsed'], 6)
            self.assertEqual(stats['frames_retained'], 6)
            self.assertEqual(stats['heartbeats'], 2)
            self.assertEqual(stats['parts_dropped_oversized'], 1)
            self.assertEqual(stats['frames_truncated'], 1)
            self.assertEqual(stats['disconnects'], 1)
            self.assertFalse(os.path.exists(stats['spool_dir']))
            frames = self.store.list_frames()
            got = {(frame['connection'], frame['sequence']): frame for frame in frames}
            self.assertEqual(set(got), {(1, 1), (1, 2), (1, 4), (1, 5), (2, 1), (2, 2)})
            for want in stream['expected_frames']:
                frame = got[(want['connection'], want['sequence'])]
                self.assertEqual(frame['source_time'], want['source_time'])
                raw = self.store.frame_raw(frame['id'])
                self.assertEqual(raw, want['body'])
                self.assertEqual(frame['digest'], hashlib.sha256(want['body']).hexdigest())
            rollback = got[(1, 4)]
            same_time = got[(1, 5)]
            self.assertEqual(rollback['source_time'], '2026-11-12T08:30:00Z')
            self.assertEqual(same_time['source_time'], got[(1, 2)]['source_time'])
            self.assertNotEqual(rollback['id'], same_time['id'])
            kinds = [gap['kind'] for gap in self.store.list_gaps()]
            for expected in ('connect', 'disconnect', 'frame_truncated', 'part_dropped',
                             'connection_summary', 'session_end'):
                self.assertIn(expected, kinds)
            drop = next(gap for gap in self.store.list_gaps()
                        if gap['kind'] == 'part_dropped')
            self.assertEqual(drop['detail']['declared_bytes'], stream['oversized_declared'])
            self.store.close()

    def test_slow_disk_drops_are_counted_not_blocking_network(self):
        with ReplaySource(live_routes()) as server:
            end = self.run_session(server, '/live', spool_capacity=1, write_delay=0.3)
            self.assertEqual(end, 'stream_final')
            stats = self.session.stats()
            self.assertGreaterEqual(stats['frames_dropped_spool_full'], 1)
            self.assertEqual(stats['frames_retained'] + stats['frames_dropped_spool_full'],
                             stats['frames_parsed'])
            self.assertEqual(stats['connections'], 2)
            drops = [gap for gap in self.store.list_gaps() if gap['kind'] == 'spool_drop']
            self.assertTrue(drops)
            self.assertGreaterEqual(sum(gap['detail']['count'] for gap in drops), 1)
            self.assertFalse(os.path.exists(stats['spool_dir']))
            self.store.close()

    def test_source_silence_is_not_reported_as_disconnect(self):
        with ReplaySource(live_routes()) as server:
            end = self.run_session(server, '/trickle', idle_timeout=0.3, max_reconnects=1)
            self.assertEqual(end, 'reconnects_exhausted')
            stats = self.session.stats()
            self.assertEqual(stats['source_silent'], 2)
            self.assertEqual(stats.get('disconnects', 0), 0)
            self.assertEqual(stats['frames_retained'], 2)
            kinds = [gap['kind'] for gap in self.store.list_gaps()]
            self.assertEqual(kinds.count('source_silent'), 2)
            self.assertNotIn('disconnect', kinds)
            self.store.close()

    def test_manual_stop_unblocks_read_and_cleans_spool(self):
        with ReplaySource(live_routes()) as server:
            self.store = FrameStore(Path(self.directory) / 'frames.sqlite3')
            session = IntakeSession(server.url + '/trickle', 'cam-test', self.store,
                                    sample_interval=0.0, max_part_bytes=4096,
                                    spool_capacity=4, read_timeout=0.05,
                                    idle_timeout=30.0, max_reconnects=0)
            outcome = []
            worker = threading.Thread(target=lambda: outcome.append(session.run()))
            worker.start()
            time.sleep(0.4)
            session.stop()
            worker.join(timeout=5)
            self.assertFalse(worker.is_alive())
            self.assertEqual(outcome, ['stop_requested'])
            stats = session.stats()
            self.assertFalse(os.path.exists(stats['spool_dir']))
            self.assertEqual(stats['frames_retained'], 1)
            self.assertEqual(stats['frames_truncated'], 1)
            kinds = [gap['kind'] for gap in self.store.list_gaps()]
            self.assertNotIn('disconnect', kinds)
            self.store.close()

    def test_cli_stream_then_frames_and_gaps(self):
        database = str(Path(self.directory) / 'cli.sqlite3')
        with ReplaySource(live_routes()) as server:
            with contextlib.redirect_stdout(io.StringIO()) as output:
                code = app.main(['stream', server.url + '/live', '--database', database,
                                 '--source', 'cli-cam', '--interval', '0',
                                 '--max-part-bytes', '4096', '--reconnects', '3'])
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(output.getvalue())['end_reason'], 'stream_final')
            with contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(app.main(['frames', '--database', database]), 0)
            frames = json.loads(output.getvalue())
            self.assertEqual(len(frames), 6)
            self.assertNotIn('raw', frames[0])
            with contextlib.redirect_stdout(io.StringIO()) as output:
                self.assertEqual(app.main(['gaps', '--database', database]), 0)
            kinds = {gap['kind'] for gap in json.loads(output.getvalue())}
            self.assertIn('frame_truncated', kinds)
            self.assertIn('part_dropped', kinds)

    def test_cli_stream_demo(self):
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(app.main(['stream-demo']), 0)
        report = json.loads(output.getvalue())
        self.assertEqual(set(report['phases']),
                         {'stream_and_slow_disk', 'interval_sampling',
                          'source_silence', 'manual_stop'})
        main = report['phases']['stream_and_slow_disk']
        self.assertGreaterEqual(main['frames_dropped_spool_full'], 1)
        self.assertEqual(main['parts_dropped_oversized'], 1)
        silence = report['phases']['source_silence']
        self.assertGreaterEqual(silence['source_silent'], 1)
        self.assertTrue(report['phases']['manual_stop']['stopped_promptly'])
        for phase in report['phases'].values():
            self.assertFalse(phase['spool_dir_exists'])
        kinds = {gap['kind'] for gap in report['gaps']}
        self.assertGreaterEqual(kinds, {'part_dropped', 'frame_truncated', 'spool_drop',
                                        'source_silent', 'disconnect'})
        reasons = {frame['reason'] for frame in report['frames']}
        self.assertGreaterEqual(reasons, {'first', 'interval', 'reconnect_first'})
        self.assertGreaterEqual(len(report['frames']), 5)


if __name__ == '__main__':
    unittest.main()
