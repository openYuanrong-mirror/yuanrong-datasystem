#!/usr/bin/env python3

import csv
import json
import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from parse_resource import load_resource_csv, render_html


class TestParseResource(unittest.TestCase):
    def test_cli_initial_time_range_keeps_samples_for_reset(self):
        with tempfile.TemporaryDirectory() as directory:
            source = os.path.join(directory, 'resource.csv')
            output = os.path.join(directory, 'report.html')
            with open(source, 'w') as stream:
                stream.write('timestamp,cpu_pct\n'
                             '1970-01-01T00:00:01+00:00,10\n'
                             '1970-01-01T00:00:02+00:00,20\n'
                             '1970-01-01T00:00:03+00:00,30\n')
            script = os.path.join(os.path.dirname(__file__), '..', '..',
                                  'parse_resource.py')
            result = subprocess.run(
                [sys.executable, script, source, '-o', output,
                 '--start-time', '1970-01-01T08:00:01.500+08:00',
                 '--end-time', '1970-01-01T08:00:02.500+08:00'],
                capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            with open(output) as stream:
                payload = stream.read().split('const DATA=', 1)[1]
            data, _ = json.JSONDecoder().raw_decode(payload)
            self.assertEqual(data['range'], [1500, 2500])
            self.assertEqual([r['cpu_pct'] for r in data['rows']],
                             [10, 20, 30])

    def test_invalid_cli_range_does_not_overwrite_report(self):
        with tempfile.TemporaryDirectory() as directory:
            source = os.path.join(directory, 'resource.csv')
            output = os.path.join(directory, 'report.html')
            with open(source, 'w') as stream:
                stream.write('timestamp,cpu_pct\n'
                             '1970-01-01T00:00:01+00:00,10\n'
                             '1970-01-01T00:00:03+00:00,30\n')
            with open(output, 'w') as stream:
                stream.write('previous report')
            script = os.path.join(os.path.dirname(__file__), '..', '..',
                                  'parse_resource.py')
            cases = [
                ('1970-01-01T00:00:03+00:00',
                 '1970-01-01T00:00:01+00:00', 'before'),
                ('1970-01-01T00:00:01.500+00:00',
                 '1970-01-01T00:00:02.500+00:00', 'no samples'),
                ('invalid', '1970-01-01T00:00:03+00:00', 'invalid timestamp'),
            ]
            for start, end, message in cases:
                with self.subTest(start=start, end=end):
                    result = subprocess.run(
                        [sys.executable, script, source, '-o', output,
                         '--start-time', start, '--end-time', end],
                        capture_output=True, text=True)
                    self.assertEqual(result.returncode, 2)
                    self.assertIn(message, result.stderr)
                    with open(output) as stream:
                        self.assertEqual(stream.read(), 'previous report')

    def test_csv_and_exact_point_payload(self):
        with tempfile.NamedTemporaryFile(mode='w', newline='', delete=False) as f:
            writer = csv.DictWriter(f, fieldnames=[
                'timestamp', 'pid', 'cpu_pct', 'rss_mb',
                'jemalloc_allocated_mb'])
            writer.writeheader()
            writer.writerow({
                'timestamp': '2026-08-31T12:00:00', 'pid': '7',
                'cpu_pct': '12.5', 'rss_mb': '100.25',
                'jemalloc_allocated_mb': '80.125'})
            path = f.name
        try:
            data = load_resource_csv(path)
            self.assertEqual(data['rows'][0]['rss_mb'], 100.25)
            html = render_html(data, 'sample.csv')
            self.assertIn('2026-08-31T12:00:00', html)
            self.assertIn('100.25', html)
            self.assertIn('jemalloc_allocated_mb', html)
            self.assertIn('data-point-index', html)
        finally:
            os.unlink(path)

    def test_empty_metric_is_omitted(self):
        with tempfile.NamedTemporaryFile(mode='w', newline='', delete=False) as f:
            f.write('timestamp,cpu_pct,rss_mb,jemalloc_allocated_mb\n')
            f.write('2026-08-31T12:00:00,1.0,10.0,\n')
            path = f.name
        try:
            data = load_resource_csv(path)
            self.assertNotIn('jemalloc_allocated_mb', data['metrics'])
        finally:
            os.unlink(path)


    def test_runtime_charts_preserve_units_zero_values_and_missing_samples(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'resource.csv')
            with open(path, 'w', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=[
                    'timestamp', 'bthread_count', 'bthread_keytable_count',
                    'bthread_keytable_memory', 'bthread_worker_count',
                    'bthread_worker_usage', 'brpc_active_requests',
                    'bthread_local_runqueue_count', 'brpc_stats_available',
                    'brpc_stats_read_failures', 'brpc_method_concurrency',
                    'bthread_group_status'])
                writer.writeheader()
                writer.writerow({
                    'timestamp': '2026-09-01T00:00:00+00:00',
                    'bthread_count': 20, 'bthread_keytable_count': 80,
                    'bthread_keytable_memory': 4096, 'bthread_worker_count': 8,
                    'bthread_worker_usage': 1.25, 'brpc_active_requests': 0,
                    'bthread_local_runqueue_count': 0, 'brpc_stats_available': 1,
                    'brpc_stats_read_failures': 0,
                    'brpc_method_concurrency': '{"example_get":0}',
                    'bthread_group_status': '0 0 '})
                writer.writerow({
                    'timestamp': '2026-09-01T00:00:01+00:00',
                    'brpc_stats_available': 0, 'brpc_stats_read_failures': 1})
            data = load_resource_csv(path)
            page = render_html(data)
        payload, _ = json.JSONDecoder().raw_decode(page.split('const DATA=', 1)[1])
        self.assertEqual(len(payload['charts']), 5)
        plotted = {metric['key']: metric for chart in payload['charts']
                   for metric in chart['metrics']}
        self.assertEqual(plotted['bthread_keytable_memory']['unit'], ' bytes')
        self.assertEqual(plotted['bthread_worker_usage']['unit'], ' workers')
        self.assertEqual(payload['rows'][0]['brpc_active_requests'], 0)
        self.assertNotIn('brpc_active_requests', payload['rows'][1])
        self.assertEqual(payload['rows'][1]['brpc_stats_available'], 0)
        self.assertNotIn('brpc_method_concurrency', plotted)
        self.assertNotIn('bthread_group_status', plotted)
        self.assertNotIn('bthread_concurrency', plotted)

    def test_old_csv_does_not_gain_empty_runtime_charts(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'resource.csv')
            with open(path, 'w') as stream:
                stream.write('timestamp,rss_mb,bthread_count\n'
                             '2026-09-01T00:00:00+00:00,100,\n')
            page = render_html(load_resource_csv(path))
        payload, _ = json.JSONDecoder().raw_decode(page.split('const DATA=', 1)[1])
        self.assertEqual([chart['title'] for chart in payload['charts']],
                         ['Process memory'])


if __name__ == '__main__':
    unittest.main()
