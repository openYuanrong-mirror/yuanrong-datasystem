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


if __name__ == '__main__':
    unittest.main()
