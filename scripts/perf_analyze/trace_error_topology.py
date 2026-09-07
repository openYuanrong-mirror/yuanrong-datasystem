#!/usr/bin/env python3
"""Build an interactive client/worker error topology.

Examples:
  python3 trace_error_topology.py trace_collect/1001
  python3 trace_error_topology.py /path/to/collection -t all --max-traces-per-code 2000 -j 8

Default ``sample`` mode reads trace_collector output files.  ``-t all`` scans
``collected/`` and ``collected_worker_logs/`` access logs directly, including
gzip rotations, and limits each client/error-code group before drawing.
"""

import argparse
from collections import defaultdict
import gzip
import html
import json
import os
from pathlib import Path
import random
import re
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Iterable

LINE_RE = re.compile(r'^(?P<prefix>.*?:\d+:)?(?P<body>\d{4}-\d\d-\d\dT[^ ]+\s*\|.*)$')
IP_RE = re.compile(r'\b(?:worker|target worker)\s*:\s*(\d{1,3}(?:\.\d{1,3}){3})(?::\d+)?', re.I)
UB_READ_SOURCE_RE = re.compile(
    r'\bClient\s+UB\s+read\s+source\s+denied\s*:\s*(\d{1,3}(?:\.\d{1,3}){3})(?::\d+)?', re.I)
CODE_RE = re.compile(r'code\s*:\s*\[([^\]]+)\]', re.I)
TRACE_RE = re.compile(r'traceId\s*[:=]\s*([^,\s|]+)', re.I)
DEFAULT_JOBS = 8
_TARGET_TRACES = set()


def _log_paths(root: Path):
    """Yield collector result files, including extensionless trace-named files."""
    if root.is_file():
        yield root
        return
    for directory, _, filenames in os.walk(root, onerror=lambda _: None):
        for filename in sorted(filenames):
            path = Path(directory, filename)
            if path.is_file():
                yield path


def _read_path(path: Path) -> Iterable[str]:
    opener = gzip.open if path.suffix == '.gz' else open
    try:
        with opener(path, 'rt', encoding='utf-8', errors='replace') as stream:
            yield from stream
    except (OSError, UnicodeError, PermissionError):
        return


def _read_lines(root: Path) -> Iterable[str]:
    for path in _log_paths(root):
        yield from _read_path(path)


def _parse_line(line: str):
    match = LINE_RE.match(line.rstrip('\n'))
    if not match:
        return None
    body = match.group('body')
    parts = [part.strip() for part in body.split('|')]
    if len(parts) < 6:
        return None
    message = '|'.join(parts[7:]) if len(parts) > 7 else ''
    trace = parts[5] or (TRACE_RE.search(message).group(1) if TRACE_RE.search(message) else '')
    if not trace or trace in {'-', 'null', 'None'}:
        return None
    source_ip = parts[3] if re.fullmatch(r'\d{1,3}(?:\.\d{1,3}){3}', parts[3]) else ''
    operation = 'SET' if re.search(r'(?:SET|Put|TransportSet)', message, re.I) else 'GET'
    code_match = CODE_RE.search(message)
    code = code_match.group(1).strip() if code_match else ''
    failed = parts[1].upper().startswith('E') or bool(code_match) and re.search(r'failed|error|cannot|can not|not found|timeout', message, re.I)
    targets = [m.group(1) for m in IP_RE.finditer(message)]
    targets.extend(m.group(1) for m in UB_READ_SOURCE_RE.finditer(message))
    return {'trace': trace, 'timestamp': parts[0], 'source_ip': source_ip,
            'operation': operation, 'code': code or 'unknown', 'failed': bool(failed),
            'targets': targets, 'raw': line.rstrip('\n')}


def _parse_access(line: str):
    parsed = _parse_line(line)
    if not parsed:
        return None
    match = LINE_RE.match(line.rstrip('\n'))
    parts = [part.strip() for part in match.group('body').split('|')]
    if len(parts) < 11:
        return None
    try:
        status = int(parts[7])
    except ValueError:
        return None
    if status == 0 or not parts[8].startswith('DS_KV_CLIENT_'):
        return None
    parsed.update({'code': str(status), 'operation': 'SET' if 'SET' in parts[8] else 'GET',
                   'failed': True, 'targets': [], 'raw': line.rstrip('\n')})
    return parsed


def _parse_worker_access(line: str):
    """Return the worker IP only for a successful worker access-log record."""
    parsed = _parse_line(line)
    if not parsed:
        return None
    match = LINE_RE.match(line.rstrip('\n'))
    parts = [part.strip() for part in match.group('body').split('|')]
    if len(parts) < 9 or parts[7] != '0' or not parts[8].startswith('DS_POSIX_'):
        return None
    return parsed if parsed['source_ip'] else None


def _access_log_paths(root: Path, client: bool):
    """Find only client or worker access logs, including compressed rotations."""
    for directory, _, filenames in os.walk(root, onerror=lambda _: None):
        for filename in sorted(filenames):
            is_log = filename.endswith('.log') or filename.endswith('.log.gz')
            if not is_log:
                continue
            if client and filename.startswith('ds_client_access_'):
                yield Path(directory, filename)
            elif not client and filename.startswith('access'):
                yield Path(directory, filename)


def _scan_client_access(path: str):
    records = []
    for line in _read_path(Path(path)):
        access = _parse_access(line)
        if access:
            records.append(access)
    return records


def _init_worker_scan(trace_ids):
    global _TARGET_TRACES
    _TARGET_TRACES = set(trace_ids)


def _scan_worker_access(path: str):
    records = []
    for line in _read_path(Path(path)):
        access = _parse_worker_access(line)
        if access and access['trace'] in _TARGET_TRACES:
            records.append((access['trace'], access['source_ip']))
    return records


def _collect_parallel(paths, scan, jobs, label, initializer=None, initargs=()):
    """Scan access files concurrently and emit deterministic main-process progress."""
    total = len(paths)
    if not total:
        print(f'  No {label} access logs found')
        return []
    print(f'  Scanning {total} {label} access logs with {min(jobs, total)} processes...')
    started = time.monotonic()
    records = []
    with ProcessPoolExecutor(max_workers=jobs, initializer=initializer, initargs=initargs) as executor:
        futures = {executor.submit(scan, str(path)): path for path in paths}
        for completed, future in enumerate(as_completed(futures), 1):
            path = futures[future]
            records.extend(future.result())
            print(f'  {label} progress: {completed}/{total} ({completed / total:.1%}) | '
                  f'{path.name} | elapsed {time.monotonic() - started:.1f}s', flush=True)
    return records


def build_all_access_report(root: Path, jobs: int, max_traces_per_code: int):
    """Build a full topology by scanning client and worker access logs exactly once."""
    client_paths = list(_access_log_paths(root / 'collected', client=True))
    worker_paths = list(_access_log_paths(root / 'collected_worker_logs', client=False))
    client_records = _collect_parallel(client_paths, _scan_client_access, jobs, 'client')
    all_traces = {}
    for access in client_records:
        all_traces.setdefault(access['trace'], {'access': access, 'targets': set(), 'worker_ips': set()})
    grouped = defaultdict(list)
    for trace, state in all_traces.items():
        access = state['access']
        grouped[(access['source_ip'] or 'unknown', access['code'])].append(trace)
    traces = {}
    sampled = 0
    for key, trace_ids in grouped.items():
        selected = random.sample(trace_ids, max_traces_per_code) if len(trace_ids) > max_traces_per_code else trace_ids
        sampled += len(selected)
        traces.update((trace, all_traces[trace]) for trace in selected)
        if len(trace_ids) > len(selected):
            print(f'  Limit {key[0]} / code {key[1]}: {len(trace_ids)} -> {len(selected)} traces')
    print(f'  Found {len(all_traces)} non-zero client error traces; retained {sampled} after per-code limits')
    worker_records = _collect_parallel(worker_paths, _scan_worker_access, jobs, 'worker',
                                       _init_worker_scan, (tuple(traces),))
    for trace, worker_ip in worker_records:
        traces[trace]['worker_ips'].add(worker_ip)
    return _report_from_traces(traces, str(root), trace_ids_only=True)


def build_report(lines: Iterable[str], source=''):
    """Compatibility parser for collector-style inputs containing access lines."""
    traces = {}
    for line in lines:
        access = _parse_access(line)
        if access:
            traces.setdefault(access['trace'], {'access': access, 'targets': set(), 'worker_ips': set()})

    for line in lines if isinstance(lines, list) else []:
        event = _parse_line(line)
        if event and event['trace'] in traces:
            traces[event['trace']]['targets'].update(event['targets'])
        worker_access = _parse_worker_access(line)
        if worker_access and worker_access['trace'] in traces:
            traces[worker_access['trace']]['worker_ips'].add(worker_access['source_ip'])

    return _report_from_traces(traces, source)


def _report_from_traces(traces, source, trace_ids_only=False):

    edges = {}
    for trace, state in traces.items():
        access = state['access']
        targets = state['worker_ips'] or state['targets'] or {'unknown'}
        for worker in targets:
            worker = worker.split(':', 1)[0]
            evidence = 'worker_observed' if worker in state['worker_ips'] else 'worker_missing'
            key = (access['source_ip'] or 'unknown', worker, access['code'], evidence)
            edge = edges.setdefault(key, {'id': len(edges), 'client': access['source_ip'] or 'unknown', 'worker': worker,
                'code': access['code'], 'evidence': evidence, 'count': 0, 'operations': set(), 'traces': []})
            edge['count'] += 1
            edge['operations'].add(access['operation'])
            edge['operation'] = ', '.join(sorted(edge['operations']))
            if trace_ids_only:
                edge['traces'].append({'trace_id': trace, 'operation': '', 'timestamp': '', 'message': ''})
            else:
                edge['traces'].append({'trace_id': trace, 'operation': access['operation'],
                    'timestamp': access['timestamp'], 'message': access['raw']})
    output_edges = []
    for edge in edges.values():
        edge['operations'] = sorted(edge['operations'])
        output_edges.append(edge)
    worker_totals = defaultdict(int)
    for edge in output_edges:
        worker_totals[edge['worker']] += edge['count']
    output_edges.sort(key=lambda edge: (-worker_totals[edge['worker']], edge['worker'], edge['id']))
    ranked_workers = sorted((worker for worker in worker_totals if worker != 'unknown'),
                            key=lambda worker: (-worker_totals[worker], worker))
    worker_ranks = {worker: rank for rank, worker in enumerate(ranked_workers, 1)}
    return {'source': source, 'edges': output_edges,
            'summary': {'trace_count': len(traces), 'error_traces': len(traces),
                        'edge_count': len(output_edges), 'client_count': len({e['client'] for e in output_edges}),
                        'worker_count': len({e['worker'] for e in output_edges}),
                        'worker_totals': dict(sorted(worker_totals.items(), key=lambda item: (-item[1], item[0]))),
                        'worker_ranks': worker_ranks} }


def build_report_from_lines(lines):
    return build_report(list(lines))


def render_html(report, source=''):
    payload = json.dumps(report, ensure_ascii=False).replace('</', '<\\/')
    title = html.escape(f'Trace Error Topology - {source}')
    template = '''<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>__TITLE__</title><style>
*{box-sizing:border-box}body{margin:0;background:#0f172a;color:#e2e8f0;font:14px system-ui,sans-serif;overflow:hidden}.bar{height:58px;padding:12px 20px;background:#111c35;border-bottom:1px solid #263554;display:flex;align-items:center;gap:12px}.bar h1{font-size:18px;margin:0 10px 0 0;color:#f8fafc}.stats{color:#94a3b8}button{background:#2563eb;border:0;color:#fff;padding:8px 12px;border-radius:6px;cursor:pointer}input{width:210px;background:#0b1224;color:#e2e8f0;border:1px solid #475569;border-radius:6px;padding:8px 10px}#viewport{position:absolute;inset:58px 360px 0 0;overflow:hidden;background:#0b1224;cursor:grab}#viewport:active{cursor:grabbing}canvas{display:block;width:100%;height:100%}#drawer{position:absolute;right:0;top:58px;bottom:0;width:360px;background:#111827;border-left:1px solid #263554;padding:20px;overflow:auto;box-shadow:-12px 0 30px #02061788}#drawer h2{margin:0 0 12px;font-size:17px}.hint{color:#94a3b8;line-height:1.6}.trace,.code{border-top:1px solid #263554;padding:10px 0}.trace b,.code button{color:#93c5fd}.trace pre{white-space:pre-wrap;color:#cbd5e1;font-size:11px;margin:6px 0}.code button{background:none;padding:0;border:0;text-align:left;font-size:14px}.code span{color:#cbd5e1}
</style></head><body><header class="bar"><h1>Trace 错误链路拓扑</h1><span class="stats" id="stats"></span><input id="search" placeholder="搜索 client / worker IP"><button id="fit">适配全图</button><button id="reset">重置视图</button><button id="export">导出 PNG</button></header><main id="viewport"><canvas id="topology"></canvas></main><aside id="drawer"><h2>链路详情</h2><p class="hint">点击 Worker 查看各错误码汇总；点击错误码或线查看 trace。拖拽空白处平移，滚轮以鼠标位置缩放。</p></aside><script>
const DATA=__PAYLOAD__,canvas=document.getElementById('topology'),ctx=canvas.getContext('2d'),viewport=document.getElementById('viewport'),drawer=document.getElementById('drawer');const clients=[...new Set(DATA.edges.map(e=>e.client))].sort(),workers=[...new Set(DATA.edges.map(e=>e.worker))].sort((a,b)=>sum(b)-sum(a));const W=Math.max(1600,clients.length*180+workers.length*220+500),H=Math.max(900,Math.max(clients.length,workers.length)*96+180),pos={},hits=[];let view={x:0,y:0,w:W,h:H},drag=null,frame=0,query='';function sum(worker){return DATA.edges.filter(e=>e.worker===worker).reduce((n,e)=>n+e.count,0)}document.getElementById('stats').textContent=`trace ${DATA.summary.error_traces} · client ${clients.length} · worker ${workers.length} · edge ${DATA.edges.length}`;clients.forEach((ip,i)=>pos['c'+ip]={x:150,y:90+i*96,kind:'client',ip});workers.forEach((ip,i)=>pos['w'+ip]={x:W-210,y:90+i*96,kind:'worker',ip});const colors=['#60a5fa','#a78bfa','#34d399','#fbbf24','#fb7185','#f472b6','#c084fc'];function resize(){const r=viewport.getBoundingClientRect(),d=devicePixelRatio||1;canvas.width=r.width*d;canvas.height=r.height*d;ctx.setTransform(d,0,0,d,0,0);draw()}function screen(p){const r=canvas.getBoundingClientRect();return{x:(p.x-view.x)*r.width/view.w,y:(p.y-view.y)*r.height/view.h}}function world(e){const r=canvas.getBoundingClientRect();return{x:view.x+(e.clientX-r.left)*view.w/r.width,y:view.y+(e.clientY-r.top)*view.h/r.height}}function round(ctx,x,y,w,h,r){ctx.beginPath();ctx.roundRect(x,y,w,h,r);ctx.fill();ctx.stroke()}function draw(){frame=0;const r=canvas.getBoundingClientRect();ctx.clearRect(0,0,r.width,r.height);hits.length=0;const zoom=r.width/view.w;ctx.lineCap='round';DATA.edges.forEach((e,i)=>{const a=screen(pos['c'+e.client]),b=screen(pos['w'+e.worker]||{x:W-210,y:H/2}),off=(i%7-3)*8*zoom,mid=(a.x+b.x)/2,active=!query||e.client.includes(query)||e.worker.includes(query);ctx.globalAlpha=active?0.84:0.12;ctx.strokeStyle=e.evidence==='worker_missing'?'#ef4444':colors[i%colors.length];ctx.lineWidth=active?2.2:1;ctx.setLineDash(e.evidence==='worker_missing'?[8,5]:[]);ctx.beginPath();ctx.moveTo(a.x+99*zoom,a.y+off);ctx.bezierCurveTo(mid-180*zoom,a.y+off,mid+180*zoom,b.y+off,b.x-99*zoom,b.y+off);ctx.stroke();ctx.setLineDash([]);if(zoom>.26){const label=`${e.code} × ${e.count}`;ctx.font=`${Math.max(10,11*zoom)}px system-ui`;const tw=ctx.measureText(label).width+10;ctx.fillStyle='#0b1224';ctx.globalAlpha=active?.92:.15;ctx.fillRect(mid-tw/2,(a.y+b.y)/2-13,tw,18);ctx.fillStyle='#f8fafc';ctx.globalAlpha=active?1:.2;ctx.fillText(label,mid-tw/2+5,(a.y+b.y)/2);hits.push({type:'edge',i,x:mid-tw/2,y:(a.y+b.y)/2-15,w:tw,h:22})}});Object.values(pos).forEach(n=>{const p=screen(n),active=!query||n.ip.includes(query),w=196*zoom,h=54*zoom;ctx.globalAlpha=active?1:.28;ctx.fillStyle=n.kind==='worker'?'#164e63':'#172554';ctx.strokeStyle=n.kind==='worker'?'#22d3ee':'#60a5fa';ctx.lineWidth=active?2:1;round(ctx,p.x-w/2,p.y-h/2,w,h,9*zoom);ctx.textAlign='center';ctx.fillStyle='#93c5fd';ctx.font=`700 ${Math.max(8,10*zoom)}px system-ui`;ctx.fillText(n.kind.toUpperCase(),p.x,p.y-6*zoom);ctx.fillStyle='#f8fafc';ctx.font=`600 ${Math.max(10,15*zoom)}px system-ui`;ctx.fillText(n.ip,p.x,p.y+14*zoom);hits.push({type:n.kind,ip:n.ip,x:p.x-w/2,y:p.y-h/2,w,h})});ctx.globalAlpha=1;ctx.textAlign='start'}function schedule(){if(!frame)frame=requestAnimationFrame(draw)}function fit(){view={x:-90,y:-90,w:W+180,h:H+180};schedule()}function showEdge(edge){drawer.innerHTML=`<h2>${edge.client} → ${edge.worker}</h2><p><b>错误码：</b>${edge.code} × ${edge.count}</p><p><b>证据：</b>${edge.evidence==='worker_missing'?'worker 未观察到，红色虚线':'worker 有日志'}</p><p><b>操作：</b>${edge.operations.join(', ')}</p>${edge.traces.map(t=>`<div class="trace"><b>${t.trace_id}</b> · ${t.operation} · ${t.timestamp}<pre>${escapeHtml(t.message)}</pre></div>`).join('')}`}function showWorker(worker){const groups={};DATA.edges.filter(e=>e.worker===worker).forEach(e=>{const k=e.code+'|'+e.evidence;(groups[k]??={code:e.code,evidence:e.evidence,count:0,edges:[]}).count+=e.count;groups[k].edges.push(e)});drawer.innerHTML=`<h2>WORKER ${worker}</h2><p class="hint">总错误 trace：${sum(worker)}；点击错误码查看关联 trace。</p>${Object.values(groups).sort((a,b)=>b.count-a.count).map((g,i)=>`<div class="code"><button data-code="${i}">${g.evidence==='worker_missing'?'红色虚线 · ':''}${g.code} × ${g.count}</button></div>`).join('')}`;drawer.querySelectorAll('[data-code]').forEach(btn=>btn.onclick=()=>showGroup(Object.values(groups)[Number(btn.dataset.code)]))}function showGroup(g){const traces=g.edges.flatMap(e=>e.traces);drawer.innerHTML=`<h2>${g.code} × ${g.count}</h2><p><b>证据：</b>${g.evidence==='worker_missing'?'worker 未观察到':'worker 有日志'}</p>${traces.map(t=>`<div class="trace"><b>${t.trace_id}</b> · ${t.operation} · ${t.timestamp}<pre>${escapeHtml(t.message)}</pre></div>`).join('')}`}function escapeHtml(v){return v.replaceAll('&','&amp;').replaceAll('<','&lt;')}viewport.addEventListener('wheel',e=>{e.preventDefault();const p=world(e),factor=e.deltaY<0?.82:1.22,nw=Math.max(260,Math.min(W*2,view.w*factor)),nh=nw*view.h/view.w;view.x=p.x-(p.x-view.x)*nw/view.w;view.y=p.y-(p.y-view.y)*nh/view.h;view.w=nw;view.h=nh;schedule()},{passive:false});viewport.addEventListener('mousedown',e=>{const h=hits.findLast(h=>e.offsetX>=h.x&&e.offsetX<=h.x+h.w&&e.offsetY>=h.y&&e.offsetY<=h.y+h.h);if(h){if(h.type==='worker')showWorker(h.ip);else if(h.type==='edge')showEdge(DATA.edges[h.i]);return}drag={x:e.clientX,y:e.clientY,vx:view.x,vy:view.y}});window.addEventListener('mouseup',()=>drag=null);window.addEventListener('mousemove',e=>{if(!drag)return;const r=canvas.getBoundingClientRect();view.x=drag.vx-(e.clientX-drag.x)*view.w/r.width;view.y=drag.vy-(e.clientY-drag.y)*view.h/r.height;schedule()});document.getElementById('fit').onclick=fit;document.getElementById('reset').onclick=()=>{view={x:0,y:0,w:W,h:H};schedule()};document.getElementById('search').oninput=e=>{query=e.target.value.trim();schedule()};document.getElementById('export').onclick=()=>{const a=document.createElement('a');a.href=canvas.toDataURL('image/png');a.download='trace-error-topology.png';a.click()};new ResizeObserver(resize).observe(viewport);fit();
</script></body></html>'''
    # Edge labels remain visible but are intentionally non-interactive; worker/code drawers own trace navigation.
    return template.replace('__TITLE__', title).replace('__PAYLOAD__', payload).replace(
        "sort((a,b)=>sum(b)-sum(a))",
        "sort((a,b)=>sum(b)-sum(a)||a.localeCompare(b))").replace(
        "ctx.fillText(n.ip,p.x,p.y+14*zoom);",
        "ctx.fillText(n.kind==='worker'&&n.ip!=='unknown'?n.ip+' #'+DATA.summary.worker_ranks[n.ip]:n.ip,p.x,p.y+14*zoom);")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''Input layout for -t all:
  <input_dir>/collected/**/ds_client_access_*.log[.gz]
  <input_dir>/collected_worker_logs/**/access*.log[.gz]

All mode: scans client access logs for non-zero DS_KV_CLIENT_GET/SET records,
then randomly retains up to --max-traces-per-code trace IDs per client and
error code before scanning worker DS_POSIX_* access records.  If no worker
access record exists for a retained trace, the worker is shown as unknown.

Sample mode: parses a trace_collector output directory and preserves its
existing detailed trace drawer information.''')
    parser.add_argument('input_dir', type=Path)
    parser.add_argument('-t', '--type', choices=('sample', 'all'), default='sample',
                        help='sample: parse trace_collector output; all: scan collected access logs (default: sample)')
    parser.add_argument('-j', '--jobs', type=int, default=DEFAULT_JOBS,
                        help=f'access-log scan processes for --type all (default: {DEFAULT_JOBS})')
    parser.add_argument('--max-traces-per-code', type=int, default=2000,
                        help='maximum traces retained per client and error code in --type all (default: 2000)')
    parser.add_argument('-o', '--output', type=Path, default=Path('trace_error_topology.html'))
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error('--jobs must be positive')
    if args.max_traces_per_code < 1:
        parser.error('--max-traces-per-code must be positive')
    if args.type == 'all':
        report = build_all_access_report(args.input_dir, args.jobs, args.max_traces_per_code)
    else:
        report = build_report(list(_read_lines(args.input_dir)), str(args.input_dir))
    args.output.write_text(render_html(report, str(args.input_dir)), encoding='utf-8')
    print(f'Wrote {args.output} ({report["summary"]["edge_count"]} edges, {report["summary"]["error_traces"]} error traces)')


if __name__ == '__main__':
    main()
