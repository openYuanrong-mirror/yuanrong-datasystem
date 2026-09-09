#!/usr/bin/env python3
"""Generate a self-contained interactive HTML report from procmon CSV."""

import argparse
import csv
import datetime
import html
import json
import os
import sys


METRICS = {
    'cpu_pct': ('CPU', '%', 'cpu'),
    'rss_mb': ('RSS', ' MB', 'memory'),
    'anon_mb': ('Anon', ' MB', 'memory'),
    'shared_mb': ('Shared', ' MB', 'memory'),
    'fd': ('File descriptors', '', 'fd'),
    'tcp_fails_per_sec': ('TCP failures/s', '/s', 'tcp'),
    'tcp_fail_rate_pct': ('TCP fail rate', '%', 'cpu'),
    'bytes_in_mb_per_sec': ('Bytes in', ' MB/s', 'network'),
    'bytes_out_mb_per_sec': ('Bytes out', ' MB/s', 'network'),
    'jemalloc_allocated_mb': ('Jemalloc allocated', ' MB', 'jemalloc'),
    'jemalloc_active_mb': ('Jemalloc active', ' MB', 'jemalloc'),
    'jemalloc_resident_mb': ('Jemalloc resident', ' MB', 'jemalloc'),
    'jemalloc_metadata_mb': ('Jemalloc metadata', ' MB', 'jemalloc'),
    'jemalloc_mapped_mb': ('Jemalloc mapped', ' MB', 'jemalloc'),
    'jemalloc_retained_mb': ('Jemalloc retained', ' MB', 'jemalloc'),
    'jemalloc_dirty_mb': ('Jemalloc dirty', ' MB', 'jemalloc'),
    'jemalloc_muzzy_mb': ('Jemalloc muzzy', ' MB', 'jemalloc'),
    'jemalloc_stats_available': ('Jemalloc stats available', '', 'status'),
    'jemalloc_stats_read_failures': ('Jemalloc read failures', '', 'status'),
}

CHARTS = {
    'cpu': 'CPU and failure rate',
    'memory': 'Process memory',
    'jemalloc': 'Jemalloc memory',
    'network': 'Network throughput',
    'fd': 'File descriptors',
    'tcp': 'TCP failures',
    'status': 'Jemalloc status',
}


def _epoch_ms(value, path, line_number):
    try:
        parsed = datetime.datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(
            f'{path}:{line_number}: invalid timestamp {value!r}') from error
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return int(parsed.timestamp() * 1000)


def load_resource_csv(path):
    """Load one resource_monitor.csv and discard columns with no values."""
    rows = []
    with open(path, newline='', encoding='utf-8-sig') as stream:
        reader = csv.DictReader(stream)
        if not reader.fieldnames or 'timestamp' not in reader.fieldnames:
            raise ValueError(f'{path}: missing timestamp CSV column')
        for line_number, source in enumerate(reader, 2):
            timestamp = (source.get('timestamp') or '').strip()
            if not timestamp:
                continue
            row = {'timestamp': timestamp,
                   'epoch_ms': _epoch_ms(timestamp, path, line_number)}
            pid = (source.get('pid') or '').strip()
            if pid:
                try:
                    row['pid'] = int(pid)
                except ValueError as error:
                    raise ValueError(
                        f'{path}:{line_number}: invalid pid {pid!r}') from error
            for key in METRICS:
                raw_value = (source.get(key) or '').strip()
                if not raw_value:
                    continue
                try:
                    row[key] = float(raw_value)
                except ValueError as error:
                    raise ValueError(
                        f'{path}:{line_number}: invalid {key} {raw_value!r}') from error
            rows.append(row)
    if not rows:
        raise ValueError(f'{path}: no resource samples found')
    metrics = [key for key in METRICS if any(key in row for row in rows)]
    return {'path': os.path.abspath(path), 'rows': rows, 'metrics': metrics}


def _chart_config(metrics):
    charts = []
    for group, title in CHARTS.items():
        keys = [key for key in metrics if METRICS[key][2] == group]
        if keys:
            charts.append({'title': title, 'metrics': [
                {'key': key, 'label': METRICS[key][0], 'unit': METRICS[key][1]}
                for key in keys]})
    return charts


def render_html(data, source_name=None, title='Resource monitor report',
                start_time=None, end_time=None):
    """Render a standalone report with nearest-sample hover and click lock."""
    rows = sorted(data['rows'], key=lambda row: row['epoch_ms'])
    start = (rows[0]['epoch_ms'] if start_time is None else
             _epoch_ms(start_time, '--start-time', 1))
    end = (max(rows[0]['epoch_ms'] + 1000, rows[-1]['epoch_ms'])
           if end_time is None else _epoch_ms(end_time, '--end-time', 1))
    if start >= end:
        raise ValueError('start time must be before end time')
    if not any(start <= row['epoch_ms'] <= end for row in rows):
        raise ValueError('no samples in selected time range')
    payload = json.dumps({
        'source': source_name or data['path'],
        'rows': rows,
        'range': [start, end],
        'charts': _chart_config(data['metrics']),
    }, ensure_ascii=False).replace('</', '<\\/')
    safe_title = html.escape(title)
    return f'''<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{safe_title}</title><style>
:root{{--bg:#f4f7fb;--card:#fff;--text:#182230;--muted:#667085;--grid:#e4e7ec}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--bg);color:var(--text);font:14px system-ui,sans-serif}}
main{{max-width:1440px;margin:auto;padding:24px}}h1{{margin:0 0 4px}}.sub{{color:var(--muted);margin-bottom:18px}}
.chart{{background:var(--card);border:1px solid var(--grid);border-radius:12px;padding:16px;margin:14px 0}}
.chart h2{{font-size:17px;margin:0 0 10px}}.plot{{position:relative;height:320px}}canvas{{width:100%;height:100%;cursor:crosshair;touch-action:pan-y}}
.legend{{display:flex;flex-wrap:wrap;gap:12px;margin-top:8px}}.legend label{{cursor:pointer}}.swatch{{display:inline-block;width:18px;height:3px;margin-right:5px;vertical-align:middle}}
.tooltip{{display:none;position:absolute;top:8px;z-index:2;pointer-events:none;white-space:pre;background:#101828ee;color:#fff;border-radius:6px;padding:8px 10px;font:12px ui-monospace,monospace}}
.time-slider{{margin:8px 18px 0 62px}}.slider-track{{position:relative;height:26px;background:#f2f4f7;border:1px solid #d0d5dd;border-radius:3px;touch-action:pan-y;user-select:none}}
.slider-track .overview{{position:absolute;inset:0;width:100%;height:100%;pointer-events:none}}
.slider-window{{position:absolute;top:0;bottom:0;background:#175cd329;border:1px solid #84adff;cursor:grab}}
.slider-handle{{position:absolute;top:-3px;bottom:-3px;width:12px;padding:0;background:#fff;border:1px solid #84adff;border-radius:3px;cursor:ew-resize;touch-action:none}}
.slider-handle::after{{content:'';position:absolute;left:4px;top:8px;bottom:8px;border-left:2px solid #84adff}}
.slider-handle[data-edge="start"]{{left:0;transform:translateX(-50%)}}.slider-handle[data-edge="end"]{{right:0;transform:translateX(50%)}}
.slider-labels{{display:flex;justify-content:space-between;gap:8px;margin-top:5px;color:var(--muted);font-size:11px}}
#reset-range{{font:inherit;padding:7px 10px;margin-top:10px;border:1px solid #d0d5dd;border-radius:6px;background:var(--card);color:var(--text);cursor:pointer}}
#range-error{{color:#b42318}}
.hint{{color:var(--muted);font-size:12px}}@media(max-width:600px){{main{{padding:12px}}.plot{{height:260px}}}}
</style></head><body><main><h1>{safe_title}</h1>
<div class="sub"></div><div class="hint">拖动图表底部滑块两端选择时间区间，拖动中间平移；图内滚轮缩放、拖动平移。所有图表同步，单击锁定采样点。横轴使用浏览器本地时间。</div>
<button id="reset-range" type="button">重置为全部时间</button>
<div id="range-error" role="alert"></div>
<div id="charts"></div></main><script>
const DATA={payload};
const COLORS=['#175cd3','#d92d20','#039855','#7a5af8','#dc6803','#0086c9','#c11574','#4e5ba6'];
const fmt=v=>Number.isInteger(v)?String(v):Number(v).toFixed(3).replace(/0+$/,'').replace(/\\.$/,'');
const fullStart=DATA.rows[0].epoch_ms,fullEnd=Math.max(fullStart+1000,DATA.rows[DATA.rows.length-1].epoch_ms);
const rangeError=document.getElementById('range-error'),DRAG_THRESHOLD=5,MIN_RANGE_MS=1,WHEEL_ZOOM_RATE=.002;
const refreshCharts=[];
let rangeStart,rangeEnd,viewRows=[];
function applyRange(start,end){{
 if(!Number.isFinite(start)||!Number.isFinite(end)||start>=end)return;
 const rows=DATA.rows.filter(r=>r.epoch_ms>=start&&r.epoch_ms<=end);
 if(!rows.length){{rangeError.textContent='所选时间范围内没有采样点，请调整时间。';return}}
 rangeStart=start;rangeEnd=end;viewRows=rows;rangeError.textContent='';
 document.querySelector('.sub').textContent=`${{DATA.source}} · ${{rows.length}} / ${{DATA.rows.length}} samples · ${{new Date(start).toLocaleString()}} — ${{new Date(end).toLocaleString()}}`;
 refreshCharts.forEach(refresh=>refresh());
}}
document.getElementById('reset-range').onclick=()=>applyRange(fullStart,fullEnd);
applyRange(...DATA.range);
function moveRange(start,span){{
 span=Math.max(MIN_RANGE_MS,Math.min(fullEnd-fullStart,span));
 start=Math.max(fullStart,Math.min(fullEnd-span,start));applyRange(start,start+span);
}}
function createTimeSlider(card,key){{
 const slider=document.createElement('div');slider.className='time-slider';
 slider.innerHTML='<div class="slider-track"><canvas class="overview"></canvas><div class="slider-window"><button type="button" class="slider-handle" data-edge="start" aria-label="调整开始时间"></button><button type="button" class="slider-handle" data-edge="end" aria-label="调整结束时间"></button></div></div><div class="slider-labels"><span></span><span></span></div>';
 card.querySelector('.plot').after(slider);
 const track=slider.querySelector('.slider-track'),windowBox=slider.querySelector('.slider-window'),preview=slider.querySelector('.overview');
 const labels=slider.querySelectorAll('.slider-labels span'),handles=slider.querySelectorAll('.slider-handle');
 let drag=null;
 function update(){{
  const left=Math.max(0,(rangeStart-fullStart)/(fullEnd-fullStart)),right=Math.min(1,(rangeEnd-fullStart)/(fullEnd-fullStart));
  windowBox.style.left=left*100+'%';windowBox.style.width=(right-left)*100+'%';
  [rangeStart,rangeEnd].forEach((value,i)=>{{labels[i].textContent=new Date(value).toLocaleString();handles[i].title=labels[i].textContent}});
 }}
 function drawOverview(){{
  const rect=track.getBoundingClientRect(),ratio=window.devicePixelRatio||1;preview.width=rect.width*ratio;preview.height=rect.height*ratio;
  const ctx=preview.getContext('2d');ctx.scale(ratio,ratio);let low=0,high=0;
  DATA.rows.forEach(row=>{{if(Number.isFinite(row[key])){{low=Math.min(low,row[key]);high=Math.max(high,row[key])}}}});
  if(high<=low)high=low+1;
  ctx.beginPath();let started=false;
  DATA.rows.forEach(row=>{{if(!Number.isFinite(row[key])){{started=false;return}}const x=(row.epoch_ms-fullStart)/(fullEnd-fullStart)*rect.width,y=rect.height-(row[key]-low)/(high-low)*rect.height;started?ctx.lineTo(x,y):ctx.moveTo(x,y);started=true}});
  ctx.strokeStyle='#98a2b3';ctx.lineWidth=1;ctx.stroke();
 }}
 function slide(clientX){{
  const delta=(clientX-drag.x)/track.getBoundingClientRect().width*(fullEnd-fullStart);
  if(drag.edge==='start')applyRange(Math.max(fullStart,Math.min(drag.end-MIN_RANGE_MS,drag.start+delta)),drag.end);
  else if(drag.edge==='end')applyRange(drag.start,Math.min(fullEnd,Math.max(drag.start+MIN_RANGE_MS,drag.end+delta)));
  else moveRange(drag.start+delta,drag.end-drag.start);
 }}
 track.onpointerdown=e=>{{
  if(e.button!==0||!e.isPrimary)return;
  const edge=e.target.dataset.edge;
  if(!windowBox.contains(e.target)){{const rect=track.getBoundingClientRect(),center=fullStart+(e.clientX-rect.left)/rect.width*(fullEnd-fullStart);moveRange(center-(rangeEnd-rangeStart)/2,rangeEnd-rangeStart)}}
  drag={{x:e.clientX,start:rangeStart,end:rangeEnd,edge}};track.setPointerCapture(e.pointerId);e.preventDefault();
 }};
 track.onpointermove=e=>{{if(drag)slide(e.clientX)}};
 track.onpointerup=e=>{{if(drag){{slide(e.clientX);drag=null}}}};
 track.onpointercancel=track.onlostpointercapture=()=>{{drag=null}};
 handles.forEach(handle=>{{handle.onkeydown=e=>{{
  if(e.key!=='ArrowLeft'&&e.key!=='ArrowRight')return;e.preventDefault();
  const step=Math.max(MIN_RANGE_MS,(rangeEnd-rangeStart)/10)*(e.key==='ArrowLeft'?-1:1);
  if(handle.dataset.edge==='start')applyRange(Math.max(fullStart,Math.min(rangeEnd-MIN_RANGE_MS,rangeStart+step)),rangeEnd);
  else applyRange(rangeStart,Math.min(fullEnd,Math.max(rangeStart+MIN_RANGE_MS,rangeEnd+step)));
 }}}});
 refreshCharts.push(update);update();new ResizeObserver(drawOverview).observe(track);
}}
function nearestIndex(target){{let best=0,delta=Infinity;viewRows.forEach((r,i)=>{{const d=Math.abs(r.epoch_ms-target);if(d<delta){{best=i;delta=d}}}});return best}}
function createChart(config,chartIndex){{
 const card=document.createElement('section');card.className='chart';
 card.innerHTML=`<h2>${{config.title}}</h2><div class="plot"><canvas data-point-index=""></canvas><div class="tooltip"></div></div><div class="legend"></div>`;
 document.getElementById('charts').appendChild(card);
 const canvas=card.querySelector('canvas'),tip=card.querySelector('.tooltip'),legend=card.querySelector('.legend');
 const lines=config.metrics.map((m,i)=>({{...m,color:COLORS[(chartIndex*3+i)%COLORS.length],visible:true}}));
 lines.forEach(line=>{{const label=document.createElement('label');label.innerHTML=`<input type="checkbox" checked> <i class="swatch" style="background:${{line.color}}"></i>${{line.label}}`;label.querySelector('input').onchange=e=>{{line.visible=e.target.checked;draw()}};legend.appendChild(label)}});
 let geom=null,hover=null,locked=null,lastX=0,dragStart=null,dragRange=null,dragged=false;
 function clampX(x){{return Math.max(geom.p.l,Math.min(geom.W-geom.p.r,x))}}
 function timeAt(x){{return geom.xmin+(x-geom.p.l)/(geom.W-geom.p.l-geom.p.r)*(geom.xmax-geom.xmin)}}
 function selected(){{return locked===null?hover:locked}}
 function draw(){{
  const rect=canvas.getBoundingClientRect(),ratio=window.devicePixelRatio||1;canvas.width=rect.width*ratio;canvas.height=rect.height*ratio;
  const ctx=canvas.getContext('2d');ctx.scale(ratio,ratio);const W=rect.width,H=rect.height,p={{l:62,r:18,t:12,b:40}};
  const visible=lines.filter(l=>l.visible);
  let ymin=0,ymax=0;visible.forEach(line=>viewRows.forEach(row=>{{const value=row[line.key];if(Number.isFinite(value)){{ymin=Math.min(ymin,value);ymax=Math.max(ymax,value)}}}}));
  if(ymax<=ymin)ymax=ymin+1;ymax*=1.08;
  const xmin=rangeStart,xmax=rangeEnd;
  const sx=x=>p.l+(x-xmin)/(xmax-xmin)*(W-p.l-p.r),sy=y=>H-p.b-(y-ymin)/(ymax-ymin)*(H-p.t-p.b);
  ctx.font='12px system-ui';ctx.fillStyle='#667085';ctx.strokeStyle='#e4e7ec';ctx.lineWidth=1;
  for(let i=0;i<=5;i++){{const y=p.t+i*(H-p.t-p.b)/5;ctx.beginPath();ctx.moveTo(p.l,y);ctx.lineTo(W-p.r,y);ctx.stroke();ctx.fillText(fmt(ymax-(ymax-ymin)*i/5),5,y+4)}}
  [0,.25,.5,.75,1].forEach(f=>{{const x=p.l+f*(W-p.l-p.r),t=xmin+f*(xmax-xmin);ctx.fillText(new Date(t).toLocaleTimeString(undefined,{{hour12:false}}),Math.max(2,Math.min(W-75,x-30)),H-12)}});
  visible.forEach(line=>{{ctx.strokeStyle=line.color;ctx.lineWidth=2;ctx.beginPath();let started=false,count=0,lastPoint;viewRows.forEach(r=>{{if(r[line.key]===undefined){{started=false;return}}const x=sx(r.epoch_ms),y=sy(r[line.key]);started?ctx.lineTo(x,y):ctx.moveTo(x,y);started=true;count++;lastPoint=[x,y]}});ctx.stroke();if(count===1){{ctx.beginPath();ctx.arc(...lastPoint,3,0,Math.PI*2);ctx.fillStyle=line.color;ctx.fill()}}}});
  geom={{W,H,p,xmin,xmax,sx,sy,visible}};const index=selected();canvas.dataset.pointIndex=index===null?'':String(index);
  if(index!==null){{const row=viewRows[index],x=sx(row.epoch_ms);ctx.strokeStyle='#344054';ctx.setLineDash([4,4]);ctx.beginPath();ctx.moveTo(x,p.t);ctx.lineTo(x,H-p.b);ctx.stroke();ctx.setLineDash([]);visible.forEach(line=>{{if(row[line.key]===undefined)return;ctx.fillStyle='#fff';ctx.strokeStyle=line.color;ctx.lineWidth=2;ctx.beginPath();ctx.arc(x,sy(row[line.key]),4,0,Math.PI*2);ctx.fill();ctx.stroke()}})}}
 }}
 function show(index,mx){{if(index===null)return;const row=viewRows[index],linesText=geom.visible.map(line=>`${{line.label}}: ${{row[line.key]===undefined?'N/A':fmt(row[line.key])+line.unit}}`);tip.textContent=`${{locked===null?'':'Locked · '}}${{row.timestamp}}\\n${{linesText.join('\\n')}}`;tip.style.display='block';tip.style.left=Math.max(4,Math.min(mx+12,geom.W-tip.offsetWidth-6))+'px'}}
 canvas.onpointerdown=e=>{{
  if(!geom||e.button!==0||!e.isPrimary)return;
  const mx=e.clientX-canvas.getBoundingClientRect().left;
  if(mx<geom.p.l||mx>geom.W-geom.p.r)return;
  dragStart=mx;dragRange=[rangeStart,rangeEnd];dragged=false;canvas.setPointerCapture(e.pointerId);e.preventDefault();
 }};
 canvas.onpointermove=e=>{{
  if(!geom)return;const mx=e.clientX-canvas.getBoundingClientRect().left;lastX=mx;
  if(dragStart!==null){{if(Math.abs(mx-dragStart)>=DRAG_THRESHOLD||dragged){{dragged=true;const span=dragRange[1]-dragRange[0];moveRange(dragRange[0]-(mx-dragStart)/(geom.W-geom.p.l-geom.p.r)*span,span)}}return}}
  if(locked!==null)return;
  if(mx<geom.p.l||mx>geom.W-geom.p.r){{hover=null;tip.style.display='none';draw();return}}
  hover=nearestIndex(timeAt(mx));draw();show(hover,mx);
 }};
 canvas.onpointerup=e=>{{
  if(dragStart===null)return;
  const mx=e.clientX-canvas.getBoundingClientRect().left,start=dragStart;dragStart=null;
  if(dragged||Math.abs(mx-start)>=DRAG_THRESHOLD){{const span=dragRange[1]-dragRange[0];moveRange(dragRange[0]-(mx-start)/(geom.W-geom.p.l-geom.p.r)*span,span);return}}
  if(locked!==null){{locked=null;hover=null;tip.style.display='none';draw();return}}
  locked=nearestIndex(timeAt(mx));lastX=mx;draw();show(locked,mx);
 }};
 canvas.onpointercancel=canvas.onlostpointercapture=()=>{{if(dragStart!==null){{dragStart=null;draw()}}}};
 canvas.onpointerleave=()=>{{if(locked===null&&dragStart===null){{hover=null;tip.style.display='none';draw()}}}};
 canvas.addEventListener('wheel',e=>{{
  const rect=canvas.getBoundingClientRect(),mx=e.clientX-rect.left;if(!geom||mx<geom.p.l||mx>geom.W-geom.p.r)return;
  e.preventDefault();const fraction=(mx-geom.p.l)/(geom.W-geom.p.l-geom.p.r),span=rangeEnd-rangeStart;
  const delta=e.deltaMode===1?e.deltaY*16:e.deltaMode===2?e.deltaY*rect.height:e.deltaY;
  const nextSpan=Math.max(MIN_RANGE_MS,Math.min(fullEnd-fullStart,span*Math.exp(Math.max(-1,Math.min(1,delta*WHEEL_ZOOM_RATE)))));
  moveRange(timeAt(clampX(mx))-fraction*nextSpan,nextSpan);
 }},{{passive:false}});
 refreshCharts.push(()=>{{hover=null;locked=null;tip.style.display='none';draw()}});
 new ResizeObserver(()=>{{draw();if(locked!==null)show(locked,lastX)}}).observe(canvas);draw();
 createTimeSlider(card,config.metrics[0].key);
}}
DATA.charts.forEach(createChart);
</script></body></html>'''


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Generate interactive HTML from resource_monitor.csv')
    parser.add_argument('input', help='procmon resource_monitor.csv')
    parser.add_argument('-o', '--output', default='resource_monitor.html')
    parser.add_argument('--title', default='Resource monitor report')
    parser.add_argument('--start-time', help='Initial range start (ISO 8601; '
                        'without an offset, uses the generator local timezone)')
    parser.add_argument('--end-time', help='Initial range end (ISO 8601; '
                        'without an offset, uses the generator local timezone)')
    args = parser.parse_args(argv)
    try:
        data = load_resource_csv(args.input)
        report = render_html(data, os.path.basename(args.input), args.title,
                             args.start_time, args.end_time)
        with open(args.output, 'w', encoding='utf-8') as stream:
            stream.write(report)
    except (OSError, ValueError) as error:
        parser.error(str(error))
    print(f'Generated {args.output} from {len(data["rows"])} samples')
    return 0


if __name__ == '__main__':
    sys.exit(main())
