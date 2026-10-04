#!/usr/bin/env python3
"""Build the internal consolidated report from verified analysis tables."""
import argparse,json
from pathlib import Path
import pandas as pd
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.styles import getSampleStyleSheet,ParagraphStyle
from reportlab.lib.utils import ImageReader
from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,Table,TableStyle,Image,PageBreak,KeepTogether

FONT=Path('/usr/share/fonts/truetype/dejavu')
pdfmetrics.registerFont(TTFont('DV',str(FONT/'DejaVuSans.ttf')))
pdfmetrics.registerFont(TTFont('DV-Bold',str(FONT/'DejaVuSans-Bold.ttf')))
pdfmetrics.registerFontFamily('DV',normal='DV',bold='DV-Bold',italic='DV',boldItalic='DV-Bold')
INK=colors.HexColor('#142e41');TEAL=colors.HexColor('#116b73');MUTED=colors.HexColor('#526572')
W,H=A4;M=44;CONTENT=W-2*M
styles=getSampleStyleSheet()
styles.add(ParagraphStyle(name='BodyFinal',fontName='DV',fontSize=9.8,leading=14.4,textColor=INK,spaceAfter=8))
styles.add(ParagraphStyle(name='TitleFinal',fontName='DV-Bold',fontSize=26,leading=31,textColor=INK,spaceAfter=12))
styles.add(ParagraphStyle(name='HeadingFinal',fontName='DV-Bold',fontSize=19,leading=24,textColor=INK,spaceAfter=12))
styles.add(ParagraphStyle(name='SubFinal',fontName='DV-Bold',fontSize=11,leading=16,textColor=TEAL,spaceBefore=9,spaceAfter=6))
styles.add(ParagraphStyle(name='SmallFinal',fontName='DV',fontSize=8.1,leading=11.7,textColor=MUTED,spaceAfter=7))
styles.add(ParagraphStyle(name='CellFinal',fontName='DV',fontSize=8.5,leading=12,textColor=INK))
styles.add(ParagraphStyle(name='CalloutFinal',fontName='DV-Bold',fontSize=11,leading=17,textColor=TEAL,spaceBefore=6,spaceAfter=10))

def p(text,style='BodyFinal'):return Paragraph(text,styles[style])
def table(rows,widths):
    a=[[p(str(v),'CellFinal') for v in row] for row in rows]
    t=Table(a,colWidths=widths,repeatRows=1,hAlign='LEFT')
    t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor('#e5eff2')),
       ('VALIGN',(0,0),(-1,-1),'TOP'),('LEFTPADDING',(0,0),(-1,-1),7),('RIGHTPADDING',(0,0),(-1,-1),7),
       ('TOPPADDING',(0,0),(-1,-1),7),('BOTTOMPADDING',(0,0),(-1,-1),7),
       ('LINEBELOW',(0,0),(-1,0),.7,TEAL),('LINEBELOW',(0,1),(-1,-1),.35,colors.HexColor('#dce3e6'))]))
    return t
def figure(root,name,width=CONTENT):
    path=root/'figures'/(name+'.png');iw,ih=ImageReader(str(path)).getSize()
    return Image(str(path),width=width,height=width*ih/iw,hAlign='CENTER')
def footer(canvas,doc):
    canvas.saveState();canvas.setStrokeColor(colors.HexColor('#d7e1e7'));canvas.line(M,40,W-M,40)
    canvas.setFont('DV',7.6);canvas.setFillColor(MUTED)
    canvas.drawString(M,28,'CANOPIE extension | Internal study closeout | 04 October 2026')
    canvas.drawRightString(W-M,28,str(doc.page));canvas.restoreState()

def main(root):
    tables=root/'tables';v=json.loads((root/'validation.json').read_text())
    assert v['total_records']==1392 and v['repeated_measured']==1080
    ratios=pd.read_csv(tables/'sql_ratio_intervals.csv',dtype={'condition':str})
    summary=pd.read_csv(tables/'condition_summary.csv');system=pd.read_csv(tables/'system_measurements.csv')
    env=pd.read_csv(tables/'sqlite_environment_ratios.csv');mix=pd.read_csv(tables/'mixed_storage_ratios.csv')
    label={'sqlite-native':'SQLite native','sqlite-shifter':'SQLite Shifter','postgres':'PostgreSQL','influx':'InfluxDB'}
    story=[]
    def add(text,style='BodyFinal'):story.append(p(text,style))
    def new(title):story.append(PageBreak());add(title,'HeadingFinal')
    add('Storage, databases<br/>and concurrency','TitleFinal')
    add('Consolidated Perlmutter experiment report','SubFinal')
    add('Manoj Khatri · HPCAI Lab · 04 October 2026','SmallFinal')
    add('Internal findings and analysis. This document closes the agreed experimental round; it is not a publication manuscript.','CalloutFinal')
    add('The CANOPIE extension now supports repeatable, configuration-driven comparisons of Lustre and memory-backed tmpfs across SQLite native, SQLite under Shifter, PostgreSQL with a Shifter server, and InfluxDB with a Shifter server. Metadata, telemetry and mixed data-type workloads run with multiple client processes on one compute node.')
    story.append(table([
      ['Evidence included','Measured / observations','Warmups'],
      ['Original four-deployment pilots','240','48'],
      ['1,024-query SQL follow-up','180','36'],
      ['Mixed-workload pilots','120','24'],
      ['SQL allocation-repeat study','540','108'],
      ['Repeated-performance subtotal','1,080','216'],
      ['10M-record calibration (single observations)','48','0'],
      ['System diagnostics (single observations)','48','0']], [300,127,CONTENT-427]))
    add('Total: 96 runs and 1,392 trial records. Calibration and diagnostics are not pooled with repeated-performance trials. Earlier integration and separate profiling-overhead experiments are outside this package.','SmallFinal')
    add('Principal findings','SubFinal')
    add('<b>Storage benefit depends on workload and concurrency.</b> Across three allocation blocks, native SQLite metadata-write throughput gains fall from <b>1.87×</b> on tmpfs at one client to <b>1.03×</b> at 64; the latter 95% interval includes parity. PostgreSQL retains a metadata-write gain of about <b>1.38×</b> at 64 clients.')
    add('<b>The metadata-only result does not describe every workload.</b> The mixed native SQLite pilot retains a <b>1.50×</b> mean write gain at 64 clients. Supported system measurements were collected, but the attempted node-wide perf counters were unavailable under the site access policy.')

    new('Design, measurement boundaries and settings')
    add('Repeated trials write one million total records in batches of 1,000 and then perform warm range-query reads returning 100 rows per query. Original SQL pilots use 64,000 queries; InfluxDB and later studies use 1,024. Each repeated run has one warmup and five measured repetitions per storage/client condition, with seeded condition ordering.')
    add('<b>Startup is already excluded from the performance timers.</b> Workers initialize, report readiness and wait for an event. The controller starts its timer immediately before releasing that event. Workload duration includes row generation and scheduling after release. Write-batch latency measures the backend insertion call; it excludes row generation. SQL completion duration also includes the final checkpoint and intervening controller/worker-finalization time.')
    story.append(table([
      ['Deployment','Observed version / measurement boundary'],
      ['SQLite native','Python 3.11.7; SQLite 3.44.2. WAL + NORMAL; automatic checkpointing at 1,000 pages and explicit final TRUNCATE checkpoint.'],
      ['SQLite Shifter','Python 3.11.15; SQLite 3.46.1. Same configured checkpoint policy, different software environment.'],
      ['PostgreSQL','Server 16.12; native Python clients. Automatic checkpointing plus explicit final CHECKPOINT.'],
      ['InfluxDB','Server 2.9.1; native Python HTTP clients. Completion at synchronous HTTP 204 acknowledgement; no equivalent final checkpoint is timed.']], [105,CONTENT-105]))
    add('PostgreSQL settings are present in the newer raw records','SubFinal')
    add('All 276 PostgreSQL trials in the four new archives record <b>fsync=on</b>, <b>synchronous_commit=on</b> and <b>full_page_writes=on</b>. The selected settings also show shared_buffers=16,384 blocks of 8kB (128 MiB), max_connections=100, checkpoint_timeout=300 s, max_wal_size=1,024 MB and autovacuum=on. The complete recorded selection is exported in database_configuration.csv. This is a selected settings snapshot, not a dump of every server parameter.')
    add('Statistical unit and limits','SubFinal')
    add('SQL repeat estimates first average five trials within each condition/allocation. Storage contrasts are paired within that allocation. Reported ratio estimates are geometric means of three allocation contrasts, with Student-t 95% intervals on their logarithms (2 degrees of freedom). These exploratory intervals assume independent, approximately normal log contrasts; no multiple-comparison adjustment is applied. The three telemetry allocations used only two distinct nodes. Mixed-pilot intervals use five within-allocation repetition contrasts and do not estimate allocation-to-allocation variation.')
    add('Counts validate stored output and expected query sizes. They do not make database durability settings or engine-specific query execution equivalent. tmpfs is volatile memory storage.','SmallFinal')

    new('SQL writes: the repeated scaling pattern')
    story.append(figure(root,'sql_write_throughput'))
    add('Figure 1. Bold lines: arithmetic mean of three allocation means. Faint lines: individual allocation means, each based on five measured trials. Write throughput includes the final SQL checkpoint. Each panel has its own y-axis scale.','SmallFinal')
    add('For metadata on Lustre, PostgreSQL rises from approximately <b>73k to 392k records/s</b> between one and 64 clients. Native SQLite falls from about <b>237k to 192k records/s</b>. On tmpfs, native SQLite falls from <b>443k to 197k records/s</b>, whereas PostgreSQL rises from <b>104k to 542k records/s</b>.')
    add('Telemetry shows a different response: PostgreSQL and SQLite gain from initial concurrency, while some curves flatten or decline between 16 and 64 clients. There is no single client count that is best for all storage/database/workload combinations.')
    add('What the repetitions add','SubFinal')
    add('The six new jobs contain three separate allocations for metadata and three for telemetry. Every allocation runs all three SQL deployments on the same node. Deployment order rotates across blocks: native/Shifter/PostgreSQL; Shifter/PostgreSQL/native; PostgreSQL/native/Shifter. This reduces a fixed deployment-order confound, but does not eliminate time-varying shared-filesystem load or establish a universal node population effect.')
    story.append(table([['Workload','Allocation jobs','Distinct compute nodes'],['Metadata','59304573, 59304575, 59304580','3'],['Telemetry','59304581, 59304582, 59304585','2 (last two allocations reused a node)']], [85,229,CONTENT-314]))
    add('Source: sql_allocation_means.csv; all 540 measured trials remain available individually in all_trials.csv.','SmallFinal')

    new('Storage gains and uncertainty')
    story.append(figure(root,'sql_write_storage_ratios'))
    add('Figure 2. tmpfs/Lustre write-throughput contrasts across three allocation blocks. Whiskers are log-t 95% intervals; dashed line denotes parity. Ratios above one favor tmpfs for this measured completion boundary.','SmallFinal')
    selected=[]
    for d in ['sqlite-native','sqlite-shifter','postgres']:
        vals=[]
        for c in ['1','64']:
            r=ratios[(ratios.workload=='metadata')&(ratios.deployment==d)&(ratios.phase=='write')&(ratios.kind=='storage')&(ratios.condition==c)].iloc[0]
            vals.append(f'{r.geometric_ratio:.2f} [{r.ci95_low:.2f}, {r.ci95_high:.2f}]')
        selected.append([label[d],*vals])
    story.append(table([['Metadata writes','1 client: ratio [95% CI]','64 clients: ratio [95% CI]'],*selected],[136,185,CONTENT-321]))
    add('The large single-client SQLite benefit contracts sharply at high concurrency. Native SQLite’s 64-client interval includes one. The Shifter estimate is about 2.2% below parity with a narrow interval in these three blocks; that small result remains exploratory and should not be generalized to every allocation or workload.')
    add('PostgreSQL retains a clear tmpfs advantage for metadata writes in these observations. The supported inference is an interaction between storage placement, concurrency and database behavior. Direct SQLite lock-wait timing, partitioning and busy-handler interventions were not measured here, so they cannot yet explain the cause.','CalloutFinal')

    new('Reads and execution environments')
    story.append(figure(root,'sql_read_throughput'))
    add('Figure 3. Warm-read throughput in the SQL allocation repeats, with 1,024 total queries per trial. Bold and faint lines use the same aggregation as Figure 1. Each panel has its own y-axis scale.','SmallFinal')
    add('<b>360 of 540 measured SQL-repeat read phases last under 50 ms.</b> At 64 clients, a 1,024-query trial assigns only 16 queries to each worker. These are synchronized warm-query bursts, not evidence of sustained service throughput. Event-release skew, scheduling, caching, validation and client-side work can matter even though process startup is outside the timer.')
    add('The earlier follow-up likewise has 120 of 180 measured read phases under 50 ms. Earlier 64,000-query pilot results remain separate contextual evidence; query count, source revisions and allocations differ. InfluxDB query rates must also be interpreted in the context of its HTTP/Flux path and schema mapping.')
    add('Native versus Shifter: what is controlled','SubFinal')
    add(f'Within the allocation-repeat study, SQLite Shifter/native write ratios range from <b>{env[env.phase=="write"].geometric_ratio.min():.2f} to {env[env.phase=="write"].geometric_ratio.max():.2f}</b> across conditions. Same-node execution and rotated order improve the comparison, but Python and SQLite versions differ. These are execution-environment ratios; they do not isolate container overhead. The supplemental figure and sqlite_environment_ratios.csv retain all intervals, including reads.')
    add('A matched-software comparison and distributed multi-node scaling were not performed. Separate allocations on different nodes are repeated single-node experiments, not one distributed experiment.','SmallFinal')

    new('Mixed workloads: composition matters')
    story.append(figure(root,'mixed_write_throughput'))
    add('Figure 4. Mixed-workload write throughput: mean ± sample SD of five measured trials within one allocation per deployment. Y-axis scales differ. SQL includes the final checkpoint; InfluxDB ends at acknowledgement, so engine rankings are not durability-normalized.','SmallFinal')
    add('Each trial writes 500,000 metadata and 500,000 telemetry records. Each logical batch contains two backend write calls; the pair is not one cross-type atomic transaction. Writes are followed by 512 metadata and 512 telemetry range queries. This is a mixed data-type workload, not simultaneous readers and writers.')
    rrows=[]
    for d in label:
        g=mix[(mix.deployment==d)&(mix.phase=='write')].set_index('clients')
        rrows.append([label[d],*[f'{g.loc[c,"ratio_of_means"]:.2f}×' for c in [1,16,64]]])
    story.append(table([['tmpfs/Lustre mean write ratio','1 client','16 clients','64 clients'],*rrows],[246,87,87,CONTENT-420]))
    add('Native SQLite retains about a 1.50× mean tmpfs benefit at 64 clients here, compared with near parity for metadata-only writes in the separate allocation-repeat study. Workload and campaign both differ, so this is evidence against treating the metadata-only pattern as universal, not an isolated causal composition experiment.')
    add('Tail latency remains consequential: native SQLite’s median trial write p99 rises from about 28 ms to 3,581 ms on Lustre between one and 64 clients. These are logical-batch latencies, not per-record latencies.','SmallFinal')

    new('Workload size: completed coverage and the gap')
    story.append(table([['Size in this implementation','Completed evidence'],['100,000 records','Repeated configurations prepared; sweep was not submitted successfully.'],['1,000,000 records','Repeated pilots cover all four deployments, all three workloads, both tiers and 1/16/64 clients.'],['10,000,000 records','48 calibration observations: four deployments × three workloads × two tiers × 1/64 clients. One observation per condition; no warmups.']], [126,CONTENT-126]))
    story.append(Spacer(1,7));story.append(figure(root,'size_calibration_context',width=450))
    add('Figure 5. Descriptive 10M/1M throughput ratios. Each cell uses one 10M observation. 1M references are SQL allocation means, mixed pilots, or original InfluxDB pilots as appropriate. Separate campaigns, allocations and some source versions differ. No size-effect interval or causal scaling estimate is claimed.','SmallFinal')
    add('The professor’s low/medium/high values were implemented as records written, while reads remain at 1,024 queries. They are not total operation counts. The 24-task repeated 100K/10M array was rejected before an array ID was assigned: estimated requested cost was 72 node-hours versus a displayed 7.00-hour balance (repository balance 6.25). This was a reservation estimate, not 72 hours consumed. Those sweeps were then explicitly omitted from this round.','SmallFinal')

    new('System diagnostics: collected and unavailable')
    story.append(figure(root,'system_cpu_iowait',width=CONTENT))
    add('Figure 6. All 48 conditions from job 59329430, one observation per condition. Node-wide percentages cover the complete trial lifecycle, including setup, validation, reads, shutdown and monitors. Heatmaps use different scales; values are rounded. Row labels: meta = metadata; tele = telemetry.','SmallFinal')
    add(f'The job completed in <b>14 minutes 46 seconds</b>. All node sample summaries were recomputed successfully. Node busy fractions range from {system.node_busy_percent.min():.2f}% to {system.node_busy_percent.max():.2f}% when averaged across the node’s 256 logical CPUs and the full lifecycle. This is not a phase-specific database CPU utilization measurement.')
    add('Sampled node used memory is MemTotal minus MemAvailable; it includes OS, cache, other node activity and tmpfs data. It is not database PSS or an isolated database memory footprint. GNU time provides waited-process CPU, maximum single-process RSS and context-switch counters. iostat logs are retained; their device counters are not treated as Lustre network/filesystem bandwidth.')
    add('<b>Node-wide perf events were unavailable in all 48 trials.</b> The probes were denied under perf_event_paranoid=2. Cycles, instructions, cache misses, perf context-switch counts and derived IPC therefore remain missing, not zero. No permissions were changed or additional jobs launched for the blocked probes.')
    add('Low node iowait does not rule out Lustre delays or SQLite lock waits. These diagnostics describe observed resource behavior; they do not identify the cause of the SQLite throughput collapse. Instrumentation overhead was not isolated in this diagnostic study.','SmallFinal')

    new('Study closeout and reproducible artifact')
    add('What this round establishes','SubFinal')
    add('A working benchmark framework now couples storage placement, database adapters, concurrent workloads, lifecycle control, correctness checks and provenance. The data demonstrate reproducible performance patterns and show why storage choice should be evaluated together with workload and concurrency. Allocation repeats strengthen the SQL evidence; resource diagnostics provide additional context.')
    add('The contribution is a validated measurement study and reusable experimental artifact. Production readiness, publication acceptance and a proven lock-contention mechanism are not established by these results. The report is intended to support discussion and the researcher’s own manuscript preparation.')
    story.append(table([
      ['Fixed item','Closeout status'],
      ['1. Mixed-workload pilots','Complete and analyzed for the implemented mixed data-type workload.'],
      ['2. Workload-size coverage','1M repeated evidence and 10M calibration complete. Repeated 100K/10M sweeps explicitly excluded after budget rejection.'],
      ['3. Comparisons and repeats','SQL allocation repeats validated and analyzed. Matched-software runtime comparison excluded from claims; no repeated InfluxDB allocation study.'],
      ['4. System measurements','48 diagnostics validated and analyzed. Available measurements retained; blocked perf events documented.'],
      ['5. Consolidated deliverables','This report, tables, figures, reproducible scripts and original inputs complete the analysis package. Publication manuscript remains the researcher’s work.']], [132,CONTENT-132]))
    add('Verification and reproduction','SubFinal')
    add('The six input archives reconcile to 96 runs. For 1,104 trials with full raw JSON, write/read latency summaries were recomputed from saved samples. The 288 original baseline records were checked against CSV and saved metadata; that input does not contain the original per-request arrays. All 82 newly exported run source manifests match bundled source snapshots. Databases were not reopened during this offline analysis.')
    add(f'The companion ZIP contains {len(list(tables.glob("*.csv")))} CSV tables, seven figures in PNG and vector PDF, original inputs, validation.json, dependency pins, SHA256SUMS and the analysis/report scripts. README.md documents reproduction and metric scope. This package adds analysis; it does not submit or rerun experiments.')
    add('Unperformed work includes distributed multi-node scaling, the professor’s partitioned-SQLite/direct-lock-wait/busy-handler experiments, matched software versions, and repeated low/high-size sweeps. Those are explicit boundaries, not silently completed checklist items.','SmallFinal')
    doc=SimpleDocTemplate(str(root/'ipdps-consolidated-report.pdf'),pagesize=A4,rightMargin=M,leftMargin=M,topMargin=43,bottomMargin=53,
        title='Storage, databases and concurrency: consolidated Perlmutter study',author='Manoj Khatri - HPCAI Lab',subject='Internal findings, analysis and study limitations')
    doc.build(story,onFirstPage=footer,onLaterPages=footer)
    print('PASS: consolidated internal report created')

if __name__=='__main__':
    a=argparse.ArgumentParser();a.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);main(a.parse_args().root)
