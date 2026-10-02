#!/usr/bin/env python3
"""Generate figures and a six-page internal progress report from saved trials."""
import argparse
from pathlib import Path
from xml.sax.saxutils import escape

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.pagesizes import letter
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,Table,TableStyle,Image,PageBreak

from analyze_comparison import analyze,SQL,WORKLOADS,CLIENTS,STORAGE,METRICS,descriptive

NAMES={'sqlite-native':'SQLite native','sqlite-shifter':'SQLite Shifter','postgres':'PostgreSQL','influx':'InfluxDB reference'}
SC={'lustre':'#246F9D','tmpfs':'#CB682E'}
DC={'sqlite-native':'#146B8B','sqlite-shifter':'#809FAF','postgres':'#25816B'}
NAVY='#17374D';MUTED='#526573'

def group(data,ds,d,w,s,c,metric):
    return np.asarray([float(r[metric]) for r in data['groups'][(ds,d,w,s,c)]])

def lookup(rows,**keys):
    found=[r for r in rows if all(r[k]==v for k,v in keys.items())]
    if len(found)!=1:raise ValueError(('Nonunique lookup',keys,len(found)))
    return found[0]

def save(fig,out,name):
    fig.savefig(out/(name+'.png'),dpi=220,facecolor='white')
    fig.savefig(out/(name+'.pdf'),facecolor='white')
    plt.close(fig)

def make_figures(data,out):
    folder=out/'figures';folder.mkdir(exist_ok=True)
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.titlesize':11,
        'axes.titleweight':'bold','axes.spines.right':False,'axes.spines.top':False,
        'axes.edgecolor':'#9EABB5','grid.color':'#DEE6EC','grid.linewidth':.6,'pdf.fonttype':42})
    for phase,deployments,size in (('write',SQL,(10.8,7.8)),('read',SQL+('influx',),(10.8,9.5))):
        fig,axes=plt.subplots(len(deployments),2,figsize=size,layout='constrained')
        for i,d in enumerate(deployments):
            ds='baseline' if d=='influx' else 'read1024'
            for j,w in enumerate(WORKLOADS):
                ax=axes[i,j]
                for s,offset,marker in (('lustre',-.08,'o'),('tmpfs',.08,'s')):
                    means,lows,highs=[],[],[]
                    for x,c in enumerate(CLIENTS):
                        values=group(data,ds,d,w,s,c,METRICS[phase])/(1000 if phase=='write' else 1)
                        st=descriptive(values)
                        means.append(st['mean']);lows.append(st['mean_t95_low']);highs.append(st['mean_t95_high'])
                        ax.scatter(x+offset+np.linspace(-.03,.03,5),values,s=12,alpha=.5,color=SC[s])
                    ax.errorbar(np.arange(3)+offset,means,yerr=[np.array(means)-lows,np.array(highs)-means],
                                color=SC[s],marker=marker,ms=4,capsize=3,lw=1.4,label=s)
                ax.set_title(NAMES[d]+' | '+w,loc='left')
                ax.set_xticks(range(3),[str(c) for c in CLIENTS]);ax.set_xlabel('Clients');ax.grid(axis='y')
                ax.set_ylabel('krecords/s' if phase=='write' else 'queries/s')
                if phase=='read':ax.set_yscale('log')
                else:ax.set_ylim(bottom=0)
                if i==0 and j==0:ax.legend(frameon=False,ncol=2,fontsize=9)
        save(fig,folder,phase+'_throughput_1024')
    fig,axes=plt.subplots(1,2,figsize=(10.8,3.6),layout='constrained')
    for ax,w in zip(axes,WORKLOADS):
        for d,offset in zip(SQL,(-.16,0,.16)):
            rr=[lookup(data['gains'],dataset='read1024',deployment=d,workload=w,phase='write',clients=c) for c in CLIENTS]
            g=np.array([r['geometric_ratio'] for r in rr]);lo=np.array([r['log_t95_low'] for r in rr]);hi=np.array([r['log_t95_high'] for r in rr])
            ax.errorbar(np.arange(3)+offset,g,yerr=[g-lo,hi-g],color=DC[d],marker='o',ms=4,capsize=3,lw=1.3,label=NAMES[d])
        ax.axhline(1,color='#61727D',ls='--',lw=1)
        ax.set_title(w.capitalize(),loc='left');ax.set_xticks(range(3),['1','16','64']);ax.set_xlabel('Clients')
        ax.set_ylabel('tmpfs / Lustre write throughput');ax.grid(axis='y');ax.set_ylim(.65,2.05)
    axes[0].legend(frameon=False,fontsize=9,loc='upper right')
    save(fig,folder,'write_storage_gains_1024')
    fig,axes=plt.subplots(3,2,figsize=(10.8,6.8),layout='constrained')
    for i,d in enumerate(SQL):
        for j,w in enumerate(WORKLOADS):
            ax=axes[i,j]
            for s,marker in (('lustre','o'),('tmpfs','s')):
                ratios=[lookup(data['context'],deployment=d,workload=w,storage=s,clients=c)['read1024_over_baseline64000'] for c in CLIENTS]
                ax.plot(range(3),ratios,marker=marker,color=SC[s],lw=1.5,label=s)
            ax.axhline(1,color='#61727D',lw=1,ls='--');ax.set_ylim(0,1.25)
            ax.set_title(NAMES[d]+' | '+w,loc='left');ax.set_xticks(range(3),['1','16','64'])
            ax.set_xlabel('Clients');ax.set_ylabel('1,024 / 64,000-query\nmean throughput');ax.grid(axis='y')
            if i==0 and j==0:ax.legend(frameon=False,ncol=2,fontsize=9)
    save(fig,folder,'read_count_context')
    fig,axes=plt.subplots(2,2,figsize=(10.8,6.0),layout='constrained')
    for i,phase in enumerate(('write','read')):
        for j,w in enumerate(WORKLOADS):
            ax=axes[i,j]
            for s,marker in (('lustre','o'),('tmpfs','s')):
                y=[lookup(data['runtime'],dataset='read1024',workload=w,storage=s,clients=c,phase=phase)['shifter_over_native'] for c in CLIENTS]
                ax.plot(range(3),y,marker=marker,color=SC[s],lw=1.5,label=s)
            ax.axhline(1,color='#61727D',ls='--',lw=1);ax.set_ylim(.75,1.23)
            ax.set_title(w.capitalize()+' | '+phase,loc='left');ax.set_xticks(range(3),['1','16','64'])
            ax.set_xlabel('Clients');ax.set_ylabel('Shifter / native mean throughput');ax.grid(axis='y')
            if i==0 and j==0:ax.legend(frameon=False,ncol=2,fontsize=9)
    save(fig,folder,'sqlite_same_node_runtime')

def report(data,out):
    fontroot=Path('/usr/share/fonts/truetype/dejavu')
    if (fontroot/'DejaVuSans.ttf').exists():
        pdfmetrics.registerFont(TTFont('Body',str(fontroot/'DejaVuSans.ttf')))
        pdfmetrics.registerFont(TTFont('BodyBold',str(fontroot/'DejaVuSans-Bold.ttf')))
        pdfmetrics.registerFontFamily('Body',normal='Body',bold='BodyBold',italic='Body',boldItalic='BodyBold')
        body,bold='Body','BodyBold'
    else:body,bold='Helvetica','Helvetica-Bold'
    styles={
        'title':ParagraphStyle('title',fontName=bold,fontSize=23,leading=28,textColor=colors.HexColor(NAVY),spaceAfter=10),
        'h1':ParagraphStyle('h1',fontName=bold,fontSize=18,leading=23,textColor=colors.HexColor(NAVY),spaceAfter=9),
        'h2':ParagraphStyle('h2',fontName=bold,fontSize=11.5,leading=15,textColor=colors.HexColor(NAVY),spaceBefore=8,spaceAfter=6),
        'body':ParagraphStyle('body',fontName=body,fontSize=9.4,leading=13.7,spaceAfter=7,textColor=colors.HexColor('#263C49')),
        'small':ParagraphStyle('small',fontName=body,fontSize=8,leading=11.2,spaceAfter=6,textColor=colors.HexColor(MUTED)),
        'cell':ParagraphStyle('cell',fontName=body,fontSize=8,leading=11,textColor=colors.HexColor('#263C49')),
        'head':ParagraphStyle('head',fontName=bold,fontSize=8,leading=11,textColor=colors.white)}
    flow=[]
    def p(text,style='body'):flow.append(Paragraph(text,styles[style]))
    def page():flow.append(PageBreak())
    def table(rows,widths):
        cells=[[Paragraph(escape(str(x)),styles['head' if i==0 else 'cell']) for x in row] for i,row in enumerate(rows)]
        tab=Table(cells,colWidths=widths,hAlign='LEFT',repeatRows=1)
        tab.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor(NAVY)),('VALIGN',(0,0),(-1,-1),'TOP'),
            ('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#EDF3F6'),colors.white]),
            ('LINEBELOW',(0,-1),(-1,-1),.5,colors.HexColor('#CFD9DF'))]))
        flow.extend([tab,Spacer(1,7)])
    def fig(name,w,h):flow.extend([Image(str(out/'figures'/(name+'.png')),width=w,height=h),Spacer(1,5)])
    def gain(d,w,c):return lookup(data['gains'],dataset='read1024',deployment=d,workload=w,phase='write',clients=c)
    def mean(d,w,s,c,m):return group(data,'read1024',d,w,s,c,m).mean()

    p('1,024-query SQL follow-up<br/>Storage benchmark analysis','title')
    p('Manoj Khatri / HPCAI-lab | Perlmutter | 2 October 2026','small')
    p('<b>Internal progress report.</b> This document summarizes saved experiments and exploratory analysis. It is not a publication manuscript. It supplements the original eight-run pilot report.')
    p('New milestone verified','h2')
    table([['New measurements','Experimental coverage','Validation'],
           ['180 measured + 36 warmups','3 SQL deployments x 2 workloads x 2 tiers x 3 client counts; 5 measured repetitions','216 full JSON trials; 437,184 latency samples; CSV, plans and source bytes reconciled']], [135,212,177])
    p('Every trial wrote 1,000,000 records in 1,000-record batches and ran 1,024 queries returning 100 rows each. Native SQLite, Shifter SQLite and PostgreSQL ran sequentially on one node per workload. The earlier InfluxDB pilots provide the 1,024-query reference; no new InfluxDB jobs were run.','small')
    p('What the follow-up establishes','h2')
    a=gain('sqlite-native','metadata',1);b=gain('sqlite-native','metadata',64)
    p(f'<b>1. Storage benefit still depends on concurrency.</b> Native SQLite metadata write throughput has a tmpfs/Lustre mean ratio of {a["ratio_of_means"]:.2f} at one client and {b["ratio_of_means"]:.2f} at 64. The 64-client geometric ratio is {b["geometric_ratio"]:.2f}, with a pointwise interval [{b["log_t95_low"]:.2f}, {b["log_t95_high"]:.2f}]; parity remains inside that interval.')
    p('<b>2. Equal query counts produce very short SQL read phases.</b> Across the 180 new measured trials, 120 read phases lasted less than 50 ms; all 60 trials with 64 clients fall in that group. Each 64-client worker issues only 16 queries. These observations describe short bursts.')
    ctx=lookup(data['context'],deployment='postgres',workload='metadata',storage='lustre',clients=64)
    p(f'<b>3. The reported read rate depends on experiment context.</b> PostgreSQL metadata reads on Lustre at 64 clients average {ctx["read1024_mean_qps"]/1000:.1f}k queries/s here versus {ctx["baseline64000_mean_qps"]/1000:.1f}k in the 64,000-query pilot. Allocation and some source files also changed, so this {ctx["baseline64000_over_read1024"]:.2f}x difference cannot be attributed solely to query count.')
    p('Metadata writes: tmpfs / Lustre mean throughput','h2')
    table([['Deployment','1 client','16 clients','64 clients']]+[
        [NAMES[d]]+[f'{gain(d,"metadata",c)["ratio_of_means"]:.2f}x' for c in CLIENTS] for d in SQL],[194,110,110,110])
    p('Adding these runs to the original baseline gives 420 distinct measured trials and 84 warmups. Separate profiling/overhead studies are outside these counts. The experiment sets are not pooled as independent replicates.','small')

    page();p('Write behavior and storage gains','h1')
    p('New SQL results only. Points are the five measured trials; lines are arithmetic means. Whiskers are pointwise 95% t intervals conditional on each allocation. Panel scales differ.','small')
    fig('write_throughput_1024',524,378)
    p('All plotted SQL write rates include the explicit final checkpoint. SQLite uses WAL/NORMAL; PostgreSQL records fsync, full_page_writes and synchronous_commit enabled. These are deployment-specific boundaries and do not establish matched durability.','small')
    fig('write_storage_gains_1024',524,175)
    p('Lower plot: geometric means of five repetition-matched tmpfs/Lustre ratios, with log-t intervals. Dashed line denotes parity. The intervals belong to geometric estimates, while page 1 reports ratios of arithmetic means.','small')

    page();p('Warm reads with equal query counts','h1')
    p('All panels use 1,024 queries per trial, with the same logical query-selection scheme and 100 returned rows per query. SQL panels are new; InfluxDB panels reuse the earlier pilot on separate allocations. Logarithmic y axes and panel-specific scales.','small')
    fig('read_throughput_1024',524,461)
    p('Points are individual trials; whiskers are pointwise 95% t intervals around mean queries/s. Query protocols and physical mappings differ: SQLite is embedded, PostgreSQL uses a local Unix socket, and InfluxDB uses HTTP/Flux with pivot/group/sort operations in this adapter. These figures characterize those implemented paths.','small')
    p('The read timer starts when the parent releases the ready workers and ends at the last worker completion. It excludes worker connection/setup, but includes release/wakeup, query selection, calls, and returned-key validation. Individual request latency times only the backend method. These two timing scopes must remain separate.','small')
    p('Equal counts remove the original count mismatch. They do not equalize phase duration, engine work, versions, server state, or durability; no isolated container-overhead or general database ranking follows from these panels.','small')

    page();p('Read length and allocation context','h1')
    p('Descriptive ratios of arithmetic mean throughput: new 1,024-query runs divided by earlier 64,000-query runs. Both sets have five measured repetitions per condition. No causal confidence interval is assigned to this cross-allocation contrast.','small')
    fig('read_count_context',524,330)
    p('PostgreSQL at 64 clients','h2')
    entries=[['Workload / tier','1,024 queries: kQ/s','64,000 queries: kQ/s','1,024 read phase (ms)']]
    for w in WORKLOADS:
        for s in STORAGE:
            r=lookup(data['context'],deployment='postgres',workload=w,storage=s,clients=64)
            entries.append([w+' / '+s,f'{r["read1024_mean_qps"]/1000:.2f}',f'{r["baseline64000_mean_qps"]/1000:.2f}',f'{r["read1024_mean_wall_s"]*1000:.2f}'])
    table(entries,[151,125,125,123])
    p('The metadata rates in the longer pilot are about 3.05-3.10x those in the new short runs; telemetry rates are about 2.12-2.15x. This is consistent with sensitivity to duration and synchronization, but it does not identify their individual contributions.','small')
    p('<b>Confounding remains:</b> the YAML settings differ only in read_queries, but allocations changed. SQLite also has different saved hashes for its controller, write coordinator and read worker; PostgreSQL differs in the factory, coordinator, lifecycle and controller. All changed filenames are listed in validation.json. Requests are a fixed deterministic prefix, not an equal-duration workload.','small')

    page();p('SQLite runtime comparison on the same node','h1')
    p('New runs: Shifter/native ratios of arithmetic mean throughput. Both deployments used the same source snapshot and node within each workload. They ran as separate sequential blocks in fixed order, so time/order effects remain. Ratios are descriptive.','small')
    fig('sqlite_same_node_runtime',524,291)
    table([['Deployment','Python','SQLite','Execution context'],
           ['Native','3.11.7','3.44.2','First deployment block'],
           ['Shifter','3.11.15','3.46.1','Second deployment block']], [133,90,90,211])
    p('Shifter mean write throughput is lower in 11 of 12 conditions. The largest observed gap is about 19.2% for telemetry on Lustre at 64 clients. Metadata writes on tmpfs at 64 clients are effectively equal in their means (ratio 1.0004), with substantial trial variability.','body')
    p('For reads, Shifter has higher means at 16 clients on tmpfs: about 11.0% for metadata and 16.6% for telemetry. These short phases are variable; native throughput CVs in those two conditions are 21.8% and 31.1%. This does not establish a container advantage.','body')
    p('This design controls the compute-node identity for the new SQL comparison. Isolating container overhead still requires matched interpreter/database builds and a counterbalanced or randomized runtime order within allocation.','small')

    page();p('Methods, provenance and remaining work','h1')
    table([['Workload','Slurm job / node','Order within allocation'],
           ['Metadata','59220317 / nid005765','SQLite native -> SQLite Shifter -> PostgreSQL'],
           ['Telemetry','59220318 / nid006712','SQLite native -> SQLite Shifter -> PostgreSQL']], [93,183,248])
    p('Source snapshot: 63c227bc25fc394d3a3752f3fe4f19ebc78fff65. All six source manifests agree, and every included source file matches its SHA-256 hash. The recorded commit is retained; a full Git object database is not part of the export. Native clients use Python 3.11.7; PostgreSQL is 16.12 in a pinned Shifter image.','small')
    p('Validation and evidence scope','h2')
    p('The new archive passes ZIP CRC and all 280 recorded file checksums. All 216 trial JSONs agree with CSV/config/plan identities, counts, finite timings, worker distributions, CPU sums and rate identities. Mean and p50/p95/p99 latencies were recomputed from 437,184 batch/query samples. Both logs show all 108 trials and the final completion marker. Original databases were not reopened.')
    p('The earlier export contains 288 CSV rows plus selected metadata, plans and hashes, without full raw latency samples or historical source bytes. Its internal consistency was rechecked, but its evidence depth differs from the new export. The earlier PostgreSQL/InfluxDB server-profiling experiments are outside this package.','small')
    p('Statistics','h2')
    p('Warmups are retained in the archive and excluded from estimates. Each condition has n=5 measured trials. Mean intervals use mean +/- t(0.975,4) x sample SD / sqrt(5). Storage/scaling/interaction contrasts use five repetition-matched log ratios and exponentiated t intervals. All intervals are pointwise and model-based, conditional on the observed allocation; serial dependence or drift may invalidate the nominal coverage.')
    p('No outliers were removed and no multiplicity-adjusted or omnibus significance claim is made. Latency summaries describe trial percentiles, not independent request replicates. Arithmetic mean throughput is the mean of per-trial rates. The old/new read ratios and native/Shifter ratios are descriptive and have no causal-effect interval.','small')
    p('Completed and next','h2')
    p('<b>Completed:</b> the 1,024-query SQL comparison, raw-data validation, source verification, uncertainty tables, figures and reproducible offline analysis. The original baseline remains a separate experiment set.')
    p('<b>Remaining:</b> mixed workloads; systematic 100,000 and 10 million record coverage; matched runtime builds and documented completion/durability policies; independent allocation repeats with execution-order control; hardware counters and aligned system-level measurements; final research synthesis and artifact preparation.')
    p('A focused same-allocation, randomized 1,024-versus-64,000-query comparison would resolve the duration question more directly if sustained read-throughput claims are needed. It should reuse one source/build snapshot and preserve the original query semantics.','small')
    p('Package inputs: read1024-results-20261002T200306Z-67f254d2.zip and ipdps-pilot-analysis.zip. Both originals are included unchanged. Tables expose full precision, provenance, trial variability and the exact source differences.','small')

    def footer(canvas,doc):
        canvas.setStrokeColor(colors.HexColor('#D6E0E6'));canvas.line(44,38,568,38)
        canvas.setFont(body,7);canvas.setFillColor(colors.HexColor(MUTED))
        canvas.drawString(44,26,'HPCAI-lab | Internal pilot analysis | 2 October 2026')
        canvas.drawRightString(568,26,str(doc.page))
    pdf=out/'read1024-comparison-report.pdf'
    doc=SimpleDocTemplate(str(pdf),pagesize=letter,rightMargin=44,leftMargin=44,topMargin=40,bottomMargin=47,
                          title='1,024-query SQL follow-up: internal pilot analysis',author='Manoj Khatri / HPCAI-lab')
    doc.build(flow,onFirstPage=footer,onLaterPages=footer)
    return pdf

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('new_archive',type=Path);p.add_argument('baseline_archive',type=Path)
    p.add_argument('--output',type=Path,default=Path('analysis-output'))
    a=p.parse_args();data=analyze(a.new_archive,a.baseline_archive,a.output)
    make_figures(data,a.output);print(report(data,a.output))
