#!/usr/bin/env python3
"""Produce pilot figures and a report from validated saved trials."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
from xml.sax.saxutils import escape

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image, PageBreak, KeepTogether

from analyze_pilots import analyze, DEPLOYMENTS, WORKLOADS, CLIENTS, PHASE_METRICS, descriptive

NAMES = {'sqlite-native':'SQLite native','sqlite-shifter':'SQLite Shifter',
         'postgres':'PostgreSQL','influx':'InfluxDB'}
COLORS={'lustre':'#286A9D','tmpfs':'#CB6A2C'}
DCOLORS={'sqlite-native':'#23688E','sqlite-shifter':'#699AB2','postgres':'#198C7B','influx':'#C36537'}
NAVY='#16334B'; MUTED='#546572'


def savefig(fig, directory, name):
    fig.savefig(directory/(name+'.png'),dpi=220,facecolor='white')
    fig.savefig(directory/(name+'.pdf'),facecolor='white')
    plt.close(fig)


def make_figures(data, out):
    figures=out/'figures'; figures.mkdir(exist_ok=True)
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.titlesize':11,
        'axes.labelsize':10,'axes.spines.right':False,'axes.spines.top':False,
        'axes.edgecolor':'#ABB5BD','grid.color':'#E1E7EB','grid.linewidth':.6,
        'pdf.fonttype':42,'axes.titleweight':'bold'})
    for phase,metric in PHASE_METRICS.items():
        fig,axes=plt.subplots(4,2,figsize=(10.8,10.0),layout='constrained')
        for i,d in enumerate(DEPLOYMENTS):
            for j,w in enumerate(WORKLOADS):
                ax=axes[i,j]
                for s,offset in (('lustre',-.08),('tmpfs',.08)):
                    means=[]; lo=[]; hi=[]
                    for k,c in enumerate(CLIENTS):
                        v=np.array([float(r[metric]) for r in data['groups'][(d,w,s,c)]])
                        if phase=='write':v=v/1000
                        ss=descriptive(v); means.append(ss['mean']);lo.append(ss['mean_t95_low']);hi.append(ss['mean_t95_high'])
                        ax.scatter(np.full(5,k+offset)+np.linspace(-.035,.035,5),v,s=12,alpha=.42,color=COLORS[s],zorder=3)
                    ax.errorbar(np.arange(3)+offset,means,yerr=[np.array(means)-lo,np.array(hi)-means],color=COLORS[s],
                                marker='o' if s=='lustre' else 's',ms=4,capsize=3,lw=1.4,label=s)
                ax.set_title(NAMES[d]+' - '+w,loc='left')
                ax.set_xticks(range(3),['1','16','64']);ax.set_xlabel('Clients')
                ax.set_ylabel('Write throughput (krecords/s)' if phase=='write' else 'Warm read throughput (queries/s)')
                ax.grid(axis='y');ax.set_xlim(-.35,2.35)
                if phase=='write':ax.set_ylim(bottom=0)
                else:ax.set_yscale('log')
                if i==0 and j==0:ax.legend(frameon=False,ncol=2,fontsize=9)
        fig.suptitle('Write completion throughput' if phase=='write' else 'Warm range-query throughput',fontsize=16,fontweight='bold',color=NAVY)
        savefig(fig,figures,phase+'_throughput')

    fig,axes=plt.subplots(2,2,figsize=(10.8,6.4),layout='constrained')
    for i,w in enumerate(WORKLOADS):
        for j,phase in enumerate(('write','read')):
            ax=axes[i,j]
            for idx,d in enumerate(DEPLOYMENTS):
                entries=sorted((r for r in data['storage_gains'] if r['deployment']==d and r['workload']==w and r['phase']==phase),key=lambda r:r['clients'])
                y=np.array([r['geometric_ratio'] for r in entries]); lower=np.array([r['log_t95_low'] for r in entries]);upper=np.array([r['log_t95_high'] for r in entries])
                ax.errorbar(np.arange(3)+(idx-1.5)*.06,y,yerr=[y-lower,upper-y],color=DCOLORS[d],marker=('o','s','^','D')[idx],ms=4,lw=1.3,capsize=2,label=NAMES[d])
            ax.axhline(1,color='#646B72',ls='--',lw=1)
            ax.set_title(w.capitalize()+' - '+phase,loc='left');ax.set_xticks(range(3),['1','16','64']);ax.set_xlabel('Clients')
            ax.set_ylabel('tmpfs / Lustre throughput');ax.set_yscale('log');ax.grid(axis='y');ax.set_xlim(-.3,2.3)
            ax.yaxis.set_major_formatter(FuncFormatter(lambda x,pos:f'{x:g}'))
            ax.yaxis.set_minor_formatter(FuncFormatter(lambda x,pos:f'{x:g}'))
    handles,labels=axes[0,0].get_legend_handles_labels();fig.legend(handles,labels,loc='outside lower center',ncol=4,frameon=False,fontsize=9)
    fig.suptitle('Storage gains within each deployment',fontsize=16,fontweight='bold',color=NAVY)
    savefig(fig,figures,'storage_gains')

    fig,axes=plt.subplots(1,2,figsize=(10.8,4.0),layout='constrained')
    for j,phase in enumerate(('write','read')):
        ax=axes[j]
        for wi,w in enumerate(WORKLOADS):
            for si,s in enumerate(('lustre','tmpfs')):
                entries=sorted((r for r in data['runtime'] if r['workload']==w and r['storage']==s and r['phase']==phase),key=lambda r:r['clients'])
                ax.plot(CLIENTS,[r['native_over_shifter'] for r in entries],marker='o' if w=='metadata' else 's',
                        linestyle='-' if w=='metadata' else '--',color=COLORS[s],label=w+' / '+s,lw=1.6)
        ax.axhline(1,color='#6B7279',ls=':',lw=1);ax.set_title(phase.capitalize(),loc='left')
        ax.set_xticks(CLIENTS);ax.set_xlabel('Clients');ax.set_ylabel('Native / Shifter throughput');ax.grid(axis='y')
    handles,labels=axes[0].get_legend_handles_labels();fig.legend(handles,labels,loc='outside lower center',ncol=2,frameon=False,fontsize=9)
    fig.suptitle('SQLite execution-environment comparison (descriptive)',fontsize=15,fontweight='bold',color=NAVY)
    savefig(fig,figures,'sqlite_runtime_ratios')

    fig,axes=plt.subplots(2,2,figsize=(10.8,6.5),layout='constrained')
    for i,w in enumerate(WORKLOADS):
        for j,s in enumerate(('lustre','tmpfs')):
            ax=axes[i,j]
            for idx,d in enumerate(DEPLOYMENTS):
                med=[];lo=[];hi=[]
                for c in CLIENTS:
                    vals=np.array([float(r['transaction_latency_p99_ms']) for r in data['groups'][(d,w,s,c)]])
                    med.append(np.median(vals));lo.append(min(vals));hi.append(max(vals))
                m=np.array(med)
                ax.errorbar(np.arange(3)+(idx-1.5)*.07,m,yerr=[m-lo,np.array(hi)-m],marker=('o','s','^','D')[idx],
                            color=DCOLORS[d],label=NAMES[d],lw=1.3,capsize=2,ms=4)
            ax.set_title(w.capitalize()+' - '+s,loc='left');ax.set_xticks(range(3),['1','16','64']);ax.set_xlabel('Clients')
            ax.set_ylabel('Median trial write p99 (ms)');ax.set_yscale('log');ax.grid(axis='y')
    handles,labels=axes[0,0].get_legend_handles_labels();fig.legend(handles,labels,loc='outside lower center',ncol=4,frameon=False,fontsize=9)
    fig.suptitle('Write tail latency (batch requests)',fontsize=16,fontweight='bold',color=NAVY)
    savefig(fig,figures,'write_tail_latency')

    fig,ax=plt.subplots(figsize=(8,3.6),layout='constrained')
    for s in ('lustre','tmpfs'):
        group=data['groups'][('influx','telemetry',s,16)]
        ax.plot(range(1,6),[float(r['read_queries_per_s']) for r in group],marker='o',color=COLORS[s],label=s,lw=1.5)
    ax.set_xticks(range(1,6));ax.set_xlabel('Measured repetition');ax.set_ylabel('Warm read throughput (queries/s)');ax.grid(axis='y');ax.legend(frameon=False)
    ax.set_title('InfluxDB telemetry reads at 16 clients',loc='left',color=NAVY)
    savefig(fig,figures,'influx_read_variability')


def make_report(data,out):
    font=Path(matplotlib.get_data_path())/'fonts'/'ttf'
    pdfmetrics.registerFont(TTFont('DV',str(font/'DejaVuSans.ttf')))
    pdfmetrics.registerFont(TTFont('DVB',str(font/'DejaVuSans-Bold.ttf')))
    pdfmetrics.registerFontFamily('DV',normal='DV',bold='DVB',italic='DV',boldItalic='DVB')
    styles={
        'title':ParagraphStyle('title',fontName='DVB',fontSize=24,leading=29,textColor=colors.HexColor(NAVY),spaceAfter=12),
        'h1':ParagraphStyle('h1',fontName='DVB',fontSize=17,leading=22,textColor=colors.HexColor(NAVY),spaceAfter=11),
        'h2':ParagraphStyle('h2',fontName='DVB',fontSize=11,leading=16,textColor=colors.HexColor(NAVY),spaceBefore=10,spaceAfter=5),
        'body':ParagraphStyle('body',fontName='DV',fontSize=9.8,leading=14.5,spaceAfter=8,textColor=colors.HexColor('#253D4C')),
        'small':ParagraphStyle('small',fontName='DV',fontSize=8.2,leading=11.8,spaceAfter=7,textColor=colors.HexColor(MUTED)),
        'cell':ParagraphStyle('cell',fontName='DV',fontSize=8.4,leading=11,textColor=colors.HexColor('#253D4C')),
        'head':ParagraphStyle('head',fontName='DVB',fontSize=8.4,leading=11,textColor=colors.white),
    }
    flow=[]
    def p(text,style='body'):flow.append(Paragraph(text,styles[style]))
    def table(rows,widths):
        converted=[[Paragraph(str(v),styles['head' if i==0 else 'cell']) for v in row] for i,row in enumerate(rows)]
        tab=Table(converted,colWidths=widths,hAlign='LEFT',repeatRows=1)
        tab.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),colors.HexColor(NAVY)),('VALIGN',(0,0),(-1,-1),'TOP'),
            ('LEFTPADDING',(0,0),(-1,-1),7),('RIGHTPADDING',(0,0),(-1,-1),7),('TOPPADDING',(0,0),(-1,-1),7),('BOTTOMPADDING',(0,0),(-1,-1),7),
            ('ROWBACKGROUNDS',(0,1),(-1,-1),[colors.HexColor('#F0F5F8'),colors.white]),('LINEBELOW',(0,-1),(-1,-1),.6,colors.HexColor('#D7E0E6'))]))
        flow.append(tab);flow.append(Spacer(1,8))
    def fig(name,width,height):flow.append(Image(str(out/'figures'/(name+'.png')),width=width,height=height));flow.append(Spacer(1,6))
    def page():flow.append(PageBreak())
    def gain(d,w,c,phase='write'):
        return next(r for r in data['storage_gains'] if r['deployment']==d and r['workload']==w and r['clients']==c and r['phase']==phase)
    def summary(d,w,s,c,metric):
        return next(r for r in data['summary'] if (r['deployment'],r['workload'],r['storage'],r['clients'],r['metric'])==(d,w,s,c,metric))

    p('Perlmutter storage benchmark<br/>Pilot analysis','title')
    p('Manoj Khatri / HPCAI-lab &nbsp;&nbsp; | &nbsp;&nbsp; 2 October 2026','small')
    p('<b>Storage placement and concurrency interact.</b> Native SQLite metadata writes gain 1.85x from tmpfs at one client, but the observed gain is 0.97x at 64 clients. PostgreSQL and InfluxDB follow different scaling patterns. These are pilot observations from one allocation per deployment and workload.')
    p('Verified analysis coverage','h2')
    table([['Deployments','Workloads and conditions','Observations'],
        ['SQLite native, SQLite Shifter, PostgreSQL, InfluxDB','Metadata and telemetry; Lustre and tmpfs; 1, 16, 64 clients','240 measured + 48 warmups; 48 conditions; 5 measured repetitions each']], [155,211,158])
    p('Each trial wrote 1,000,000 records in batches of 1,000. SQL pilots issued 64,000 range queries; InfluxDB issued 1,024. Each query returned 100 rows. PostgreSQL and InfluxDB used native Python clients with a Shifter server.','small')
    p('Write throughput: tmpfs / Lustre','h2')
    entries=[['Deployment','Metadata: 1 client','Metadata: 64','Telemetry: 1','Telemetry: 64']]
    for d in DEPLOYMENTS:
        entries.append([NAMES[d]]+[f"{gain(d,w,c)['ratio_of_arithmetic_means']:.2f}x" for w,c in (('metadata',1),('metadata',64),('telemetry',1),('telemetry',64))])
    table(entries,[132,98,98,98,98])
    p('Ratios above use arithmetic mean throughput. SQL write completion includes the final checkpoint. InfluxDB write completion ends at HTTP acknowledgement, so these ratios characterize each deployment; they do not establish a durability-matched engine ranking.','small')
    p('Three findings worth carrying forward','h2')
    p('<b>SQLite metadata:</b> native tmpfs write throughput falls from 442.47 to 192.20 krecords/s as clients increase from 1 to 64. Its 64-client storage-gain interval spans parity; the mean alone does not establish that Lustre is faster.')
    p('<b>Concurrency:</b> PostgreSQL metadata writes on Lustre reach 5.39x the one-client throughput at 64 clients. InfluxDB reaches 9.60x for metadata and 12.57x for telemetry under its HTTP-acknowledgement boundary.')
    p('<b>Read variability:</b> InfluxDB telemetry at 16 clients on Lustre has 39.2% sample CV. Two repetitions deliver about 192-196 queries/s, and three deliver 432-446. All five observations are retained.')
    p('Status: reproducible pilot analysis complete. Independent allocation repeats, matched comparison protocols, mixed workloads and placement-model evaluation remain.','small')

    page();p('Write throughput and concurrency','h1')
    p('Points are the five measured trials. Lines are arithmetic means; whiskers are pointwise 95% t intervals using within-allocation repetitions. Panel scales differ.','small')
    fig('write_throughput',524,485)
    p('<b>Measurement boundary:</b> SQLite and PostgreSQL include final checkpoint completion. InfluxDB includes HTTP acknowledgement and excludes later server shutdown. Both client-side preparation and execution behavior can influence the measured phase.','small')
    p('Increasing clients reduces metadata write throughput for SQLite. PostgreSQL shows its largest gains by 16 clients, with workload/storage-dependent behavior at 64. InfluxDB mean write throughput rises through 64 clients in both workloads.')

    page();p('Warm read throughput','h1')
    p('Logarithmic y axes show queries/s. Points are individual trials; whiskers are pointwise 95% t intervals around arithmetic means. These are warm reads after writes and validation, not cold storage reads.','small')
    fig('read_throughput',524,485)
    p('SQL pilots ran 64,000 queries per trial; InfluxDB ran 1,024. Query translation, client protocol, engine versions and physical schemas differ. Per-second normalization alone does not remove these protocol and duration differences.','small')
    p('Native SQLite metadata reads on tmpfs peak at 16 among the tested client counts: 152.22 kqueries/s versus 81.51 at 64. PostgreSQL metadata reads on Lustre rise from 6.08 to 200.12 kqueries/s between 1 and 64 clients.')

    page();p('Storage gains and interaction contrasts','h1')
    p('Plot points are geometric means of repetition-matched throughput ratios. Whiskers are pointwise 95% t intervals on log ratios. These estimates differ slightly from ratios of arithmetic means on page 1.','small')
    fig('storage_gains',524,311)
    p('How much does the storage gain change from 1 to 64 clients?','h2')
    p('Interaction contrast I = (tmpfs/Lustre at 64 clients) / (tmpfs/Lustre at 1 client). A value below 1 means the relative tmpfs advantage shrinks. The table uses geometric means of five block contrasts.','small')
    entries=[['Deployment','Metadata write I [95% CI]','Telemetry write I [95% CI]']]
    for d in DEPLOYMENTS:
        vals=[]
        for w in WORKLOADS:
            r=next(r for r in data['interactions'] if r['deployment']==d and r['workload']==w and r['phase']=='write' and r['clients']==64)
            vals.append(f"{r['geometric_ratio']:.3f} [{r['log_t95_low']:.3f}, {r['log_t95_high']:.3f}]")
        entries.append([NAMES[d],*vals])
    table(entries,[132,196,196])
    p('Native SQLite metadata has the strongest reduction: I = 0.525 [0.432, 0.639]. InfluxDB metadata is closer to unchanged: 0.978 [0.929, 1.029]. These are exploratory contrasts, not an omnibus interaction test or evidence across independent allocations.','small')

    page();p('Runtime comparison and unstable conditions','h1')
    fig('sqlite_runtime_ratios',524,194)
    p('<b>Runtime ratios are descriptive.</b> Native uses Python 3.11.7 / SQLite 3.44.2; Shifter uses Python 3.11.15 / SQLite 3.46.1. They ran on different nodes and allocations. Their core worker, workload and backend hashes match, but the controller changed. Observed differences cannot be assigned solely to container overhead.','small')
    p('Shifter mean write throughput is lower in 11 of 12 SQLite conditions; the largest deficit is 15.6%. Telemetry on Lustre at 64 clients is 1.3% higher. Reads show differences in both directions. No causal runtime confidence interval is reported.')
    fig('influx_read_variability',524,236)
    p('InfluxDB telemetry, Lustre, 16 clients: arithmetic mean 340.48 queries/s; sample SD 133.54; median 431.62. The mean is sensitive to the slow repetitions. No observation was removed and no cause is inferred from this archive alone.','small')
    p('Write tail latency also rises with concurrency. Native SQLite metadata median trial p99 on tmpfs increases from 2.91 ms (1 client) to 4,339.14 ms (64 clients). These are batch latencies, not per-record latencies; full tail-latency figures and tables are in the accompanying package.','small')

    page();p('Methods, provenance and next experiments','h1')
    p('Statistics and reproducibility','h2')
    p('Warmups are excluded by their recorded phase, leaving five observations per condition. Arithmetic means, sample SD, medians, ranges and CV are exported. Mean intervals use mean +/- t(0.975, 4) x SD/sqrt(5). No outliers are removed.')
    p('Storage and concurrency ratio intervals pair conditions by repetition within each allocation. For log ratios z, the reported geometric estimate is exp(mean(z)); interval endpoints are exp(mean(z) +/- t(0.975, 4) x SD(z)/sqrt(5)). Interaction contrasts subtract the 1-client log storage ratio from the 64-client ratio. Pairing uses the saved randomized repetition blocks; trials are not simultaneous.')
    p('<b>Uncertainty scope:</b> these model-based, pointwise intervals assume the five blocks adequately represent within-allocation variability. They are not adjusted for multiple comparisons, and do not cover between-node, between-day or between-allocation variation. Five trials in one job are not five independent allocations.')
    p('Source inventory','h2')
    entries=[['Deployment / workload','Slurm job','Node','Engine version']]
    for r in data['inventory']:
        ver=r['engine_version'].split(' (')[0]
        entries.append([NAMES[r['deployment']]+' / '+r['workload'],r['job_id'],r['hostname'],ver])
    table(entries,[220,84,98,122])
    p('Input: ipdps-pilot-analysis.zip. Full SHA-256 is recorded in validation.json. CSV values reconcile with saved configurations, plans and selected metadata. Source-hash manifests are compared, but source bytes and individual request samples are not in this archive. Server-profiling runs are separate and are not pooled into these throughput estimates.','small')
    p('Next experiments that address specific gaps','h2')
    p('<b>1.</b> Add independent allocation repeats with declared blocking and CPU affinity. <b>2.</b> For matched read comparisons, rerun SQL with the same query count and selection protocol as InfluxDB, or explicitly study query-duration sensitivity. <b>3.</b> Align write boundaries and document durability semantics before engine rankings. <b>4.</b> Use matching Python/SQLite builds for a controlled runtime comparison. <b>5.</b> Add mixed workloads and workload sizes, then evaluate a placement model on held-out allocations.')

    def footer(canvas,doc):
        canvas.setStrokeColor(colors.HexColor('#D8E1E7'));canvas.line(44,36,568,36)
        canvas.setFont('DV',8);canvas.setFillColor(colors.HexColor(MUTED))
        canvas.drawString(44,23,'HPCAI-lab  |  Perlmutter pilot analysis  |  2 October 2026')
        canvas.drawRightString(568,23,str(doc.page))
    doc=SimpleDocTemplate(str(out/'pilot-analysis-report.pdf'),pagesize=letter,leftMargin=44,rightMargin=44,topMargin=40,bottomMargin=48,
        title='Perlmutter storage benchmark - pilot analysis',author='Manoj Khatri / HPCAI-lab')
    doc.build(flow,onFirstPage=footer,onLaterPages=footer)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input',type=Path);parser.add_argument('--output',type=Path,default=Path('analysis-output'))
    args=parser.parse_args();data=analyze(args.input,args.output)
    make_figures(data,args.output);make_report(data,args.output)
    print('REPORT='+str(args.output/'pilot-analysis-report.pdf'))


if __name__=='__main__':main()
