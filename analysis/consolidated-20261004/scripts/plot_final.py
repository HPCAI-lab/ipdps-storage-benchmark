#!/usr/bin/env python3
"""Create publication-quality graphics for an internal, explicitly bounded study."""
import argparse
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.colors import TwoSlopeNorm
import numpy as np
import pandas as pd

LABEL={'sqlite-native':'SQLite native','sqlite-shifter':'SQLite Shifter','postgres':'PostgreSQL','influx':'InfluxDB'}
ORDER=list(LABEL)
COLORS={'lustre':'#3869a8','tmpfs':'#cc6828'}
MET={'write':'completion_throughput_records_s','read':'read_queries_per_s'}
plt.rcParams.update({'font.family':'DejaVu Sans','font.size':10,'axes.titlesize':11,
 'axes.labelsize':10,'xtick.labelsize':9,'ytick.labelsize':9,'axes.spines.top':False,
 'axes.spines.right':False,'savefig.dpi':190,'pdf.fonttype':42,'axes.axisbelow':True})

def save(fig,folder,name):
    fig.savefig(folder/(name+'.png'),bbox_inches='tight',facecolor='white')
    fig.savefig(folder/(name+'.pdf'),bbox_inches='tight',facecolor='white',metadata={'Creator':'Reproducible IPDPS analysis'})
    plt.close(fig)

def main(root):
    tables=root/'tables';out=root/'figures';out.mkdir(exist_ok=True)
    block=pd.read_csv(tables/'sql_allocation_means.csv')
    ratios=pd.read_csv(tables/'sql_ratio_intervals.csv',dtype={'condition':str})
    for phase,metric in MET.items():
        fig,axes=plt.subplots(2,3,figsize=(9.3,5.4),sharex=True,constrained_layout=True)
        for row,w in enumerate(('metadata','telemetry')):
            for col,d in enumerate(ORDER[:3]):
                ax=axes[row,col];g=block[(block.workload==w)&(block.deployment==d)]
                for tier in COLORS:
                    s=g[g.storage==tier]
                    for _,a in s.groupby('allocation_block'):
                        a=a.set_index('clients').loc[[1,16,64]]
                        ax.plot([0,1,2],a[metric]/1000,color=COLORS[tier],lw=.8,alpha=.25)
                    a=s.groupby('clients')[metric].mean().loc[[1,16,64]]
                    ax.plot([0,1,2],a/1000,'o-',color=COLORS[tier],lw=1.8,label=tier)
                ax.set_title(f'{w.capitalize()} | {LABEL[d]}')
                ax.set_ylim(bottom=0);ax.grid(axis='y',alpha=.18)
                ax.set_xticks([0,1,2],[1,16,64])
                if col==0:ax.set_ylabel('Write krecords/s' if phase=='write' else 'Read kqueries/s')
                if row==1:ax.set_xlabel('Clients (categorical spacing)')
        axes[0,0].legend(frameon=False,fontsize=9)
        save(fig,out,'sql_'+phase+'_throughput')

    fig,axes=plt.subplots(2,3,figsize=(9.3,5.25),sharex=True,sharey=True,constrained_layout=True)
    for row,w in enumerate(('metadata','telemetry')):
        for col,d in enumerate(ORDER[:3]):
            ax=axes[row,col]
            g=ratios[(ratios.workload==w)&(ratios.deployment==d)&(ratios.phase=='write')&(ratios.kind=='storage')].set_index('condition').loc[['1','16','64']]
            m=g.geometric_ratio.to_numpy();lo=g.ci95_low.to_numpy();hi=g.ci95_high.to_numpy()
            ax.axhline(1,color='#777777',linestyle='--',lw=1)
            ax.errorbar([0,1,2],m,yerr=[m-lo,hi-m],fmt='o-',color='#12676b',capsize=4,lw=1.8)
            for x,y in enumerate(m):ax.annotate(f'{y:.2f}',(x,hi[x]),xytext=(0,6),textcoords='offset points',ha='center',fontsize=9)
            ax.set_title(f'{w.capitalize()} | {LABEL[d]}');ax.set_xticks([0,1,2],[1,16,64]);ax.set_ylim(.80,2.07)
            ax.grid(axis='y',alpha=.18)
            if col==0:ax.set_ylabel('tmpfs / Lustre throughput')
            if row==1:ax.set_xlabel('Clients (categorical spacing)')
    save(fig,out,'sql_write_storage_ratios')

    summary=pd.read_csv(tables/'condition_summary.csv')
    mixed=summary[summary.study=='mixed-pilots']
    fig,axes=plt.subplots(2,2,figsize=(9.3,5.3),sharex=True,constrained_layout=True)
    for ax,d in zip(axes.flat,ORDER):
        for tier in COLORS:
            g=mixed[(mixed.deployment==d)&(mixed.storage==tier)].set_index('clients').loc[[1,16,64]]
            ax.errorbar([0,1,2],g[MET['write']+'_mean']/1000,yerr=g[MET['write']+'_sd']/1000,
                fmt='o-',capsize=4,color=COLORS[tier],label=tier)
        ax.set_title(LABEL[d]);ax.set_xticks([0,1,2],[1,16,64]);ax.tick_params(labelbottom=True);ax.set_ylim(bottom=0);ax.grid(axis='y',alpha=.18)
        ax.set_ylabel('Write krecords/s');ax.set_xlabel('Clients (categorical spacing)')
    axes[0,0].legend(frameon=False)
    save(fig,out,'mixed_write_throughput')

    # One 10M observation / mean from the designated 1M context. No interval.
    cal=pd.read_csv(tables/'size_calibration_context.csv')
    rowkeys=[(w,d) for w in ('metadata','telemetry','mixed') for d in ORDER]
    colkeys=[('lustre',1),('lustre',64),('tmpfs',1),('tmpfs',64)]
    fig,axes=plt.subplots(1,2,figsize=(9.3,6.0),constrained_layout=True)
    for ax,phase in zip(axes,('write','read')):
        matrix=np.array([[cal[(cal.phase==phase)&(cal.workload==w)&(cal.deployment==d)&(cal.storage==s)&(cal.clients==c)].descriptive_10M_over_1M.iloc[0] for s,c in colkeys] for w,d in rowkeys])
        im=ax.imshow(np.log2(matrix),cmap='PuOr_r',norm=TwoSlopeNorm(vmin=-2.6,vcenter=0,vmax=2.6),aspect='auto')
        for (r,c),v in np.ndenumerate(matrix):ax.text(c,r,f'{v:.2f}',ha='center',va='center',fontsize=9,color='white' if abs(np.log2(v))>1.7 else '#17232c')
        ax.set_xticks(range(4),['Lustre\n1','Lustre\n64','tmpfs\n1','tmpfs\n64'])
        ax.set_yticks(range(12),[f'{w if w=="mixed" else w[:4]} | {LABEL[d]}' for w,d in rowkeys] if phase=='write' else [])
        ax.set_title(f'{phase.capitalize()} throughput: 10M / 1M')
        for y in (3.5,7.5):ax.axhline(y,color='white',lw=2)
    cb=fig.colorbar(im,ax=axes,shrink=.75,pad=.03,ticks=[-2,-1,0,1,2]);cb.ax.set_yticklabels(['0.25','0.5','1','2','4']);cb.set_label('Throughput ratio (log color scale)')
    save(fig,out,'size_calibration_context')

    system=pd.read_csv(tables/'system_measurements.csv')
    fig,axes=plt.subplots(1,2,figsize=(9.3,6),constrained_layout=True)
    for ax,key,title,fmt in zip(axes,['node_busy_percent','node_iowait_percent'],['Node busy (%)','Node iowait (%)'],['.2f','.3f']):
        matrix=np.array([[system[(system.workload==w)&(system.deployment==d)&(system.storage==s)&(system.clients==c)][key].iloc[0] for s,c in colkeys] for w,d in rowkeys])
        im=ax.imshow(matrix,cmap='Blues',vmin=0,vmax=matrix.max(),aspect='auto')
        for (r,c),v in np.ndenumerate(matrix):ax.text(c,r,format(v,fmt),ha='center',va='center',fontsize=9,color='white' if v>matrix.max()*.63 else '#17232c')
        ax.set_title(title);ax.set_xticks(range(4),['Lustre\n1','Lustre\n64','tmpfs\n1','tmpfs\n64'])
        ax.set_yticks(range(12),[f'{w if w=="mixed" else w[:4]} | {LABEL[d]}' for w,d in rowkeys] if key=='node_busy_percent' else [])
        for y in (3.5,7.5):ax.axhline(y,color='white',lw=2)
        fig.colorbar(im,ax=ax,shrink=.75,pad=.025)
    save(fig,out,'system_cpu_iowait')

    env=pd.read_csv(tables/'sqlite_environment_ratios.csv')
    fig,axes=plt.subplots(1,2,figsize=(9.3,5.4),sharey=True,constrained_layout=True)
    keys=[(w,s,c) for w in ('metadata','telemetry') for s in ('lustre','tmpfs') for c in (1,16,64)]
    for ax,phase in zip(axes,('write','read')):
        for y,(w,s,c) in enumerate(keys):
            r=env[(env.phase==phase)&(env.workload==w)&(env.storage==s)&(env.clients==c)].iloc[0]
            ax.errorbar(r.geometric_ratio,y,xerr=[[r.geometric_ratio-r.ci95_low],[r.ci95_high-r.geometric_ratio]],fmt='o',capsize=3,color=COLORS[s])
        ax.axvline(1,color='#777777',ls='--');ax.set_title(phase.capitalize());ax.grid(axis='x',alpha=.18)
        ax.set_xlabel('Shifter / native throughput')
    axes[0].set_yticks(range(len(keys)),[f'{w[:4]} | {s} | {c}' for w,s,c in keys]);axes[0].invert_yaxis()
    save(fig,out,'sqlite_environment_ratios')
    print('PASS: seven figures saved as PNG and vector PDF')

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1]);main(p.parse_args().root)
