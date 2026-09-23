from __future__ import annotations
import argparse,json,shutil,subprocess,sys,time
import pandas as pd
import config
from streaming_utils import get_logger,df_to_md
log=get_logger('benchmark'); PY=sys.executable
def sh(args): subprocess.run([PY,*args],cwd=str(config.BASE_DIR),check=True)
def clean_lake():
    for d in [config.BRONZE_DIR,config.SILVER_DIR,config.GOLD_DIR,config.QUARANTINE_DIR,config.CHECKPOINT_DIR]: shutil.rmtree(d,ignore_errors=True); d.mkdir(parents=True,exist_ok=True)
def stats(name):
    p=config.CHECKPOINT_DIR/name; return json.loads(p.read_text()) if p.exists() else {}
def run_profile(profile,limit):
    clean_lake(); start=time.perf_counter(); sh(['kafka_producer.py','--profile',profile,'--limit',str(limit),'--reset']); sh(['kafka_consumer.py','--max-idle','6','--flush-every','20000']); sh(['spark_bronze_to_silver.py','--profile',profile,'--once']); elapsed=time.perf_counter()-start
    ps=stats('producer_stats.json'); cs=stats('consumer_stats.json'); ss=stats(f'silver_stats_{profile}.json')
    return {'profile':profile,'producer_rps':ps.get('throughput_rps',0),'consumer_rps':cs.get('throughput_rps',0),'consumer_lag':cs.get('total_lag',0),'spark_avg_batch_ms':ss.get('avg_batch_latency_ms',0),'spark_rows_per_sec':ss.get('processed_rows_per_sec',0),'silver_rows':ss.get('input_rows',0),'end_to_end_sec':elapsed}
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--limit',type=int,default=100000); a=ap.parse_args(); rows=[run_profile('v1',a.limit),run_profile('v2',a.limit)]; raw=pd.DataFrame(rows); raw.to_csv(config.REPORT_DIR/'performance_metrics.csv',index=False)
    def gain(v1,v2,higher):
        if not v1 or not v2:return '—'
        return f'{(v2/v1-1)*100:.1f}%' if higher else f'{(v1/v2-1)*100:.1f}%'
    v1,v2=rows; summary=pd.DataFrame([['Producer throughput',v1['producer_rps'],v2['producer_rps'],gain(v1['producer_rps'],v2['producer_rps'],True)],['Spark throughput',v1['spark_rows_per_sec'],v2['spark_rows_per_sec'],gain(v1['spark_rows_per_sec'],v2['spark_rows_per_sec'],True)],['Spark avg batch latency',v1['spark_avg_batch_ms'],v2['spark_avg_batch_ms'],gain(v1['spark_avg_batch_ms'],v2['spark_avg_batch_ms'],False)],['End-to-end wall-clock',v1['end_to_end_sec'],v2['end_to_end_sec'],gain(v1['end_to_end_sec'],v2['end_to_end_sec'],False)],['Final consumer lag',v1['consumer_lag'],v2['consumer_lag'],gain(v1['consumer_lag'],v2['consumer_lag'],False)]],columns=['Metric','V1 baseline','V2 optimised','V2 gain'])
    report='# Performance Report\n\n## Workload\nFixed bounded source slice; identical workload and clean lake for each profile.\n\n## Head-to-head\n'+df_to_md(summary)+'\n\n## What changed in V2\n| Lever | V1 | V2 |\n|---|---:|---:|\n| Shuffle partitions | 200 | 8 |\n| Max files/trigger | 1 | 16 |\n| Broadcast zone join | No | Yes |\n| Kafka batch size | 16 KB | 64 KB |\n| Kafka linger | 0 ms | 50 ms |\n| Kafka compression | none | gzip |\n\n## Raw metrics\n'+df_to_md(raw)+'\n'
    (config.REPORT_DIR/'performance_report.md').write_text(report); print(summary.to_string(index=False)); return 0
if __name__=='__main__': raise SystemExit(main())
