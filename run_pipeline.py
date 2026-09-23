from __future__ import annotations
import argparse,shutil,subprocess,sys,time
import config
from streaming_utils import get_logger,ensure_topic
log=get_logger('run_pipeline'); PY=sys.executable
def stage(title,args):
    start=time.perf_counter(); log.info('START %s',title); r=subprocess.run([PY,*args],cwd=str(config.BASE_DIR));
    if r.returncode: log.error('FAILED %s',title); raise SystemExit(r.returncode)
    log.info('DONE %s in %.1fs',title,time.perf_counter()-start)
def clean_lake():
    for d in [config.BRONZE_DIR,config.SILVER_DIR,config.GOLD_DIR,config.QUARANTINE_DIR,config.CHECKPOINT_DIR]: shutil.rmtree(d,ignore_errors=True); d.mkdir(parents=True,exist_ok=True)
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--profile',default=config.ACTIVE_PROFILE,choices=['v1','v2']); ap.add_argument('--limit',type=int); ap.add_argument('--load-source',action='store_true'); ap.add_argument('--keep-lake',action='store_true'); a=ap.parse_args(); start=time.perf_counter(); log.info('NYC Taxi streaming pipeline profile=%s',a.profile)
    if a.load_source: stage('load source',['00_load_source.py'])
    if not a.keep_lake: clean_lake()
    ensure_topic(log); prod=['kafka_producer.py','--profile',a.profile,'--reset'];
    if a.limit is not None: prod += ['--limit',str(a.limit)]
    stage('produce',prod); stage('consume',['kafka_consumer.py','--max-idle','8','--flush-every','20000']); stage('bronze-to-silver',['spark_bronze_to_silver.py','--profile',a.profile,'--once']); stage('load fact',['load_to_postgres.py','--profile',a.profile]); stage('silver-to-gold',['spark_silver_to_gold.py','--profile',a.profile]); stage('validate',['validate.py']); log.info('TOTAL %.1fs',time.perf_counter()-start); return 0
if __name__=='__main__': raise SystemExit(main())
