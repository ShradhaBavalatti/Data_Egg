from __future__ import annotations
import glob,json,sys
import pandas as pd
import config
from streaming_utils import get_logger,pg_connect,df_to_md
log=get_logger('validate')
def count_lines(pattern):
    return sum(sum(1 for _ in open(f,encoding='utf-8')) for f in glob.glob(pattern,recursive=True))
def parquet_count(path):
    files=glob.glob(str(path)+'/**/*.parquet',recursive=True)
    if not files:return 0
    import pyarrow.parquet as pq
    return sum(pq.ParquetFile(f).metadata.num_rows for f in files)
def pg_count(cur,table):
    cur.execute(f'SELECT count(*) FROM {table}'); return int(cur.fetchone()[0])
def load_stats(name):
    p=config.CHECKPOINT_DIR/name; return json.loads(p.read_text()) if p.exists() else {}
def main():
    producer=load_stats('producer_stats.json'); consumer=load_stats('consumer_stats.json')
    conn=pg_connect();
    try:
        with conn.cursor() as cur:
            pg={t:pg_count(cur,t) for t in [config.SRC_TABLE,f'{config.DW_SCHEMA}.fact_trips',f'{config.DW_SCHEMA}.gold_borough_hourly',f'{config.DW_SCHEMA}.gold_zone_flows',f'{config.DW_SCHEMA}.gold_payment_daily']}
    finally: conn.close()
    bronze=count_lines(str(config.BRONZE_DIR/'**/*.json')); silver=parquet_count(config.SILVER_DIR); quarantine=count_lines(str(config.QUARANTINE_DIR/'**/*.json')); dedup=bronze-silver-quarantine
    counts=pd.DataFrame([['Source',pg[config.SRC_TABLE]],['Bronze',bronze],['Silver',silver],['Quarantine',quarantine],['Deduplicated',dedup],['fact_trips',pg[f'{config.DW_SCHEMA}.fact_trips']],['Gold borough hourly',pg[f'{config.DW_SCHEMA}.gold_borough_hourly']],['Gold zone flows',pg[f'{config.DW_SCHEMA}.gold_zone_flows']],['Gold payment daily',pg[f'{config.DW_SCHEMA}.gold_payment_daily']]],columns=['Layer','Records'])
    checks=[]
    if silver:
        df=pd.read_parquet(config.SILVER_DIR)
        checks=[('no_null_trip_key',not df.trip_key.isna().any()),('no_duplicate_trip_key',not df.trip_key.duplicated().any()),('no_null_pickup_location',not df.PULocationID.isna().any()),('no_negative_fares',not (df.fare_amount<0).any()),('no_null_passenger_count',not df.passenger_count.isna().any()),('valid_pickup_locations',bool(df.PULocationID.between(1,265).all())),('valid_dropoff_locations',bool(df.DOLocationID.between(1,265).all())),('silver_le_source',silver<=pg[config.SRC_TABLE]),('fact_equals_silver',pg[f'{config.DW_SCHEMA}.fact_trips']==silver),('reconciliation',silver+quarantine<=bronze and dedup>=0)]
    else: checks=[('reconciliation',silver+quarantine<=bronze and dedup>=0)]
    overall=all(v for _,v in checks); qdf=pd.DataFrame(checks,columns=['Check','Result']); kafka=pd.DataFrame([['producer_rps',producer.get('throughput_rps',0)],['consumer_rps',consumer.get('throughput_rps',0)],['final_lag',consumer.get('total_lag',0)],['peak_lag',consumer.get('peak_lag',0)],['avg_lag',consumer.get('avg_lag',0)]],columns=['Metric','Value'])
    report='# Validation Report\n\n## Layer counts\n'+df_to_md(counts)+'\n\n## Reconciliation\n`Bronze = Silver + Quarantine + Deduplicated`\n\n'+f'`{bronze} = {silver} + {quarantine} + {dedup}`\n\n## Silver quality checks\n'+df_to_md(qdf)+'\n\n## Kafka monitoring\n'+df_to_md(kafka)+f'\n\n**Overall: {"PASS" if overall else "FAIL"}**\n'
    (config.REPORT_DIR/'validation_report.md').write_text(report); log.info('Validation %s', 'PASS' if overall else 'FAIL'); return 0 if overall else 1
if __name__=='__main__': raise SystemExit(main())
