from __future__ import annotations
import argparse, io
from pyspark.sql import functions as F
import config
from streaming_utils import get_logger, get_spark, pg_connect
log=get_logger('load->postgres')
FACT_TABLE=f'{config.DW_SCHEMA}.fact_trips'
FACT_COLS=['trip_id','trip_key','vendor_id','tpep_pickup_datetime','tpep_dropoff_datetime','passenger_count','trip_distance','pu_location_id','do_location_id','payment_type','fare_amount','tip_amount','total_amount','tip_pct']
MAP={'trip_id':'trip_id','trip_key':'trip_key','VendorID':'vendor_id','tpep_pickup_datetime':'tpep_pickup_datetime','tpep_dropoff_datetime':'tpep_dropoff_datetime','passenger_count':'passenger_count','trip_distance':'trip_distance','PULocationID':'pu_location_id','DOLocationID':'do_location_id','payment_type':'payment_type','fare_amount':'fare_amount','tip_amount':'tip_amount','total_amount':'total_amount','tip_pct':'tip_pct'}
def copy_into_fact(pdf):
    if pdf.empty:return 0
    conn=pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE TEMP TABLE _stg_fact (LIKE {FACT_TABLE}) ON COMMIT DROP')
            buf=io.StringIO(); pdf[FACT_COLS].to_csv(buf,index=False,header=False,na_rep=''); buf.seek(0)
            cur.copy_expert("COPY _stg_fact (%s) FROM STDIN WITH (FORMAT csv, NULL '')" % ','.join(FACT_COLS),buf)
            cur.execute(f"INSERT INTO {FACT_TABLE} ({','.join(FACT_COLS)}) SELECT {','.join(FACT_COLS)} FROM _stg_fact ON CONFLICT (trip_key) DO NOTHING")
            inserted=cur.rowcount
        conn.commit(); return inserted
    finally: conn.close()
def make_foreachbatch():
    def process(batch,batch_id):
        if batch.isEmpty():return
        selected=batch.select(*[F.col(k).alias(v) for k,v in MAP.items()]); pdf=selected.toPandas()
        for c in ['vendor_id','passenger_count','pu_location_id','do_location_id','payment_type']: pdf[c]=pdf[c].astype('Int64')
        inserted=copy_into_fact(pdf); log.info('Fact batch %s: %s rows, %s inserted',batch_id,len(pdf),inserted)
    return process
def run(profile):
    spark=get_spark(profile); conn=pg_connect()
    try:
        with conn.cursor() as cur: cur.execute(f'TRUNCATE {FACT_TABLE}')
        conn.commit()
    finally: conn.close()
    schema=spark.read.parquet(str(config.SILVER_DIR)).schema
    stream=spark.readStream.schema(schema).option('maxFilesPerTrigger',config.PROFILES[profile]['max_files_per_trigger']).parquet(str(config.SILVER_DIR))
    q=stream.writeStream.foreachBatch(make_foreachbatch()).option('checkpointLocation',str(config.CHECKPOINT_DIR/'fact_load')).trigger(availableNow=True).start(); q.awaitTermination()
    conn=pg_connect()
    try:
        with conn.cursor() as cur: cur.execute(f'SELECT count(*) FROM {FACT_TABLE}'); log.info('fact_trips rows=%s',cur.fetchone()[0])
    finally: conn.close()
    spark.stop()
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--profile',default=config.ACTIVE_PROFILE,choices=['v1','v2']); a=ap.parse_args(); run(a.profile); return 0
if __name__=='__main__': raise SystemExit(main())
