from __future__ import annotations
import argparse, io
from pyspark.sql import functions as F
import config
from streaming_utils import get_logger,get_spark,pg_connect
log=get_logger('silver->gold')
PAYMENT_LABELS={1:'Credit card',2:'Cash',3:'No charge',4:'Dispute',5:'Unknown',6:'Voided trip'}
GOLD_TABLES={'gold_borough_hourly':{'table':f'{config.DW_SCHEMA}.gold_borough_hourly','cols':['pickup_borough','window_start','trips','total_revenue','avg_fare','avg_distance','avg_tip_pct'],'pk':['pickup_borough','window_start']},'gold_zone_flows':{'table':f'{config.DW_SCHEMA}.gold_zone_flows','cols':['pickup_zone','dropoff_zone','trips','avg_fare','avg_distance'],'pk':['pickup_zone','dropoff_zone']},'gold_payment_daily':{'table':f'{config.DW_SCHEMA}.gold_payment_daily','cols':['trip_date','payment_type','payment_label','trips','total_revenue'],'pk':['trip_date','payment_type']}}
def copy_upsert(pdf,spec):
    if pdf.empty:return 0
    cols=spec['cols']; table=spec['table']; conn=pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f'CREATE TEMP TABLE _stg (LIKE {table}) ON COMMIT DROP')
            buf=io.StringIO(); pdf[cols].to_csv(buf,index=False,header=False,na_rep=''); buf.seek(0)
            cur.copy_expert("COPY _stg (%s) FROM STDIN WITH (FORMAT csv, NULL '')" % ','.join(cols),buf)
            nonpk=[c for c in cols if c not in spec['pk']]; keys=','.join(spec['pk']); updates=','.join(f'{c}=EXCLUDED.{c}' for c in nonpk)
            cur.execute(f"INSERT INTO {table} ({','.join(cols)}) SELECT {','.join(cols)} FROM _stg ON CONFLICT ({keys}) DO UPDATE SET {updates}"); n=cur.rowcount
        conn.commit(); return n
    finally: conn.close()
def build_gold(batch,zones,broadcast_join):
    z=F.broadcast(zones) if broadcast_join else zones
    pu=z.select(F.col('location_id').alias('PULocationID'),F.col('borough').alias('pickup_borough'),F.col('zone').alias('pickup_zone'))
    do=z.select(F.col('location_id').alias('DOLocationID'),F.col('zone').alias('dropoff_zone'))
    x=batch.join(pu,'PULocationID','left').join(do,'DOLocationID','left')
    g1=x.withColumn('window_start',F.date_trunc('hour','tpep_pickup_datetime')).filter(F.col('pickup_borough').isNotNull()).groupBy('pickup_borough','window_start').agg(F.count('*').alias('trips'),F.round(F.sum('total_amount'),2).alias('total_revenue'),F.round(F.avg('fare_amount'),2).alias('avg_fare'),F.round(F.avg('trip_distance'),2).alias('avg_distance'),F.round(F.avg('tip_pct'),4).alias('avg_tip_pct'))
    g2=x.filter(F.col('pickup_zone').isNotNull()&F.col('dropoff_zone').isNotNull()).groupBy('pickup_zone','dropoff_zone').agg(F.count('*').alias('trips'),F.round(F.avg('fare_amount'),2).alias('avg_fare'),F.round(F.avg('trip_distance'),2).alias('avg_distance'))
    pairs=[]
    for k,v in PAYMENT_LABELS.items(): pairs.extend([F.lit(k),F.lit(v)])
    m=F.create_map(*pairs)
    g3=x.withColumn('trip_date',F.to_date('tpep_pickup_datetime')).withColumn('payment_label',F.coalesce(m[F.col('payment_type')],F.lit('Unknown'))).filter(F.col('payment_type').isNotNull()).groupBy('trip_date','payment_type','payment_label').agg(F.count('*').alias('trips'),F.round(F.sum('total_amount'),2).alias('total_revenue'))
    return {'gold_borough_hourly':g1,'gold_zone_flows':g2,'gold_payment_daily':g3}
def make_foreachbatch(zones,broadcast_join):
    def process(batch,batch_id):
        if batch.isEmpty():return
        for name,df in build_gold(batch,zones,broadcast_join).items():
            df.persist(); count=df.count(); df.write.mode('overwrite').parquet(str(config.GOLD_DIR/name)); loaded=copy_upsert(df.toPandas(),GOLD_TABLES[name]); log.info('%s batch %s rows=%s loaded=%s',name,batch_id,count,loaded); df.unpersist()
    return process
def run(profile):
    spark=get_spark(profile); zones=spark.read.option('header','true').csv(str(config.ZONE_LOOKUP_CSV)).select(F.col('LocationID').cast('int').alias('location_id'),F.col('Borough').alias('borough'),F.col('Zone').alias('zone'),F.col('service_zone')).persist()
    conn=pg_connect()
    try:
        with conn.cursor() as cur:
            for spec in GOLD_TABLES.values(): cur.execute(f'TRUNCATE {spec["table"]}')
        conn.commit()
    finally: conn.close()
    schema=spark.read.parquet(str(config.SILVER_DIR)).schema; stream=spark.readStream.schema(schema).option('maxFilesPerTrigger',config.PROFILES[profile]['max_files_per_trigger']).parquet(str(config.SILVER_DIR))
    q=stream.writeStream.foreachBatch(make_foreachbatch(zones,config.PROFILES[profile]['broadcast_zone_join'])).option('checkpointLocation',str(config.CHECKPOINT_DIR/'gold')).trigger(availableNow=True).start(); q.awaitTermination(); zones.unpersist(); spark.stop()
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--profile',default=config.ACTIVE_PROFILE,choices=['v1','v2']); a=ap.parse_args(); run(a.profile); return 0
if __name__=='__main__': raise SystemExit(main())
