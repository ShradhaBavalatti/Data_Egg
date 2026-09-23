from __future__ import annotations
import argparse,json
from pyspark.sql import functions as F
import config
from streaming_utils import get_logger,get_spark,build_trip_struct
log=get_logger('bronze->silver')
def parse_bronze(spark,profile):
 schema=build_trip_struct(); raw=(spark.readStream.format('text').option('recursiveFileLookup','true').option('maxFilesPerTrigger',config.PROFILES[profile]['max_files_per_trigger']).load(str(config.BRONZE_DIR)))
 parsed=raw.select('value',F.from_json('value',schema,{'mode':'PERMISSIVE','columnNameOfCorruptRecord':'_corrupt_record'}).alias('d'))
 return parsed.select('d.*',F.col('value').alias('raw_json'))
def with_validation(df):
 r=config.VALIDATION_RULES; dur=(F.unix_timestamp('tpep_dropoff_datetime')-F.unix_timestamp('tpep_pickup_datetime'))
 corrupt=F.col('_corrupt_record').isNotNull()|F.col('tpep_pickup_datetime').isNull()
 reason=(F.when(F.col('_corrupt_record').isNotNull(),'corrupt_record')
 .when(F.col('trip_key').isNull()|F.col('trip_id').isNull(),'null_business_identifier')
 .when(F.col('tpep_pickup_datetime').isNull(),'null_event_time')
 .when(F.col('PULocationID').isNull()|F.col('DOLocationID').isNull(),'null_location')
 .when((F.col('fare_amount')<r['fare_amount_min'])|(F.col('total_amount')<r['total_amount_min']),'negative_amount')
 .when((F.col('trip_distance')<r['trip_distance_min'])|(F.col('trip_distance')>r['trip_distance_max']),'distance_out_of_range')
 .when((F.col('passenger_count')<r['passenger_count_min'])|(F.col('passenger_count')>r['passenger_count_max']),'passenger_out_of_range')
 .when((F.col('PULocationID')<r['min_location_id'])|(F.col('PULocationID')>r['max_location_id'])|(F.col('DOLocationID')<r['min_location_id'])|(F.col('DOLocationID')>r['max_location_id']),'location_id_out_of_range')
 .when(F.col('tpep_dropoff_datetime').isNotNull()&(F.col('tpep_dropoff_datetime')<F.col('tpep_pickup_datetime')),'dropoff_before_pickup')
 .when(F.col('tpep_dropoff_datetime').isNotNull()&(dur>r['max_trip_hours']*3600),'trip_too_long')
 .when(~F.year('tpep_pickup_datetime').isin([2024]),'out_of_period')
 .otherwise(F.lit(None).cast('string')))
 return df.withColumn('is_corrupt',corrupt).withColumn('reject_reason',reason)
def clean_valid(df):
 for c,v in config.NULLABLE_FILL.items(): df=df.withColumn(c,F.coalesce(F.col(c),F.lit(v)))
 df=df.withColumn('tip_pct',F.when(F.col('fare_amount')>0,F.col('tip_amount')/F.col('fare_amount')).otherwise(F.lit(0.0)))
 df=df.withColumn('year',F.year(config.EVENT_TIME_COLUMN)).withColumn('month',F.month(config.EVENT_TIME_COLUMN))
 cols=[n for n,_,_ in config.TRIP_SCHEMA if n!='_corrupt_record']+['tip_pct','year','month']; return df.select(*cols)
def run(profile,once):
 spark=get_spark(profile); validated=with_validation(parse_bronze(spark,profile)); valid=validated.filter((~F.col('is_corrupt'))&F.col('reject_reason').isNull()); rejected=validated.filter(F.col('is_corrupt')|F.col('reject_reason').isNotNull())
 silver=clean_valid(valid).withWatermark(config.EVENT_TIME_COLUMN,config.WATERMARK_DELAY).dropDuplicates([config.BUSINESS_KEY,config.EVENT_TIME_COLUMN])
 trig={'availableNow':True} if once else {'processingTime':config.PROFILES[profile]['trigger_interval']}
 sw=(silver.writeStream.format('parquet').outputMode('append').option('path',str(config.SILVER_DIR)).option('checkpointLocation',str(config.CHECKPOINT_DIR/'silver')).partitionBy('year','month'))
 qw=(rejected.select('raw_json',F.coalesce('reject_reason',F.lit('schema_violation')).alias('reject_reason'),F.current_timestamp().alias('quarantined_at')).writeStream.format('json').outputMode('append').option('path',str(config.QUARANTINE_DIR)).option('checkpointLocation',str(config.CHECKPOINT_DIR/'quarantine')))
 q1=sw.trigger(**trig).start(); q2=qw.trigger(**trig).start()
 if once: q1.awaitTermination(); q2.awaitTermination(); _capture_progress(q1,profile); _capture_progress(q2,profile)
 else: spark.streams.awaitAnyTermination()
 log.info('Bronze to Silver complete'); spark.stop()
def _capture_progress(query,profile):
 prog=[x for x in query.recentProgress if x.get('numInputRows',0)>0]
 if not prog:return
 durations=[x.get('durationMs',{}).get('triggerExecution',0) for x in prog]; rows=sum(x.get('numInputRows',0) for x in prog); total=sum(durations); stats={'profile':profile,'batches':len(prog),'input_rows':int(rows),'total_batch_ms':int(total),'avg_batch_latency_ms':round(total/len(prog),1),'processed_rows_per_sec':round(rows/(total/1000),1) if total else 0}; (config.CHECKPOINT_DIR/f'silver_stats_{profile}.json').write_text(json.dumps(stats,indent=2)); log.info('micro-batch stats %s',stats)
def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--profile',default=config.ACTIVE_PROFILE,choices=['v1','v2']); ap.add_argument('--once',action='store_true'); a=ap.parse_args(); run(a.profile,a.once); return 0
if __name__=='__main__': raise SystemExit(main())
