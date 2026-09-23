from __future__ import annotations
import os
from pathlib import Path
BASE_DIR=Path(__file__).resolve().parent
DATA_DIR=BASE_DIR/'data'; LAKE_DIR=BASE_DIR/'lake'; BRONZE_DIR=LAKE_DIR/'bronze_data'; SILVER_DIR=LAKE_DIR/'silver'; GOLD_DIR=LAKE_DIR/'gold'; QUARANTINE_DIR=LAKE_DIR/'quarantine'; CHECKPOINT_DIR=LAKE_DIR/'checkpoints'; LOG_DIR=BASE_DIR/'logs'; REPORT_DIR=BASE_DIR
for _d in (DATA_DIR,BRONZE_DIR,SILVER_DIR,GOLD_DIR,QUARANTINE_DIR,CHECKPOINT_DIR,LOG_DIR): _d.mkdir(parents=True,exist_ok=True)
RAW_PARQUET=DATA_DIR/'yellow_tripdata_2024-01.parquet'; ZONE_LOOKUP_CSV=DATA_DIR/'taxi_zone_lookup.csv'
PG={'host':os.getenv('PGHOST','localhost'),'port':int(os.getenv('PGPORT','5432')),'dbname':os.getenv('PGDATABASE','taxi_stream'),'user':os.getenv('PGUSER',os.getenv('USER','postgres')),'password':os.getenv('PGPASSWORD','')}
SRC_SCHEMA='taxi_src'; DW_SCHEMA='taxi_dw'; SRC_TABLE=f'{SRC_SCHEMA}.trips'; ZONE_TABLE=f'{SRC_SCHEMA}.zones'
SOURCE_ROW_LIMIT=int(os.getenv('SOURCE_ROW_LIMIT','1200000'))
KAFKA_BOOTSTRAP=os.getenv('KAFKA_BOOTSTRAP','localhost:9092'); TOPIC_TRIPS='taxi.trips.raw'; CONSUMER_GROUP='bronze-writer'; NUM_PARTITIONS=3; REPLICATION_FACTOR=1
EXTRACT_CURSOR_COLUMN='trip_id'; EXTRACT_TIMESTAMP_COLUMN='last_updated'; PRODUCER_CHECKPOINT=CHECKPOINT_DIR/'producer_offset.json'
DUPLICATE_FRACTION=float(os.getenv('DUPLICATE_FRACTION','0.01')); CORRUPT_FRACTION=float(os.getenv('CORRUPT_FRACTION','0.005')); RANDOM_SEED=42
SCHEMA_VERSION='1.0'
EVENT_TIME_COLUMN='tpep_pickup_datetime'; BUSINESS_KEY='trip_key'; WATERMARK_DELAY='10 minutes'
TRIP_SCHEMA=[('trip_id','long',False),('trip_key','string',False),('VendorID','integer',True),('tpep_pickup_datetime','timestamp',False),('tpep_dropoff_datetime','timestamp',True),('passenger_count','integer',True),('trip_distance','double',True),('RatecodeID','integer',True),('store_and_fwd_flag','string',True),('PULocationID','integer',True),('DOLocationID','integer',True),('payment_type','integer',True),('fare_amount','double',True),('extra','double',True),('mta_tax','double',True),('tip_amount','double',True),('tolls_amount','double',True),('improvement_surcharge','double',True),('total_amount','double',True),('congestion_surcharge','double',True),('airport_fee','double',True),('last_updated','timestamp',True)]
MANDATORY_FIELDS=['trip_id','trip_key','tpep_pickup_datetime','PULocationID','DOLocationID']
NULLABLE_FILL={'passenger_count':1,'congestion_surcharge':0.0,'airport_fee':0.0,'RatecodeID':99,'store_and_fwd_flag':'N'}
VALIDATION_RULES={'fare_amount_min':0.0,'total_amount_min':0.0,'trip_distance_min':0.0,'trip_distance_max':200.0,'passenger_count_min':0,'passenger_count_max':8,'max_trip_hours':12,'min_location_id':1,'max_location_id':265}
SPARK_APP_NAME='nyc-taxi-streaming'
PROFILES={'v1':{'trigger_interval':'10 seconds','max_files_per_trigger':1,'shuffle_partitions':200,'broadcast_zone_join':False,'state_ttl':'10 minutes'},'v2':{'trigger_interval':'5 seconds','max_files_per_trigger':16,'shuffle_partitions':8,'broadcast_zone_join':True,'state_ttl':'10 minutes'}}
PRODUCER_PROFILES={'v1':{'batch_size':16384,'linger_ms':0,'compression_type':None},'v2':{'batch_size':65536,'linger_ms':50,'compression_type':'gzip'}}
ACTIVE_PROFILE=os.getenv('PIPELINE_PROFILE','v2')
def spark_conf(profile:str)->dict:
    p=PROFILES[profile]
    return {'spark.sql.shuffle.partitions':str(p['shuffle_partitions']),'spark.sql.session.timeZone':'UTC','spark.sql.streaming.schemaInference':'false','spark.driver.memory':os.getenv('SPARK_DRIVER_MEMORY','4g'),'spark.sql.streaming.minBatchesToRetain':'2','spark.sql.adaptive.enabled':'true'}
