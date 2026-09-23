from __future__ import annotations
import hashlib,json,logging
import config
def get_logger(name):
    logger=logging.getLogger(name)
    if logger.handlers: return logger
    logger.setLevel(logging.INFO); fmt=logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s')
    sh=logging.StreamHandler(); sh.setFormatter(fmt); logger.addHandler(sh)
    fh=logging.FileHandler(config.LOG_DIR/'pipeline.log'); fh.setFormatter(fmt); logger.addHandler(fh); logger.propagate=False
    return logger
def trip_business_key(vendor_id,pickup,pu_loc,do_loc,total,fare):
    s='|'.join(str(x) for x in (vendor_id,pickup,pu_loc,do_loc,total,fare)); return hashlib.md5(s.encode('utf-8')).hexdigest()
def build_trip_struct():
    from pyspark.sql.types import StructType,StructField,LongType,IntegerType,DoubleType,StringType,TimestampType
    m={'long':LongType(),'integer':IntegerType(),'double':DoubleType(),'string':StringType(),'timestamp':TimestampType()}
    fields=[StructField(n,m[t],nullable) for n,t,nullable in config.TRIP_SCHEMA]; fields.append(StructField('_corrupt_record',StringType(),True)); return StructType(fields)
def get_spark(profile):
    from pyspark.sql import SparkSession
    b=SparkSession.builder.appName(config.SPARK_APP_NAME).master('local[*]')
    for k,v in config.spark_conf(profile).items(): b=b.config(k,v)
    s=b.getOrCreate(); s.sparkContext.setLogLevel('WARN'); return s
def ensure_topic(log=None):
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError
    admin=KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP,client_id='capstone-admin')
    try:
        admin.create_topics([NewTopic(config.TOPIC_TRIPS,num_partitions=config.NUM_PARTITIONS,replication_factor=config.REPLICATION_FACTOR)])
        (log or logging.getLogger()).info('Kafka topic created: %s',config.TOPIC_TRIPS)
    except TopicAlreadyExistsError: (log or logging.getLogger()).info('Kafka topic already exists: %s',config.TOPIC_TRIPS)
    finally: admin.close()
def read_producer_offset():
    if config.PRODUCER_CHECKPOINT.exists(): return int(json.loads(config.PRODUCER_CHECKPOINT.read_text()).get('last_trip_id',0))
    return 0
def write_producer_offset(last_id):
    config.PRODUCER_CHECKPOINT.parent.mkdir(parents=True,exist_ok=True); config.PRODUCER_CHECKPOINT.write_text(json.dumps({'last_trip_id':int(last_id)},indent=2))
def pg_connect(dbname=None):
    import psycopg2
    params=dict(config.PG)
    if dbname: params['dbname']=dbname
    if not params.get('password'): params.pop('password',None)
    return psycopg2.connect(**params)
def df_to_md(df):
    cols=list(df.columns); head='| '+' | '.join(map(str,cols))+' |'; sep='| '+' | '.join('---' for _ in cols)+' |'; rows=['| '+' | '.join(map(str,r))+' |' for r in df.itertuples(index=False,name=None)]; return '\n'.join([head,sep,*rows])
