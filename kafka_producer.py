from __future__ import annotations
import argparse,json,random,time
from datetime import datetime
import config
from streaming_utils import get_logger,pg_connect,ensure_topic,read_producer_offset,write_producer_offset
log=get_logger('producer')
SELECT_COLS=['trip_id','trip_key','vendor_id','tpep_pickup_datetime','tpep_dropoff_datetime','passenger_count','trip_distance','ratecode_id','store_and_fwd_flag','pu_location_id','do_location_id','payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount','congestion_surcharge','airport_fee','last_updated']
JSON_KEYS=['trip_id','trip_key','VendorID','tpep_pickup_datetime','tpep_dropoff_datetime','passenger_count','trip_distance','RatecodeID','store_and_fwd_flag','PULocationID','DOLocationID','payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount','congestion_surcharge','airport_fee','last_updated']
def make_producer(profile):
 from kafka import KafkaProducer
 s=config.PRODUCER_PROFILES[profile]; kw={'bootstrap_servers':config.KAFKA_BOOTSTRAP,'key_serializer':lambda v:v.encode(),'value_serializer':lambda v:v.encode(),'batch_size':s['batch_size'],'linger_ms':s['linger_ms'],'acks':'all','retries':3}
 if s['compression_type']: kw['compression_type']=s['compression_type']
 return KafkaProducer(**kw)
def row_to_json(row):
 out={}
 for k,c in zip(JSON_KEYS,SELECT_COLS):
  v=row[c]; out[k]=v.isoformat(sep=' ') if isinstance(v,datetime) else v
 return json.dumps(out,default=str,separators=(',',':'))
def corrupt(payload,rng):
 mode=rng.choice(['truncate','null_key','bad_type']);
 if mode=='truncate': return payload[:max(1,len(payload)//2)]
 obj=json.loads(payload)
 if mode=='null_key': obj['PULocationID']=None
 else: obj['fare_amount']='not-a-number'
 return json.dumps(obj,separators=(',',':'))
def run(profile,limit,reset):
 ensure_topic(log); rng=random.Random(config.RANDOM_SEED)
 if reset: write_producer_offset(0)
 offset=read_producer_offset(); producer=make_producer(profile); conn=pg_connect(); cur=conn.cursor(name='extract_cursor'); cur.itersize=10000
 where=f'WHERE trip_id > %s'; params=[offset]
 if limit is not None: where+=' AND trip_id <= %s'; params.append(offset+limit)
 cur.execute(f'SELECT {", ".join(SELECT_COLS)} FROM {config.SRC_TABLE} {where} ORDER BY trip_id',params)
 start=time.perf_counter(); unique=dups=corrupt_n=0; last=offset
 try:
  for row in cur:
   d=dict(zip(SELECT_COLS,row)); payload=row_to_json(d); key=str(d['trip_key']); producer.send(config.TOPIC_TRIPS,key=key,value=payload); unique+=1; last=int(d['trip_id'])
   if rng.random()<config.DUPLICATE_FRACTION: producer.send(config.TOPIC_TRIPS,key=key,value=payload); dups+=1
   if rng.random()<config.CORRUPT_FRACTION: producer.send(config.TOPIC_TRIPS,key=key,value=corrupt(payload,rng)); corrupt_n+=1
   if unique%100000==0: log.info('published %s',unique)
  producer.flush()
 finally:
  producer.close(); cur.close(); conn.close()
 write_producer_offset(last); sec=time.perf_counter()-start; stats={'unique':unique,'duplicates':dups,'corrupt':corrupt_n,'total':unique+dups+corrupt_n,'seconds':sec,'throughput_rps':(unique+dups+corrupt_n)/sec if sec else 0,'last_trip_id':last}; log.info('Producer complete: %s',stats); return stats
def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--profile',default=config.ACTIVE_PROFILE,choices=['v1','v2']); ap.add_argument('--limit',type=int); ap.add_argument('--reset',action='store_true'); a=ap.parse_args(); s=run(a.profile,a.limit,a.reset); (config.CHECKPOINT_DIR/'producer_stats.json').write_text(json.dumps(s,indent=2)); return 0
if __name__=='__main__': raise SystemExit(main())
