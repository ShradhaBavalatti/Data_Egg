from __future__ import annotations
import argparse,json,time
from datetime import datetime,timezone
from pathlib import Path
import config
from streaming_utils import get_logger
log=get_logger('consumer')
def make_consumer():
 from kafka import KafkaConsumer
 c=KafkaConsumer(config.TOPIC_TRIPS,bootstrap_servers=config.KAFKA_BOOTSTRAP,group_id=config.CONSUMER_GROUP,auto_offset_reset='earliest',enable_auto_commit=False,consumer_timeout_ms=1000,value_deserializer=lambda v:v.decode('utf-8'),max_poll_records=2000)
 return c
def bronze_path(ts):
 d=config.BRONZE_DIR/f'year={ts.year:04d}'/f'month={ts.month:02d}'/f'day={ts.day:02d}'; d.mkdir(parents=True,exist_ok=True); return d
def flush(buffer,seq):
 if not buffer:return seq
 now=datetime.now(timezone.utc); path=bronze_path(now)/f'part-{now:%H%M%S}-{seq:05d}.json'; path.write_text('\n'.join(buffer)+'\n',encoding='utf-8'); log.info('Bronze file %s (%s records)',path.relative_to(config.BRONZE_DIR.parent),len(buffer)); return seq+1
def lag_snapshot(consumer):
 from kafka import TopicPartition
 parts=consumer.partitions_for_topic(config.TOPIC_TRIPS) or set(); tps=[TopicPartition(config.TOPIC_TRIPS,p) for p in parts]
 if not tps:return {'partitions':0,'total_lag':0,'end_offsets':{}}
 ends=consumer.end_offsets(tps); lags=[]
 for tp in tps:
  try: pos=consumer.position(tp)
  except Exception: pos=0
  lags.append(max(0,ends[tp]-pos))
 return {'partitions':len(tps),'total_lag':int(sum(lags)),'end_offsets':{str(tp.partition):int(ends[tp]) for tp in tps}}
def run(max_idle,flush_every):
 c=make_consumer(); buffer=[]; seq=0; consumed=0; last_msg=time.monotonic(); start=time.perf_counter(); peak_lag=avg_samples=[]
 try:
  while True:
   polled=c.poll(timeout_ms=1000,max_records=2000)
   if polled:
    for _,msgs in polled.items():
     for msg in msgs: buffer.append(msg.value); consumed+=1
    last_msg=time.monotonic()
    if len(buffer)>=flush_every: seq=flush(buffer,seq); buffer=[]; c.commit()
    snap=lag_snapshot(c); peak_lag.append(snap['total_lag'])
   elif time.monotonic()-last_msg >= (max_idle if consumed else max_idle*2): break
  if buffer: seq=flush(buffer,seq); buffer=[]; c.commit()
  snap=lag_snapshot(c)
 finally: c.close()
 sec=time.perf_counter()-start; stats={'consumed':consumed,'files':seq,'seconds':sec,'throughput_rps':consumed/sec if sec else 0,**snap,'peak_lag':max(peak_lag) if peak_lag else 0,'avg_lag':sum(peak_lag)/len(peak_lag) if peak_lag else 0}; log.info('Consumer complete: %s',stats); return stats
def main():
 ap=argparse.ArgumentParser(); ap.add_argument('--max-idle',type=float,default=8); ap.add_argument('--flush-every',type=int,default=20000); a=ap.parse_args(); s=run(a.max_idle,a.flush_every); (config.CHECKPOINT_DIR/'consumer_stats.json').write_text(json.dumps(s,indent=2)); return 0
if __name__=='__main__': raise SystemExit(main())
