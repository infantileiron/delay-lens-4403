from redis import Redis
from ..config.settings import REDIS_URL
QUEUE="delay-lens:runs"
QUEUED_SET="delay-lens:queued"
# Enqueue only when the id is not already waiting, so recovery and repeated
# submissions never produce duplicate queue entries.
_ENQUEUE="""
if redis.call('SADD',KEYS[1],ARGV[1])==1 then
 redis.call('RPUSH',KEYS[2],ARGV[1])
 return 1
end
return 0
"""
# Pop and drop the dedup marker atomically: a crash leaves the id in neither
# structure, so startup recovery can safely re-queue it.
_DEQUEUE="""
local item=redis.call('LPOP',KEYS[1])
if item then redis.call('SREM',KEYS[2],item) end
return item
"""
def client(): return Redis.from_url(REDIS_URL,decode_responses=True)
def enqueue(run_id): client().eval(_ENQUEUE,2,QUEUED_SET,QUEUE,str(run_id))
def dequeue(): return client().eval(_DEQUEUE,2,QUEUE,QUEUED_SET)
