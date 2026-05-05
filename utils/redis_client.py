import redis.asyncio as redis
import os

redis_client =redis.Redis(
    host='redis-13525.c283.us-east-1-4.ec2.redns.redis-cloud.com',
    port=13525,
    decode_responses=True,
    username="default",
    password="KvpwhWFD9n4yYa2fjWDVMsB29pSfusLe",
)

def publish(channel, message):
    redis_client.publish(channel, message)

def get_pubsub():
    return redis_client.pubsub()
