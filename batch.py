import json
import time
from kafka import KafkaConsumer, SimpleProducer, KafkaClient

from elasticsearch import Elasticsearch

consumer = KafkaConsumer('event', group_id='listing-indexer', bootstrap_servers=['kafka:9092'])
kafka = KafkaClient('kafka:9092')
producer = SimpleProducer(kafka)
initial = {}
try:
	producer.send_messages(b'event', json.dumps(initial).encode('utf-8'))
except:
	time.sleep(2)
	producer.send_messages(b'event', json.dumps(initial).encode('utf-8'))

es = Elasticsearch(['es'])

for message in consumer:
	event = json.loads(message.value.decode('utf8'))
	if event:
		es.index(index='listing_index', doc_type='listing', id=event['event_id'],body=event)
		print(event," added to es!")
