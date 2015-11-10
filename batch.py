import json
import time
from kafka import KafkaConsumer, SimpleProducer, KafkaClient

from elasticsearch import Elasticsearch

consumer = KafkaConsumer('event', group_id='listing-indexer', bootstrap_servers=['kafka:9092'])
kafka = KafkaClient('kafka:9092')
producer = SimpleProducer(kafka)
initial = {'description': 'initial', 'creator_id': 1, 'location': 'initial', 'start_time': '2015-11-09T17:01:16Z', 'event_id': 1, 'name': 'initial'}
try:
	producer.send_messages(b'event', json.dumps(initial).encode('utf-8'))
except:
	time.sleep(3)
	producer.send_messages(b'event', json.dumps(initial).encode('utf-8'))

es = Elasticsearch(['es'])

for message in consumer:
	event = json.loads(message.value.decode('utf8'))
	if event:
		es.index(index='listing_index', doc_type='listing', id=event['event_id'],body=event)
		print(event," added to es!")
