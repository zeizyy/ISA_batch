import json
from kafka import KafkaConsumer

from elasticsearch import Elasticsearch

consumer = KafkaConsumer('event', group_id='listing-indexer', bootstrap_servers=['kafka:9092'])
es = Elasticsearch(['es'])
for message in consumer:
	event = json.loads(message.value.decode('utf8'))
	es.index(index='listing_index', doc_type='listing', id=event['event_id'],body=event)
	