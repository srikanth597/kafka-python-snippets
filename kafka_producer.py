import certifi
from kafka import KafkaProducer
from dotenv import load_dotenv
import json
import os
load_dotenv()

broker = os.getenv('broker')
source_topic ="infoaxs.shipment.tracking-events-v1"

access_key = os.getenv('access_key')
secret_key = os.getenv('secret_key')

def post_message(key,value):
    producer = KafkaProducer(
        bootstrap_servers=broker,
        sasl_mechanism='PLAIN',
        sasl_plain_username=access_key,
        sasl_plain_password=secret_key,
        security_protocol='SASL_SSL',
        client_id="Failed-msg-test",
        # ssl_certfile="./cert.ca"
        ssl_cafile=certifi.where(),
        # consumer_timeout_ms=5000,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    print('**********PRODUCING*********')
    print(value)
    producedRecord=producer.send(source_topic,value=value,key=str(key).encode('utf-8'))
    print('==>',producedRecord.get())
    print('***DONE PRODUCING****')


post_message(904823284,{"shipmentId":904823284})