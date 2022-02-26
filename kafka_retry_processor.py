from kafka import KafkaConsumer

from dotenv import load_dotenv
import os
from kafka import TopicPartition
import certifi
import logging

load_dotenv()

logging.basicConfig(
    filename=f'app_test.log', level=logging.INFO, filemode='w')

broker = os.getenv('broker')
source_topic = ""

access_key = os.getenv('access_key')
secret_key = os.getenv('secret_key')

if __name__ == '__main__':
    consumer = KafkaConsumer(
        # source_topic,
        group_id="get-messages-for-debugging",
        bootstrap_servers=broker,
        sasl_mechanism='PLAIN',
        sasl_plain_username=access_key,
        sasl_plain_password=secret_key,
        security_protocol='SASL_SSL',
        ssl_cafile=certifi.where(),
        consumer_timeout_ms=20000,
        # value_deserializer=lambda m: orjson.loads(str(m.decode("utf-8"))),
        enable_auto_commit=True,  # If false Then manage offset commit on your own
        auto_offset_reset='earliest',
        auto_commit_interval_ms=3000
    )
    # ******Logic for seeking from particular offsets
    topicPartition = TopicPartition(source_topic, 1)
    consumer.assign([topicPartition])
    consumer.seek(topicPartition, 1220144)

    count = 0
    print("START", consumer)
    try:
        for message in consumer:
            if message.offset > 1262797:
                consumer.close()
                logging.info("exiting")
                SystemExit(1)
            logging.info(dict(message.headers)["retry_reason"])
            SystemExit(1)
            logging.info("Partition: {} :: Offset: {} :: key={}".format(
                message.partition, message.offset, message.key))
            count = count+1
    except Exception as e:
        print('Unknown Exception but still continuing', e)
    finally:
        print(count, "~ messages processed")
        consumer.close()
