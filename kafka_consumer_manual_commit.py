import timeit
from kafka import KafkaConsumer
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
from kafka import TopicPartition
import certifi
from kafka.structs import OffsetAndMetadata
from orjson import orjson

load_dotenv()

broker = os.getenv('broker')
source_topic = os.getenv('source_topic')
dest_topic = os.getenv('dest_topic')
access_key = os.getenv('access_key')
secret_key = os.getenv('secret_key')
consumer_group_id = os.getenv('consumer_group_id')
last_offsets = orjson.loads(os.getenv('last_offsets'))


def post_message(key, value, headers):
    # dest_topic='failed-v0-v1-messages-test'
    producer = KafkaProducer(
        bootstrap_servers=broker,
        sasl_mechanism='PLAIN',
        sasl_plain_username=access_key,
        sasl_plain_password=secret_key,
        security_protocol='SASL_SSL',
        client_id="Failed-msg-test",
        ssl_cafile=certifi.where(),
    )
    producer.send(dest_topic, value=str(value).encode('utf-8'),
                  key=str(key).encode('utf-8'), headers=headers)
    # print('==>',producedRecord.get())
    print('***DONE PRODUCING****')


def process_message(key, value, headers):
    try:
        # stupidValue = str(processed_value).endswith("s") or str(processed_value).endswith("w") or key==b'\x00\x00\x00\x01'
        stupidValue = key == b'\x00\x00\x00\x01'
        if(stupidValue):
            print("some stupid value wasted my whole day", key.decode("utf-8"))
        else:
            processed_key = key.decode("utf-8")
            processed_value = value.decode("utf-8")
            parsed_data = orjson.loads(processed_value)
            errorMsg = parsed_data["errorMessage"]
            if (str(errorMsg).startswith("empty String") or str(errorMsg).startswith("Cannot deserialize")):
                actual_value = parsed_data["parsedXml"] if parsed_data.get(
                    "parsedXml") != None else parsed_data["rawXml"]
                print('PRODUCING message which has this error', errorMsg)
                post_message(key=processed_key,
                             value=actual_value, headers=headers)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    start = timeit.default_timer()
    consumer = KafkaConsumer(
        source_topic,
        group_id=consumer_group_id,
        bootstrap_servers=broker,
        sasl_mechanism='PLAIN',
        sasl_plain_username=access_key,
        sasl_plain_password=secret_key,
        security_protocol='SASL_SSL',
        ssl_cafile=certifi.where(),
        consumer_timeout_ms=20000,
        # value_deserializer=lambda m: orjson.loads(str(m.decode("utf-8"))),
        enable_auto_commit=False,  # If false Then manage offset commit on your own
        auto_offset_reset='earliest',
        # auto_commit_interval_ms=3000
    )
    # ******Logic for seeking from particular offsets
    # topicPartition = TopicPartition(source_topic, 4)
    # consumer.assign([topicPartition])
    # consumer.seek(topicPartition, 15847)

    count = 0
    print("START", consumer)
    try:
        for message in consumer:
            if message.offset < int(last_offsets[message.partition]):
                print("Partition: {} :: Offset: {} :: key={}".format(
                    message.partition, message.offset, message.key))
                process_message(key=message.key,
                                value=message.value, headers=message.headers)
                commitedPartition = TopicPartition(
                    source_topic, partition=message.partition)
                consumer.commit(
                    {commitedPartition: OffsetAndMetadata(message.offset+1, None)})
                count = count+1
            else:
                print('Crossed the mentioned boundaries in the settings')
    except Exception as e:
        print('Unknown Exception but still continuing', e)
    finally:
        print(count, "~ messages processed")
        end = timeit.default_timer()
        print("time took: ", end-start)
        consumer.close()
