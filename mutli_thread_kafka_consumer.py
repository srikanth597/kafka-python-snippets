import sys
from kafka import KafkaConsumer
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
from kafka import TopicPartition
import certifi
from kafka.structs import OffsetAndMetadata
from orjson import orjson
from multiprocessing import Process
import logging
import timeit
load_dotenv()

broker = os.getenv('broker')
source_topic = os.getenv('source_topic')
dest_topic = os.getenv('dest_topic')
access_key = os.getenv('access_key')
secret_key = os.getenv('secret_key')
consumer_group_id = os.getenv('consumer_group_id')
last_offsets = orjson.loads(os.getenv('last_offsets'))


def post_message(key, value, headers, producer,logger):
    # dest_topic='failed-v0-v1-messages-test'
    producer.send(dest_topic, value=str(value).encode('utf-8'),
                  key=str(key).encode('utf-8'), headers=headers)
    # print('==>',producedRecord.get())
    logger.info('***DONE PRODUCING****')


def process_message(key, value, headers, producer,logger):
    try:
        # stupidValue = str(processed_value).endswith("s") or str(processed_value).endswith("w") or key==b'\x00\x00\x00\x01'
        stupidValue = key == b'\x00\x00\x00\x01'
        if(stupidValue):
            logger.info("some stupid value wasted my whole day %s", key.decode("utf-8"))
        else:
            processed_key = key.decode("utf-8")
            processed_value = value.decode("utf-8")
            parsed_data = orjson.loads(processed_value)
            errorMsg = parsed_data["errorMessage"]
            if (str(errorMsg).startswith("empty String") or str(errorMsg).startswith("Cannot deserialize")):
                actual_value = parsed_data["parsedXml"] if parsed_data.get(
                    "parsedXml") != None else parsed_data["rawXml"]
                logger.info('PRODUCING message which has this error %s', errorMsg)
                post_message(key=processed_key, value=actual_value,
                             headers=headers, producer=producer,logger=logger)
    except Exception as e:
        logger.info("Error while grabbing the XML message %s", e)


def consume(pid):
    logging.basicConfig(
        filename=f'logs/app{pid}.log', level=logging.INFO, filemode='w')
    start=timeit.default_timer()
    try:
        producer = KafkaProducer(
            bootstrap_servers=broker,
            sasl_mechanism='PLAIN',
            sasl_plain_username=access_key,
            sasl_plain_password=secret_key,
            security_protocol='SASL_SSL',
            client_id="Failed-msg-test",
            ssl_cafile=certifi.where(),
        )

        consumer = KafkaConsumer(
            # source_topic,
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
    except Exception as e:
        print(e, pid)
        sys.exit(1)

    topicPartition = TopicPartition(source_topic, pid)
    consumer.assign([topicPartition])

    # ******Logic for seeking from particular offsets
    # consumer.seek(topicPartition, 15847)
    count = 0
    logging.info("STARTING....%s", consumer)
    try:
        for message in consumer:
            if message.offset < int(last_offsets[message.partition]):
                logging.info("Partition: %s :: Offset: %s :: key=%s",
                             message.partition, message.offset, message.key)
                process_message(key=message.key, value=message.value,
                                headers=message.headers, producer=producer,logger=logging)
                commitedPartition = TopicPartition(
                    source_topic, partition=message.partition)
                consumer.commit(
                    {commitedPartition: OffsetAndMetadata(message.offset+1, None)})
            else:
                logging.info('Crossed the mentioned boundaries in the settings Partition:: %s, offset:: %s :::PID:::: %s',
                             message.partition, message.offset, os.getpid())
            count = count+1
    except Exception as e:
        logging.info('Unknown Exception but still continuing %s', e)
    finally:
        end=timeit.default_timer()
        logging.info("%s~ messages processed", count-1)
        logging.info("%s~ seconds took", end-start)
        consumer.close()
        producer.close()


if __name__ == '__main__':
    for i in range(30):
        process=Process(target=consume,args=(i,))
        process.start()