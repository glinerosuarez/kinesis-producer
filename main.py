import argparse
import logging
import time
from typing import List

from aws_utils import kinesis
from consts import STREAM_NAME
from xml_generator import XmlGenerator

logging.basicConfig(
  format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
  datefmt='%Y-%m-%d %H:%M:%S',
  level=logging.INFO,
  handlers=[logging.FileHandler("producer.log")]
)


def produce_batch(type_of_reading: str, xml_batch: List[bytes]) -> None:
    logging.info(f"Sending {len(xml_batch)} records to stream.")
    response = kinesis.put_records(
        StreamName=STREAM_NAME,
        Records=[{"Data": xml_data, "PartitionKey": type_of_reading} for xml_data in xml_batch],
    )

    # inspect records to check for any that failed to be written to Kinesis
    for i, record_response in enumerate(response['Records']):
        error_code = record_response.get('ErrorCode')
        if error_code:
            err_msg = record_response['ErrorMessage']
            logging.error(f"Failed to produce xml_file because {err_msg}")
        else:
            seq = record_response['SequenceNumber']
            shard = record_response['ShardId']
            logging.info(f"Produced xml sequence {seq} to Shard {shard}")


def main(type_of_reading: str):
    logging.info(f'Starting xml {type_of_reading} Producer.')
    last_ts = float('inf')  # Init last_ts to a large number so the first batch will be produced immediately.

    for ts, xml_batch in XmlGenerator(type_of_reading).get_batches():
        seconds_to_wait = (ts - last_ts) / 1_000 if last_ts < ts else 0
        logging.info(f"waiting {seconds_to_wait}s before producing next batch.")
        time.sleep(seconds_to_wait)
        try:
            logging.info(f"Sending records for ts: {ts}")
            produce_batch(type_of_reading, xml_batch)
        except Exception as e:
            logging.error({'message': 'Error producing record', 'error': str(e), 'record': xml_batch})

        last_ts = ts


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("type_of_reading", type=str)
    args = parser.parse_args()
    main(args.type_of_reading)
