import io
import logging
from typing import List
from concurrent.futures import ThreadPoolExecutor, wait

import boto3
import botocore

import consts

config = botocore.config.Config(max_pool_connections=50)
s3_client = boto3.Session(profile_name="core-commercial").client('s3', config=config)
s3_resource = boto3.Session(profile_name="core-commercial").resource("s3", config=config)
kinesis = boto3.Session(profile_name="core-commercial").client('kinesis')


def list_keys(bucket: str, prefix: str) -> List[str]:
    """
    Returns all the object keys in a bucket.
    :param bucket: Bucket name to list.
    :param prefix: Limits the response to keys that begin with the specified prefix.
    :return: key for each object.
    """
    return [obj.key for obj in (s3_resource.Bucket(bucket).objects.filter(Prefix=str(prefix)).all())]


def list_obj_in_batches(bucket: str, prefix: str, max_batch_size: int = 128_000_000):
    batch_size = 0
    result = []

    for obj in s3_resource.Bucket(bucket).objects.filter(Prefix=str(prefix)).all():
        if batch_size + obj.size >= max_batch_size:
            if len(result) == 0:
                raise ValueError(f"max_batch_size too small.")
            yield result, batch_size
            result = []
            batch_size = obj.size
            result.append(obj)
        else:
            result.append(obj)
            batch_size += obj.size


def get_file_contents_in_batches(bucket: str, prefix: str, max_batch_size: int = 128_000_000, n_threads: int = 50):
    result = []

    with ThreadPoolExecutor(max_workers=n_threads) as executor:
        for objs, batch_size in list_obj_in_batches(bucket=bucket, prefix=prefix, max_batch_size=max_batch_size):
            logging.info(f"Downloading {len(objs)} xml files.")
            wait([executor.submit(lambda: result.append(obj.get()["Body"].read().decode("utf-8"))) for obj in objs])

            logging.info(f"{len(result)} xml files download successfully for {bucket}/{prefix}.")
            yield result
            result = []


def download_mem(bucket_name: str, key: str) -> io.BytesIO:
    """
    Download a S3 file as a binary stream.
    :param bucket_name: The name of the bucket where the file is stored.
    :param key: Path to the file to download.
    :return: binary stream with the file's data.
    """
    stream = io.BytesIO()
    s3_resource.Object(bucket_name, key).download_fileobj(stream)
    stream.seek(0)  # Change stream position to the start.
    return stream


def upload_mem(file, bucket, key):
    s3_client.put_object(Body=file, Bucket=bucket, Key=key)


def upload_fileobj(buffer, bucket, key):
    s3_client.upload_fileobj(buffer, bucket, key)


if __name__ == '__main__':
    for data in get_file_contents_in_batches(consts.BUCKET, f"{consts.TRGT_DIR}/ACOUSTIC/year=2023/month=01/day=01/", 30_000):
        breakpoint()