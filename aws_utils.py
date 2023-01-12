import io
from typing import List

import boto3

s3_client = boto3.Session(profile_name="core-commercial").client('s3')
s3_resource = boto3.Session(profile_name="core-commercial").resource("s3")


def list_keys(bucket: str, prefix: str) -> List[str]:
    """
    Returns all the object keys in a bucket.
    :param bucket: Bucket name to list.
    :param prefix: Limits the response to keys that begin with the specified prefix.
    :return: key for each object.
    """
    return [obj.key for obj in (s3_resource.Bucket(bucket).objects.filter(Prefix=str(prefix)).all())]


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
