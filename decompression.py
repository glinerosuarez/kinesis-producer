import argparse
import io
import json
import logging
import multiprocessing
import tarfile
from concurrent.futures import ThreadPoolExecutor, wait
from typing import List

from aws_utils import download_mem, upload_mem, list_obj_in_batches
from consts import READING_TYPES, YEARS, MONTHS, DAYS, BUCKET, SRC_DIR

logging.basicConfig(
  format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
  datefmt='%Y-%m-%d %H:%M:%S',
  level=logging.INFO,
  handlers=[logging.FileHandler(f"unpacker_{multiprocessing.current_process().pid}.log")]
)

unpacked_files = 0
xml_files = 0


def validate_arg(arg, valid_vals):
    if arg not in valid_vals:
        raise ValueError(f"Invalid argument: {arg}, must be one of {valid_vals}")


def unpack_tar(batch: List[str], trgt_file: str) -> None:
    global unpacked_files, xml_files
    buffer = io.BytesIO()

    for obj in batch:
        tar_obj = download_mem(BUCKET, obj.key)
        tar = tarfile.open(fileobj=tar_obj)

        for m_info in tar.getmembers():  # Iterate over files inside the tar file.
            member = tar.extractfile(m_info)
            member.seek(0)
            json_obj = json.dumps(
                dict(payload=member.read().decode("utf-8"), tenant_id="bhp", partition_id=args.reading_type)
            ).encode('utf-8')
            buffer.write(json_obj)
            buffer.write("\n".encode("utf-8"))
            xml_files += 1

    buffer.seek(0)
    upload_mem(buffer, BUCKET, trgt_file)

    unpacked_files += len(batch)
    logging.info(
        f"{unpacked_files} tar files have been unpacked successfully, {xml_files} xml files have been uploaded to s3."
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("reading_type", type=str)
    parser.add_argument("year", type=str)
    parser.add_argument("month", type=str)
    parser.add_argument("day", type=str)
    args = parser.parse_args()

    validate_arg(args.reading_type, READING_TYPES)
    validate_arg(args.year, YEARS)
    validate_arg(args.month, MONTHS)
    validate_arg(args.day, DAYS)

    src_dir = f"{SRC_DIR}/{args.reading_type}/year={args.year}/month={args.month}/day={args.day}/"
    trgt_dir = src_dir.replace("unprocessed-raw", "unpacked-compacted-raw")

    logging.info(f"Unpacking tar files in {src_dir}.")

    with ThreadPoolExecutor(max_workers=5) as executor:
        wait([
            executor.submit(unpack_tar, batch, trgt_dir + f"{args.reading_type}_{len(batch)}_{size}")
            for batch, size in list_obj_in_batches(bucket=BUCKET, prefix=src_dir)
        ])

    logging.info(f"{xml_files} xml files unpacked successfully from {unpacked_files} tar files for {src_dir}.")
