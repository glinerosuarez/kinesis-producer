import argparse
import logging
import multiprocessing
import tarfile
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, wait
from typing import List

import pendulum

from aws_utils import list_keys, download_mem, upload_mem
from consts import READING_TYPES, YEARS, MONTHS, DAYS, BUCKET, NS

logging.basicConfig(
  format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
  datefmt='%Y-%m-%d %H:%M:%S',
  level=logging.INFO,
  handlers=[logging.FileHandler(f"unpacker_{multiprocessing.current_process().pid}.log")]
)


def get_tar_keys(rt: str, y: str, m: str, d: str) -> List[str]:
    """
    Unpack xml's from tar files
    :param rt: one of READING_TYPES
    :param y: year, one of YEARS
    :param m: month, one of MONTHS
    :param d: day, one of DAYS
    :return:
    """
    def validate_arg(arg, valid_vals):
        if arg not in valid_vals:
            raise ValueError(f"Invalid argument: {arg}, must be one of {valid_vals}")

    validate_arg(rt, READING_TYPES)
    validate_arg(y, YEARS)
    validate_arg(m, MONTHS)
    validate_arg(d, DAYS)

    src_dir = f"bhp/legacy-data-snapshot/unprocessed-raw/{rt}/year={y}/month={m}/day={d}/"

    return list_keys(BUCKET, src_dir)


def unpack_tar(key: str) -> None:
    trgt_dir = "/".join(key.split("/")[:-1]).replace("unprocessed-raw", "unpacked-raw")
    #logging.info(f"unpacking file {key}.")

    obj = download_mem(BUCKET, key)
    tar = tarfile.open(fileobj=obj)

    for m_info in tar.getmembers():  # Iterate over files inside the tar file.
        member = tar.extractfile(m_info)
        xml_data = ET.fromstring(member.read())
        ts = xml_data.findall(f'.//{{{NS}}}messageTimestampUTC')
        if len(ts) != 1:
            raise ValueError("number of messageTimestampUTC elements is different than 0.")
        ts = int(pendulum.parse(ts[0].text, tz="UTC").timestamp() * 1_000)
        trgt_file = f"{trgt_dir}/{ts}_{m_info.name}"
        member.seek(0)
        upload_mem(member, BUCKET, trgt_file)
        logging.info(f"file {trgt_file} has been uploaded successfully.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("reading_type", type=str)
    parser.add_argument("year", type=str)
    parser.add_argument("month", type=str)
    parser.add_argument("day", type=str)
    args = parser.parse_args()

    with ThreadPoolExecutor(max_workers=10) as executor:
        wait([
            executor.submit(unpack_tar, key)
            for key in get_tar_keys(args.reading_type, args.year, args.month, args.day)
        ])
    logging.info(f"xml files unpacked successfully for {args.reading_type} {args.year} {args.month} {args.day}")