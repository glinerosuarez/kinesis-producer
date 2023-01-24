import argparse
import io
import logging
import multiprocessing
import re
import time
import xml.etree.ElementTree as ET
from typing import Iterator, List

import pandas as pd

import aws_utils
import consts
from aws_utils import get_file_contents_in_batches


logging.basicConfig(
  format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
  datefmt='%Y-%m-%d %H:%M:%S',
  level=logging.INFO,
  handlers=[logging.FileHandler(f"unpacker_{multiprocessing.current_process().pid}.log")]
)


class XMLFlattener:
    VEHICLE_COMPONENT_NS = "http://www.uptake.com/bhp/1/vehicleComponent"
    SIGNAL_NS = "{http://uptake.com/bhp/1/sensors}"

    @staticmethod
    def get_version_n_encoding(string: str):
        re_expression = r"<\?xml version='(.*?)' encoding='(.*?)'\?>"
        match = re.search(re_expression, string)
        return (match.group(1), match.group(2)) if match else None

    @staticmethod
    def get_opening_tag(string: str):
        match = re.search(r'<NS1:(.*?) ', string)
        return match.group(1) if match else None

    def get_xml_iter(self, lines: List[str]) -> Iterator[str]:
        lines = iter(lines)
        first_line = next(lines)
        if self.get_version_n_encoding(first_line) is None:
            raise ValueError(f"The first line of the file is not in the expected format.")
        else:
            xml = first_line

        for i, line in enumerate(lines):
            if self.get_version_n_encoding(line) is None:
                xml += line
            else:
                end = line.find("<?")
                xml += line[:end]
                yield xml
                xml = line[end:]

    def flatten_signals(self, xmls: List[str]):
        signal_rows = []

        for xml in xmls:
            signal_rows.append(self.flatten_signal(xml))

        return pd.DataFrame(signal_rows)

    def flatten_signal(self, xml: str):
        record = dict()
        root = ET.fromstring(xml)

        for parent_e in root:
            for e in parent_e:
                if len(e) > 0:
                    if e.tag == f"{self.SIGNAL_NS}readingCollection":
                        for r in e:
                            col_name = col_val = col_uom = None
                            for at in r:
                                if at.tag == f"{self.SIGNAL_NS}attributeName":
                                    col_name = at.text
                                elif at.tag == f"{self.SIGNAL_NS}attributeValue":
                                    col_val = at.text
                                elif at.tag == f"{self.SIGNAL_NS}attributeUoM":
                                    col_uom = at.text
                                else:
                                    raise ValueError(f"Unknown reading attribute: {at.tag}: {at.text}")
                            record[col_name] = col_val
                            if col_uom is not None:
                                record[col_name + "_UoM"] = col_uom
                    else:
                        raise ValueError(f"Unknown collection of elements: {e.tag}.")
                else:
                    record[e.tag.replace(self.SIGNAL_NS, "")] = e.text

        return record

    def flatten(self, file: str):
        match = re.search(r'xmlns:NS1="(.*?)"', file)
        ns = match.group(1)
        if ns == self.SIGNAL_NS.strip("{}"):
            self.flatten_signal(file)
        elif ns == self.VEHICLE_COMPONENT_NS:
            ...
        else:
            raise ValueError(f"Unknown namespace: {ns}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("reading_type", type=str)
    parser.add_argument("year", type=str)
    parser.add_argument("month", type=str)
    parser.add_argument("day", type=str)
    args = parser.parse_args()

    flattener = XMLFlattener()
    prefix = f"{consts.TRGT_DIR}/{args.reading_type}/year={args.year}/month={args.month}/day={args.day}/"
    logging.info(f"Starting flattener for files in: {prefix}.")

    for files in get_file_contents_in_batches(bucket=consts.BUCKET, prefix=prefix):
        logging.info(f"Flattening {len(files)} xml files.")
        df = flattener.flatten_signals(files)

        buf = io.BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)

        filepath = f"{consts.FLATTENED_FILES_DIR}/{args.reading_type}/year={args.year}/month={args.month}/day={args.day}/"
        filename = f"{args.reading_type}_{int(time.time() * 1_000)}_{args.year}{args.month}{args.day}_{len(df.index)}.csv"
        logging.info(f"Saving file to {filepath + filename}.")
        aws_utils.upload_fileobj(buf, consts.BUCKET, filepath + filename)
        logging.info(f"File saved successfully.")
