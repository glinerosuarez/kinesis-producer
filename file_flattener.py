import abc
import argparse
import csv
import io
import logging
import multiprocessing
import xml.etree.ElementTree as ET
from typing import List, Dict, Optional

import pandas as pd

import aws_utils
import consts
from aws_utils import get_file_contents_in_batches


logging.basicConfig(
  format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
  datefmt='%Y-%m-%d %H:%M:%S',
  level=logging.INFO,
  handlers=[logging.FileHandler(f"flattener_{multiprocessing.current_process().pid}.log")]
)


class XMLFlattener(abc.ABC):
    @property
    @abc.abstractmethod
    def ns(self) -> str:
        raise NotImplementedError

    @property
    @abc.abstractmethod
    def end_tag(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def flatten(self, xml: str) -> List[Dict]:
        raise NotImplementedError

    def flatten_batch(self, xmls: List[str]) -> pd.DataFrame:
        rows = []
        for xml in xmls:
            rows.extend(self.flatten(xml))

        return pd.DataFrame(rows)

    def flatten_compacted_files(self, compacted_files: List[str]) -> pd.DataFrame:
        return self.flatten_batch(
            [
                xml.strip() + "\n" + self.end_tag
                for cf in compacted_files
                for xml in cf.split(self.end_tag)
                if len(xml.strip()) > 0
            ]
        )


class VehicleComponentFlattener(XMLFlattener):
    @property
    def end_tag(self) -> str:
        return "</NS1:vehicleComponent>"

    @property
    def ns(self) -> str:
        return "{http://www.uptake.com/bhp/1/vehicleComponent}"

    def flatten(self, xml: str) -> List[Dict]:
        records = []
        vc_attrs = dict()
        vc = ET.fromstring(xml)

        for e in vc:
            if len(e) > 0:
                if e.tag == f"{self.ns}componentCollection":
                    for component in e:
                        self.parse_component(component, None, records)
                else:
                    raise ValueError(f"Unknown collection of elements: {e.tag}.")
            else:
                vc_attrs[e.tag.replace(self.ns, "")] = e.text

        return [{**vc_attrs, **r} for r in records]

    def parse_component(self, component: ET.Element, parent_code: Optional[str], records: List[Dict]) -> None:
        record = {}
        component_code_element = component.find(f'./{self.ns}componentCode')
        component_code = None if component_code_element is None else component_code_element.text

        for element in component:
            if element.tag == f"{self.ns}subcomponentCollection":
                for subcomponent in element:
                    self.parse_component(subcomponent, component_code, records)
            elif element.tag == f"{self.ns}componentAttributeCollection":
                for at in element:
                    if len(at) == 2:
                        record[at.find(f'./{self.ns}attributeName').text] = at.find(f'./{self.ns}attributeValue').text
                    elif len(at) == 1:
                        record[at.find(f'./{self.ns}attributeName').text] = None
                    else:
                        raise ValueError(f"Unknown attribute structure: {list(at)}")
            else:
                if len(element) > 0:
                    raise ValueError(f"Unknown collection: {element}")
                record[element.tag.replace(self.ns, "")] = element.text

        record["parent_code"] = parent_code
        records.append(record)


class SignalFlattener(XMLFlattener):
    @property
    def end_tag(self) -> str:
        return "</NS1:message>"

    @property
    def ns(self) -> str:
        return "{http://uptake.com/bhp/1/sensors}"

    def flatten(self, xml: str) -> List[Dict]:
        record = dict()
        root = ET.fromstring(xml)

        for parent_e in root:
            for e in parent_e:
                if e.tag == f"{self.ns}readingCollection":
                    for r in e:
                        col_name = col_val = col_uom = None
                        for at in r:
                            if at.tag == f"{self.ns}attributeName":
                                col_name = at.text
                            elif at.tag == f"{self.ns}attributeValue":
                                col_val = at.text
                            elif at.tag == f"{self.ns}attributeUoM":
                                col_uom = at.text
                            else:
                                raise ValueError(f"Unknown reading attribute: {at.tag}: {at.text}")
                        record[col_name] = col_val
                        if col_uom is not None:
                            record[col_name + "_UoM"] = col_uom
                else:
                    if len(e) > 0:
                        raise ValueError(f"Unknown collection of elements: {e.tag}.")
                    record[e.tag.replace(self.ns, "")] = e.text

        return [record]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("reading_type", type=str)
    parser.add_argument("year", type=str)
    parser.add_argument("month", type=str)
    parser.add_argument("day", type=str)
    args = parser.parse_args()

    flattener = SignalFlattener() if args.reading_type in consts.SIGNALS else VehicleComponentFlattener()
    prefix = f"{consts.TRGT_DIR}/{args.reading_type}/year={args.year}/month={args.month}/day={args.day}/"
    logging.info(f"Flattening files in: {prefix}.")

    for files in get_file_contents_in_batches(bucket=consts.BUCKET, prefix=prefix, max_batch_size=3e8):
        logging.info(f"Flattening {len(files)} xml files.")
        df = flattener.flatten_compacted_files(files)
        buf = io.BytesIO()
        df.to_csv(buf, index=False, quoting=csv.QUOTE_ALL)
        buf.seek(0)

        filepath = f"{consts.FLATTENED_FILES_DIR}/{args.reading_type}/year={args.year}/month={args.month}/day={args.day}/"
        filename = f"{args.reading_type}_{args.year}{args.month}{args.day}_{len(df.index)}.csv"
        logging.info(f"Saving file to {filepath + filename}.")
        aws_utils.upload_fileobj(buf, consts.BUCKET, filepath + filename)
        logging.info(f"File saved successfully.")
