from itertools import cycle
from typing import List, Tuple, Iterator
import xml.etree.ElementTree as ET

from attr import define

from aws_utils import list_keys, download_mem
from consts import BUCKET, DAYS, TRGT_DIR


@define
class XmlGenerator:
    reading_type: str

    def _get_day_iterator(self) -> Iterator[str]:
        for d in cycle(DAYS):
            yield sorted(list_keys(BUCKET, TRGT_DIR + f"/{self.reading_type}/year=2023/month=01/day={d}"))

    def _get_key_batch_iterator(self) -> Iterator[Tuple[int, List[str]]]:
        batch = []
        for keys in self._get_day_iterator():
            for k in keys:
                # keys' structure: DIR/ts_hash.xml, here we extract the 'ts' part
                # example: 'bhp/legacy-data-snapshot/unpacked-raw/ACOUSTIC/year=2023/month=01/day=01/1672531153000_47c9a9fd-df80-48a9-b711-f0a640b5db0b.xml'
                ts = k.split("/")[-1].split("_")[0]

                if len(batch) == 0:
                    last_ts = ts
                    batch = [k]
                else:
                    if ts == last_ts:
                        batch.append(k)
                    else:
                        yield int(last_ts), batch
                        last_ts = ts
                        batch = [k]
        yield int(last_ts), batch

    def get_batches(self):
        for ts, key_batch in self._get_key_batch_iterator():
            yield [download_mem(BUCKET, key) for key in key_batch]


if __name__ == '__main__':
    XmlGenerator("ACOUSTIC").get_batches()


