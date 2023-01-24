import io
import re
import xml.etree.ElementTree as ET
from typing import Iterator, List


class XMLFlattener:

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

        #tag = self.get_opening_tag(next(lines))
        #if tag is None:
        #    raise ValueError(f"No tag found.")

        for i, line in enumerate(lines):
            print(f"line {i}: {line}")
            if self.get_version_n_encoding(line) is None:
                xml += line
            else:
                end = line.find("<?")
                xml += line[:end]
                yield xml
                xml = line[end:]

    def flatten(self, file: bytes):
        decoded_xml = file.decode('utf-8')
        root = ET.fromstring(decoded_xml)
        breakpoint()


if __name__ == '__main__':
    with open('example7') as f:
        parser = XMLFlattener()
        for xml in parser.get_xml_iter(f.readlines()):
            breakpoint()
        #XMLFlattener().flatten(f.read())

