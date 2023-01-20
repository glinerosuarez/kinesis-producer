import json

from acoustic_parser_lambda import ATTRS, READINGS

if __name__ == '__main__':
    schema = [dict(Name=col, Type="string", Comment="") for col in (ATTRS + READINGS)]
    print(json.dumps(schema, indent=4))
