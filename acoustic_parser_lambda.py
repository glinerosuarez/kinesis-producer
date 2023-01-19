import base64
import json
import xml.etree.ElementTree as ET

NS = "http://uptake.com/bhp/1/sensors"
ATTRS = [
    "vehicleIdentifier",
    "componentIdentifier",
    "positionInTrain",
    "typeOfReading",
    "readingTimestampUTC",
    "readingLocation",
    "sourceSystem"
]
READINGS = [
    "SensorDataQualityDescription",
    "SiteTimeZoneId",
    "SiteName",
    "TrainDirection",
    "VehicleTag",
    "VehicleEndLeading",
    "TrackSide",
    "TrainAxleNumber",
    "VehicleAxleNumber",
    "VehicleSide",
    "RailBAMBearingFaultCode",
    "RailBAMWheelFaultCode",
    "RMSTotalDB",
    "RMSBandDB",
    "LooseFrettingDB",
    "RollerDB",
    "CupDB",
    "ConeDB",
    "NoisyDB",
    "RMSBandWheelflatDB",
    "WheelflatDB",
    "TrainVehicleNumber",
    "WHEEL_TEMPERATURE",
    "BEARING_TEMPERATURE",
    "weight",
    "weight_UoM",
    "vertical_peak_UoM",
    "vertical_peak",
    "speed",
    "speed_UoM",
    "BrokenSpringDefect",
]

READINGS_W_UOM = ["weight", "vertical_peak", "speed"]

print('Loading function')


def lambda_handler(event, context):
    output = []

    for record in event['records']:
        payload = base64.b64decode(record['data'])
        parsed_records = parse_xml(payload)

        # Do custom processing on the payload here
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(parsed_records).decode("utf-8")
        }
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))
    return {'records': output}


def parse_xml(input_xml):
    decoded_xml = input_xml.decode('utf-8')
    xml_string = str(decoded_xml)
    root = ET.fromstring(str(xml_string))  # create element tree object
    payload = root.find(f'./{{{NS}}}messagePayload')
    reading_collection = payload.find(f'./{{{NS}}}readingCollection')
    readings = {k: None for k in READINGS}

    attrs = {a: payload.find(f'./{{{NS}}}{a}').text for a in ATTRS}
    if reading_collection is not None:
        for r in reading_collection:
            reading_name = r.find(f'./{{{NS}}}attributeName')
            if reading_name is not None:
                readings[reading_name.text] = r.find(f'./{{{NS}}}attributeValue').text
                if reading_name.text in READINGS_W_UOM:
                    readings[reading_name.text + "_UoM"] = r.find(f'./{{{NS}}}attributeUoM').text

    record = json.dumps({**attrs, **readings})
    return record.encode('utf-8')
