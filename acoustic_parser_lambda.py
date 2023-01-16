import base64
import xml.etree.ElementTree as ET


NS = "http://uptake.com/bhp/1/sensors"
ATTRS = [
    "vehicleIdentifier",
    "componentIdentifier",
    "positionInTrain",
    "typeOfReading",
    "readingTimestampUTC",
    "readingLocation"
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
]
READINGS = {r: i for r, i in zip(READINGS, range(len(READINGS)))}

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
    attrs = [payload.find(f'./{{{NS}}}{a}').text for a in ATTRS]
    readings = [""] * len(READINGS)

    for r in reading_collection:
        pos = READINGS.get(r.find(f'./{{{NS}}}attributeName').text)
        if pos is not None:
            readings[pos] = r.find(f'./{{{NS}}}attributeValue').text

    record = "|".join(attrs + readings) + "\n"
    print(f"Record: {record}")
    return record.encode('utf-8')
