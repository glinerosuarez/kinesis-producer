import base64
import json
import boto3
import os
import time
import csv
import sys

from xml.etree.ElementTree import XML, fromstring
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
        parsedRecords = parseXML(payload)

        # Do custom processing on the payload here
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(parsedRecords)
        }
        output.append(output_record)

    print('Successfully processed {} records.'.format(len(event['records'])))
    return {'records': output}


def parseXML(inputXML):
    xmlstring = str(inputXML.decode('utf-8'))
    root = ET.fromstring(str(xmlstring))  # create element tree object
    payload = root.find(f'./{{{NS}}}messagePayload')
    reading_collection = payload.find(f'./{{{NS}}}readingCollection')
    attrs = [payload.find(f'./{{{NS}}}{a}').text for a in ATTRS]
    readings = [None] * len(READINGS)

    for r in reading_collection:
        pos = READINGS.get(r.find(f'./{{{NS}}}attributeName').text)
        if pos is not None:
            readings[pos] = r.find(f'./{{{NS}}}attributeValue').text

    record = "|".join(attrs + readings + ["\n"])
    print(record)
    return record.encode('utf-8')


if __name__ == '__main__':
    with open("1672963593000_14f2a453-1bb9-4c47-a7a5-dbc15e86523e.xml") as f:
        parseXML(f.read().encode("utf-8"))