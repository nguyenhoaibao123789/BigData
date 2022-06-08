"""
F21: get data from Kinesis stream.
"""
import logging
import base64
import datetime
import json
import ast
import boto3
import botocore
from datetime import date
import time
import urllib3

logging.basicConfig(filename='Resources/data_from_kinesis_to_firehose.log',level=logging.INFO)
def get_data_from_openweathermap_api():
    """
    Function to get data from api of OpenWeather website.
    Get current weather of 70 citys by citys.txt.
    Returns:
        byte_array: Data from API which is converted to bytes.
    """
    today = date.today()
    unix_current=int(time.mktime(today.timetuple()))
    base_url='http://api.openweathermap.org/data/2.5/air_pollution/history?lat=10.762622&lon=106.660172&start=1577836800&end='
    key='b60f0372e742bee4573835edf3732072'
    url=base_url+str(unix_current)+'&appid='+key
    http = urllib3.PoolManager()
    response=http.request("GET", url)
    if response.status == 200:
        data_bytes=response.data
        data_dict=json.loads(data_bytes)
        data=data_dict['list']
    else:
        logging.exception(response.status)
    return data

def flatten(dictionary):
    """ Function to flatten data
    flattern a dictionary, json variable.

    Args:
        dictionary: a dictionary contain data.
    """
    try:
        date=datetime.datetime.utcfromtimestamp(dictionary['dt']).strftime('%Y-%m-%d %H')+":00:00"
        data={
            'Datetime':date,
            'CO':dictionary['components']['co'],
            'NO':dictionary['components']['no'],
            'NO2':dictionary['components']['no2'],
            'O3':dictionary['components']['o3'],
            'SO2':dictionary['components']['so2'],
            'PM2_5':dictionary['components']['pm2_5'],
            'PM10':dictionary['components']['pm10'],
            'NH3':dictionary['components']['nh3'],
            'Air_quality':dictionary['main']['aqi']
        }
    except (KeyError,TypeError) as error:
        logging.exception(error)
        return "error"
    return data

def push_to_firehose(stream_name,data):
    """ Function to push data
    Push data to a kinesis firehose stream

    Args:
        stream_name: Name of the stream to push data to.
        data: data pf byte type to push.
    """
    try:
        firehose = boto3.client("firehose",endpoint_url="http://host.docker.internal:4566")
        firehose.put_record(
            DeliveryStreamName = stream_name,
            Record={
                "Data":json.dumps(data)
            }
        )
    except (TypeError,KeyError,
            botocore.exceptions.ClientError,
            botocore.exceptions.ConnectionClosedError) as error:
        logging.exception(error)
        return "error"
    return "succeed"

def transform_data(dataraw):
    """ Function to transform data
    transform a string to dictionary
    Args:
        data_decode: a string contain all data.
    """
    try:
        list_data=[]
        for record in dataraw:
            data=flatten(record)
            list_data.append(data)
    except (AttributeError,SyntaxError) as error:
        logging.exception(error)
        return "error"
    return list_data

def main(event, context):
    """ Function to catch push event from kinesis stream
        Extract data from event and processing.

    Args:
        event: the data that's passed to the function upon execution.
        context: context object contains all data and methods related to lambda function.
    """
    # pylint: disable=unused-argument
    #s3_client = boto3.client('s3', endpoint_url="http://host.docker.internal:4566")
    dataraw=get_data_from_openweathermap_api()
    list_data=transform_data(dataraw)
    firehose_stream_name ='firehosestream'
    push_to_firehose(firehose_stream_name,list_data)
    return "succeed"
    