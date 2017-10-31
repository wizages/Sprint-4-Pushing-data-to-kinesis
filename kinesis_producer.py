import boto3
import json
from datetime import datetime
import calendar
import random
import time

my_stream_name = 'ClicksOnAds'

kinesis_client = boto3.client('kinesis', region_name='us-west-2')

def put_to_stream(payload, thing_id):

    print payload

    put_response = kinesis_client.put_record(
            StreamName=my_stream_name,
            Data=json.dumps(payload),
            PartitionKey=thing_id)


f = open('input.csv')
while True:
    filesize = 99999
    offset = random.randrange(filesize)
    f.seek(offset)
    f.readline()
    random_line = f.readline()
    if len(random_line) == 0:
        f.seek(0)
        random_line = f.readline()

    random_line = random_line.replace('\"', '').replace('\n', '')
    split_line = random_line.split(",")

    thing_id = split_line[4]
    payload = {
        'event_time':calendar.timegm(datetime.utcnow().timetuple()),
        'user_id':split_line[3],
        'advertiser_id':split_line[4],
        'campaign_id':split_line[5],
        'ad_id':split_line[6],
        'rendering_id':split_line[7],
        'creative_version':split_line[8],
        'site_id':split_line[9],
        'placement_id':split_line[10],
        'country_code':split_line[11],
        'state_region':split_line[12],
        'browser_platform_id':split_line[13],
        'browser_platform_version':split_line[14],
        'operating_system_id':split_line[15],
        'designated_market_area_id':split_line[16],
        'city_id':split_line[17],
        'zip_postal_code':split_line[18]
    }


    put_to_stream(payload, thing_id)

    # wait for 5 second
    time.sleep(5)