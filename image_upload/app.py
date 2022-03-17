import json
import boto3
import base64
import pymysql
import requests
from requests_toolbelt.multipart import decoder


def create_connection_token():
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name="ap-northeast-2"
    )
    get_secret_value_response = client.get_secret_value(
        SecretId='rds-secret-220317'
    )
    token = get_secret_value_response['SecretString']
    return eval(token)


def db_ops():
    token = create_connection_token()
    try:
        connection = pymysql.connect(
            host='database-1.cluster-czlcogja2wtq.ap-northeast-2.rds.amazonaws.com',
            user='admin',
            password=token['password'],
            db='sparta',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

    except pymysql.MySQLError as e:
        print("connection error!!")
        return e

    print("connection ok!!")
    return connection


def uploadToS3(body, originalFileName):
    s3 = boto3.client('s3')
    s3.put_object(
        ACL="public-read",
        Bucket='aws-sparta',
        Body=body,
        Key=originalFileName,
        ContentType='image/' + originalFileName.split('.')[1]
    )

    conn = db_ops()
    cursor = conn.cursor()
    cursor.execute("insert into image(url) value('" + originalFileName + "')")
    conn.commit()


def lambda_handler(event, context):
    if 'Content-Type' in event['headers']:
        content_type_header = event['headers']['Content-Type']
    else:
        content_type_header = event['headers']['content-type']

    postdata = base64.b64decode(event['body']).decode('iso-8859-1')
    lst = []
    for part in decoder.MultipartDecoder(postdata.encode('utf-8'), content_type_header).parts:
        lst.append(part.text)

    decoder_filename = decoder.MultipartDecoder(postdata.encode('utf-8'), content_type_header)
    file_name = decoder_filename.parts[0].headers[b'Content-Disposition'].decode().split(';')[2].split('=')[1].replace(
        '"', '')

    uploadToS3(lst[0].encode('iso-8859-1'), file_name)

    return {
        "statusCode": 200,
        # Cross Origin처리
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        "body": json.dumps({
            "message": "success",
        }),
    }
