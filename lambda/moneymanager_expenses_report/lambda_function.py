import pandas as pd
import boto3
import moneymanager as mm

S3_BUCKET = 'pcl-expenses-txns-upload'
TXN_DATA_KEY = '2023-01-01 ~ 12-31.xlsx'
USER_DATA_DIR = '/opt'
user_data = {
    'gsheet_config': USER_DATA_DIR + '/gsheet_config.json',
    'client_secret': USER_DATA_DIR + '/client_secret.json',
    'mysql_credentials': USER_DATA_DIR + '/mysql_credentials.json'
}

s3_client = None


def lambda_handler(event, context):
    status_code = 500
    message = 'failed'

    s3_txns = txns_from_s3()

    mm.db.set_user_data(
        gsheet_config=user_data['gsheet_config'],
        client_secret=user_data['client_secret'],
        mysql_credentials=user_data['mysql_credentials']
    )
    mm.db.DB_SOURCE = 'remote'
    mm.load()

    # mm.update(txns=s3_txns)

    refresh_event_data()

    status_code = 200
    message = f'found {len(s3_txns)} txns'
    response = {
        'status_code': status_code,
        'message': message
    }

    print(response)

    return response


def txns_from_s3():
    load_s3_client()
    response = s3_client.get_object(
        Bucket=S3_BUCKET,
        Key=TXN_DATA_KEY
    )
    xlsx_contents = response['Body'].read()
    txns = pd.read_excel(xlsx_contents)
    return txns


def load_s3_client():
    global s3_client
    if not s3_client:
        s3_client = boto3.client('s3')


def refresh_event_data():
    load_s3_client()
    s3_client.delete_object(
        Bucket=S3_BUCKET,
        Key=TXN_DATA_KEY
    )