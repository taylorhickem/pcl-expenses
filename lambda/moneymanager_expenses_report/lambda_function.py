import os
import pandas as pd
import boto3
import moneymanager as mm
import dynamodb


ENV_VARIABLES = [
    'S3_BUCKET',
    'S3_PREFIX',
    'DATA_FILE',
    'DB_TABLE_NAME'
]
ENV = 'prod'
DATA_FILE = 'txns.xlsx'
DB_TABLE_NAME = 'pcl_expense_txns'
S3_BUCKET = ''
S3_PREFIX = ''
TXN_DATA_KEY = ''
LAYERS_DIR = '/opt'
LOCAL_DIR = ''
USER_DATA_FILES = {
    'gsheet_config': 'gsheet_config.json',
    'client_secret': 'client_secret.json'
}
DB_CONFIG_PATH = 'dynamodb_config.json'
USER_DATA = {}
s3_client = None


def lambda_handler(event, context):
    response = run(env=ENV, event=event, context=context)
    return response


def run(env='prod', event=None, context=None):

    env_load(env=env)
    set_user_data(env=env)

    try:
        txns = get_txns(env=env)
        moneymanager_load(env=env)
        success, message = mm.update(txns)
        if success:
            status_code = 200
            message = f'found {len(txns)} txns. {message}'
        else:
            status_code = 500
    except Exception as e:
        status_code = 500
        message = f'ERROR. failed to update report. {str(e)}'
    else:
        cleanup()

    response = {
        'status_code': status_code,
        'message': message
    }
    print(response)

    return response


def env_load(env='prod'):
    if env == 'prod':
        load_parameters()
    elif env == 'dev':
        dev_env_load()


def get_txns(env='prod'):
    if env == 'prod':
        txns = txns_from_s3()
    elif env == 'dev':
        txns = pd.read_excel(DATA_FILE)
    return txns


def set_user_data(env='prod'):
    global USER_DATA
    if env == 'prod':
        USER_DATA = {k: os.path.join(LAYERS_DIR, USER_DATA_FILES[k])
                     for k in USER_DATA_FILES}
    elif env == 'dev':
        USER_DATA = {k: os.path.join(LOCAL_DIR, USER_DATA_FILES[k])
                     for k in USER_DATA_FILES}


def moneymanager_load(env='prod'):
    USER_DATA['db_config'] = DB_CONFIG_PATH
    mm.db.set_user_data(**USER_DATA)
    mm.DB_TABLE_NAME = DB_TABLE_NAME
    mm.db.DB_SOURCE = 'generic'
    mm.db.load(generic_con_class=dynamodb.DynamoDBAPI)
    mm.load()


def load_parameters():
    for k in ENV_VARIABLES:
        if k in os.environ:
            globals()[k] = os.environ[k]
    globals()['TXN_DATA_KEY'] = f'{S3_PREFIX}/{DATA_FILE}'


def txns_from_s3():
    load_s3_client()
    print(f'downloading txn data from S3 Bucket: {S3_BUCKET} Object key: {TXN_DATA_KEY}')
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


def cleanup(env='prod'):
    if env == 'prod':
        refresh_event_data()


def refresh_event_data():
    load_s3_client()
    s3_client.delete_object(
        Bucket=S3_BUCKET,
        Key=TXN_DATA_KEY
    )


def dev_env_load():
    pass