""" this script imports the export file from money manager app
converts the raw file into an expense report with the tables:

tables:
1. main categories

the report has rows of the categories
and columns for each month in the year

"""
#test2

#-----------------------------------------------------------------------------
#import dependencies
#-----------------------------------------------------------------------------
from sqlgsheet import database as db
import pandas as pd
import datetime as dt

#-----------------------------------------------------------------------------
#Variables
#-----------------------------------------------------------------------------
KEY_FIELD = 'start_date'
LAST_MODIFIED_FIELD = 'last_modified'
DB_TABLE_NAME = 'expense_txns'
FIELDS = {
    'amount': 'SGD',
    'start_date': 'Period',
    'txn_type': 'Income/Expense',
    'category': 'Category',
    'subcategory': 'Subcategory'
}
DROP_FIELDS = [
    'Accounts.1'
]
TXN_TYPE_EXPENSE = 'Exp.'
REPORTING_YEAR = 0
REPORTING_MONTH = 0
REPORTS = {}


def update(txns_raw: pd.DataFrame):
    report_success = True
    message = 'report updated.'

    load()
    if len(txns_raw) > 0:
        txns = transactions_format(txns_raw)
        new_txns = db_update(txns)
        reports_update(txns)
        post_to_gsheet()
        if not new_txns:
            message = 'report refreshed with no new transactions.'
    else:
        message = 'no transactions detected.'


    return report_success, message


def load():
    db.load_config()
    db.load_gsheet()
    update_config()


def update_config():
    global REPORTING_MONTH, REPORTING_YEAR
    config = db.get_sheet('expenses', 'config')
    parameters = config[config['group'] == 'reporting'][
        ['parameter', 'value']].set_index('parameter')['value']
    REPORTING_YEAR = int(parameters['reporting_year'])
    REPORTING_MONTH = int(parameters['reporting_month'])


def transactions_format(txns_raw: pd.DataFrame):
    for f in DROP_FIELDS:
        del txns_raw[f]
    txns = txns_raw.copy()
    txns['start_date'] = txns[FIELDS['start_date']]
    txns['day'] = txns['start_date'].apply(lambda x: x.day)
    txns['month'] = txns['start_date'].apply(lambda x: x.month)
    txns['year'] = txns['start_date'].apply(lambda x: x.year)
    txns = txns[txns[FIELDS['txn_type']] == TXN_TYPE_EXPENSE].copy()
    txns = txns[txns['year'] == REPORTING_YEAR].copy()
    txns = txns[txns['month'] <= REPORTING_MONTH].copy()
    return txns


def reports_update(txns: pd.DataFrame):
    global REPORTS
    if len(txns) > 0:
        # 01 main category report
        txns_pvt = pd.pivot_table(txns, index=FIELDS['category'], columns='month',
            values=FIELDS['amount'], aggfunc='sum')
        txns_pvt.fillna(0, inplace=True)
        REPORTS['main_category_report'] = txns_pvt

        # 02 subcategory report
        sub_cat_pvt = pd.pivot_table(txns, index=[FIELDS['category'], FIELDS['subcategory']], columns='month',
                                     values=FIELDS['amount'], aggfunc='sum')
        sub_cat_pvt.fillna(0, inplace=True)
        REPORTS['subcategory_report'] = sub_cat_pvt


def post_to_gsheet():
    # 01 main category
    main_category_report = REPORTS['main_category_report']
    db.post_to_gsheet(main_category_report, 'expenses', 'main_category_report',
                      input_option='USER_ENTERED')
    db.post_to_gsheet(main_category_report.reset_index()[['Category']],
                      'expenses', 'main_categories',
                      input_option='USER_ENTERED')

    #02 subcategories
    subcategory_report = REPORTS['subcategory_report']

    #data fields
    db.post_to_gsheet(subcategory_report, 'expenses', 'subcategory_report',
                      input_option='USER_ENTERED')
    #category field
    db.post_to_gsheet(subcategory_report.reset_index()[[FIELDS['category'], FIELDS['subcategory']]],
                      'expenses', 'subcategories',
                      input_option='USER_ENTERED')


def db_update(rows: pd.DataFrame, has_duplicates=True):
    """ updates the database with new rows
    """
    new_rows = True
    if db.table_exists(DB_TABLE_NAME) and has_duplicates:
        unique_rows = remove_duplicates(rows)
        if len(unique_rows) > 0:
            db_update(unique_rows, has_duplicates=False)
        else:
            new_rows = False
    else:
        db_rows_insert(rows)
    return new_rows

def remove_duplicates(rows: pd.DataFrame) -> pd.DataFrame:
    """ removes duplicates and returns only unique rows that are not in the db
    """
    # get the rows from the db
    db_rows = db_query()

    # add only the new events not already in the database
    not_in_db = pd.concat([db_rows, db_rows, rows])
    not_in_db.drop_duplicates(
        subset=[KEY_FIELD],
        keep=False,
        inplace=True
    )

    return not_in_db


def db_query() -> pd.DataFrame:
    db_events = db.get_table(DB_TABLE_NAME)
    if db_events is None:
        db_events = pd.DataFrame([])
    else:
        if LAST_MODIFIED_FIELD in db_events:
            del db_events[LAST_MODIFIED_FIELD]
    return db_events


def db_rows_insert(rows: pd.DataFrame):
    with_lm = add_lm_timestamp(rows)
    if db.table_exists(DB_TABLE_NAME):
        db.rows_insert(with_lm, DB_TABLE_NAME, con=db.con)
    else:
        db.update_table(with_lm, DB_TABLE_NAME, append=False)


def add_lm_timestamp(rows: pd.DataFrame) -> pd.DataFrame:
    with_lm = rows.copy()
    if len(with_lm) > 0:
        last_modified = dt.datetime.now()
        with_lm[LAST_MODIFIED_FIELD] = last_modified
    return with_lm
