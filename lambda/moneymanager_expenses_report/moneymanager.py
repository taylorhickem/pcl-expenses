""" this script imports the export file from money manager app
converts the raw file into an expense report with the tables:

tables:
1. main categories

the report has rows of the categories
and columns for each month in the year

"""
#-----------------------------------------------------------------------------
#import dependencies
#-----------------------------------------------------------------------------
from sqlgsheet import database as db
import pandas as pd


#-----------------------------------------------------------------------------
#Variables
#-----------------------------------------------------------------------------
TABLES = {
    'events': pd.DataFrame([])
}
FIELDS = {
    'amount': 'SGD',
    'start_date': 'Period',
    'txn_type': 'Income/Expense',
    'category': 'Category'
}
TXN_TYPE_EXPENSE = 'Expense'
REPORTING_YEAR = 0
REPORTING_MONTH = 0


def update(txns: pd.DataFrame):
    load()
    load_transactions(txns)
    post_to_gsheet()
    print('report updated!')


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


def load_transactions(events: pd.DataFrame):
    global TABLES
    events['start_date'] = events[FIELDS['start_date']].apply(lambda x: x.date())
    events['day'] = events['start_date'].apply(lambda x: x.day)
    events['month'] = events['start_date'].apply(lambda x: x.month)
    events['year'] = events['start_date'].apply(lambda x: x.year)
    events = events[events[FIELDS['txn_type']] == TXN_TYPE_EXPENSE].copy()
    events = events[events['year'] == REPORTING_YEAR].copy()
    events = events[events['month'] <= REPORTING_MONTH].copy()
    events_pvt = pd.pivot_table(events, index=FIELDS['category'], columns='month',
        values=FIELDS['amount'], aggfunc='sum')
    events_pvt.fillna(0, inplace=True)
    TABLES['main_category_report'] = events_pvt


def post_to_gsheet():
    # 01 main category
    main_category_report = TABLES['main_category_report']
    db.post_to_gsheet(main_category_report, 'expenses', 'main_category_report',
                      input_option='USER_ENTERED')
    db.post_to_gsheet(main_category_report.reset_index()[['Category']],
                      'expenses', 'main_categories',
                      input_option='USER_ENTERED')

