""" this script imports the export file from money manager app
converts the raw file into two expense reports

1. main categories
2. sub categories

each report has rows of the categories (or subcategories)
and columns for each month in the year

"""
#-----------------------------------------------------------------------------
#import dependencies
#-----------------------------------------------------------------------------
import sys
from sqlgsheet import database as db
import datetime as dt
import pandas as pd


#-----------------------------------------------------------------------------
#Variables
#-----------------------------------------------------------------------------
TABLES = {
    'events': []
}
DATE_FORMAT = '%d-%m-%Y'
CSV_ENCODING = 'iso-8859-1'
CSV_FILENAME = '../../2022.csv'
XLSX_FILENAME = '../../2023-01-01 ~ 12-31.xlsx'
REPORING_YEAR = 2020
REPORING_MONTH = 12


#-----------------------------------------------------------------------------
#main
#-----------------------------------------------------------------------------
def update(txns=None):
    load()
    load_transactions(events=txns)
    post_to_gsheet()
    print('report updated!')


#-----------------------------------------------------------------------------
#setup
#-----------------------------------------------------------------------------
def load():
    db.load_config()
    db.load_gsheet()
    update_config()


def update_config():
    global REPORING_YEAR, REPORING_MONTH
    config = db.get_sheet('expenses', 'config')
    parameters = config[config['group'] == 'reporting'][
        ['parameter', 'value']].set_index('parameter')['value']
    REPORING_YEAR = int(parameters['reporting_year'])
    REPORING_MONTH = int(parameters['reporting_month'])


#-----------------------------------------------------------------------------
#subfunctions
#-----------------------------------------------------------------------------
def load_transactions(events=None, event_format='xlsx'):
    global TABLES
    if events is None:
        if event_format == 'xlsx':
            events = pd.read_excel(XLSX_FILENAME)
        elif event_format == 'csv':
            events = pd.read_csv(CSV_FILENAME, encoding=CSV_ENCODING)
    subfields = ['Period',
                 'Category',
                 'Subcategory',
                 'Amount']
    events['start_date'] = events['Period'].apply(lambda x: x.date())
    events['day'] = events['start_date'].apply(lambda x: x.day)
    events['month'] = events['start_date'].apply(lambda x: x.month)
    events['year'] = events['start_date'].apply(lambda x: x.year)
    events = events[events['Income/Expense'] == 'Expense'].copy()
    events = events[events['year'] == REPORING_YEAR].copy()
    events = events[events['month'] <= REPORING_MONTH].copy()
    events_pvt = pd.pivot_table(events, index='Category', columns='month',
                             values='Amount', aggfunc='sum')
    events_pvt.fillna(0, inplace=True)
    TABLES['main_category_report'] = events_pvt


def post_to_gsheet():
    # 01 main category
    main_category_report = TABLES['main_category_report']
    # data fields
    db.post_to_gsheet(main_category_report, 'expenses', 'main_category_report',
                      input_option='USER_ENTERED')
    # category field
    db.post_to_gsheet(main_category_report.reset_index()[['Category']],
                      'expenses', 'main_categories',
                      input_option='USER_ENTERED')

    # 02 subcategories
    subcategory_report = TABLES['subcategory_report']
    # data fields
    db.post_to_gsheet(subcategory_report, 'expenses', 'subcategory_report',
                      input_option='USER_ENTERED')
    # category field
    db.post_to_gsheet(subcategory_report.reset_index()[['Category', 'Subcategory']],
                      'expenses', 'subcategories',
                      input_option='USER_ENTERED')


# -----------------------------------------------------
# Command line interface
# -----------------------------------------------------
def autorun():
    if len(sys.argv) > 1:
        process_name = sys.argv[1]
        if process_name == 'pink_floyd':
            print('dont take a slice of my pie')
    else:
        update()


if __name__ == "__main__":
    autorun()
# -----------------------------------------------------
# Reference code
# -----------------------------------------------------

#-----------------------------------------------------------------------------
#main
#-----------------------------------------------------------------------------
