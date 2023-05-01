from pandas.tseries.offsets import BDay
import logging
from datetime import datetime, date, timedelta

def check_between_days_boolean(execution_date,work_days=True,minimum_days=1,maximum_days=1):
    # Use this function with the ShortCircuitOperator to return True or False
    # Used for 'run or not' scenarios
    # execution_date is the day run date time
    # work_days is True when wanting to use conventional business work days from pandas Bday package (Mon - Fri)
    # minimum_days is the days added to the first day of the month for the date range to begin from
    # minimum_days is the days added to the first day of the month for the date range to end from

    # convert execution date of DAG run into useable timestamp and date for comparison
    run_timestamp = datetime.fromtimestamp(execution_date.timestamp())
    run_date = run_timestamp.date()
    month_first_day = run_date.replace(day=1)
    if work_days is True:
        # Gets first day of the month and adds number of business days to date from minimum days specified
        minimum_days_date = (month_first_day + BDay(minimum_days)).date()
        # Gets first day of the month and adds number of business days to date from maximum days specified
        maximum_days_date = (month_first_day + BDay(maximum_days)).date()
    else:
        # Gets first day of the month and adds number of days to date from minimum days specified
        minimum_days_date = (month_first_day + timedelta(minimum_days)).date()
        # Gets first day of the month and adds number of days to date from maximum days specified
        maximum_days_date = (month_first_day + timedelta(maximum_days)).date()
    logging.info("This is the run date: {}".format(str(run_date)))
    logging.info("This is the first day of the month for the run date: {}".format(str(month_first_day)))
    logging.info("This is the minimum date: {}".format(str(minimum_days_date)))
    logging.info("This is the maximum date: {}".format(str(maximum_days_date)))
    if minimum_days_date <= run_date <= maximum_days_date:
        return True
    return False

def check_between_days_branch_two_options(execution_date,task_1,task_2,work_days=True,minimum_days=1,maximum_days=1):
    # Use this function with the BranchPythonOperator to Task 1 or Task 2
    # Used for 'run this or that' scenarios
    # execution_date is the day run date time
    # Task 1 is the task to run when execution date falls within a date range
    # Task 2 is the task to run when execution date falls outside the date range
    # work_days is True when wanting to use conventional business work days from pandas Bday package (Mon - Fri)
    # minimum_days is the days added to the first day of the month for the date range to begin from
    # minimum_days is the days added to the first day of the month for the date range to end from

    # convert execution date of DAG run into useable timestamp and date for comparison
    run_timestamp = datetime.fromtimestamp(execution_date.timestamp())
    run_date = run_timestamp.date()
    month_first_day = run_date.replace(day=1)
    if work_days is True:
        # Gets first day of the month and adds number of business days to date from minimum days specified
        minimum_days_date = (month_first_day + BDay(minimum_days)).date()
        # Gets first day of the month and adds number of business days to date from maximum days specified
        maximum_days_date = (month_first_day + BDay(maximum_days)).date()
    else:
        # Gets first day of the month and adds number of days to date from minimum days specified
        minimum_days_date = (month_first_day + timedelta(minimum_days)).date()
        # Gets first day of the month and adds number of days to date from maximum days specified
        maximum_days_date = (month_first_day + timedelta(maximum_days)).date()
    logging.info("This is the run date: {}".format(str(run_date)))
    logging.info("This is the first day of the month for the run date: {}".format(str(month_first_day)))
    logging.info("This is the minimum date: {}".format(str(minimum_days_date)))
    logging.info("This is the maximum date: {}".format(str(maximum_days_date)))
    if minimum_days_date <= run_date <= maximum_days_date:
        return task_1
    return task_2