from constants import USERS_TABLE, PIPELINE_SUBSCRIPTION
from storage import dataframe_from_query


def send_report(email_addr, report):
    #todo notify of changes
    pass


def notify_subscribers_dl_event(report):
    q = "select * from {} where subscription = '{}'".format(USERS_TABLE, PIPELINE_SUBSCRIPTION)
    subscribers = dataframe_from_query(q)
    for em in subscribers.values.tolist():
        send_report(em, report)
