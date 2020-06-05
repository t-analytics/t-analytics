from apiclient.discovery import build
from google.oauth2 import service_account
import requests
import pandas as pd
import io
from datetime import datetime


class DoubleClickBManager:
    def __init__(self, path_to_json):
        self.SCOPES = ["https://www.googleapis.com/auth/doubleclickbidmanager"]
        self.credentials = service_account.Credentials.from_service_account_file(path_to_json)
        self.scoped_credentials = self.credentials.with_scopes(self.SCOPES)
        self.analytics = build('doubleclickbidmanager', 'v1.1', credentials=self.scoped_credentials)

    def get_line_items(self):
        line_items = self.analytics.lineitems().downloadlineitems().execute()
        line_items_df = pd.read_csv(io.StringIO(line_items['lineItems']))
        return line_items_df

    def create_body(self, date_from, date_to, report_type, advertiser_id, dimensions, metrics):
        body = {
            "kind": "doubleclickbidmanager#query",
            "metadata": {
                "title": f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}",
                "dataRange": "CUSTOM_DATES",
                "format": "CSV",
                "running": True,
                "sendNotification": False
            },
            "params": {
                "type": report_type,
                "filters": [
                    {
                        "type": "FILTER_ADVERTISER",
                        "value": advertiser_id
                    }
                ],
                "groupBys": dimensions,
                "metrics": metrics
            },
            "reportDataEndTimeMs": int(datetime.timestamp(datetime.strptime(date_to, "%Y-%m-%d")) * 1000),
            "reportDataStartTimeMs": int(datetime.timestamp(datetime.strptime(date_from, "%Y-%m-%d")) * 1000)
        }
        return body

    def get_query_id(self, body):
        query_data = self.analytics.queries().createquery(body=body).execute()
        query_id = query_data['queryId']
        return query_id

    def get_report(self, date_from, date_to, report_type, advertiser_id, dimensions, metrics):
        body = self.create_body(date_from, date_to, report_type, advertiser_id, dimensions, metrics)
        query_id = self.get_query_id(body)

        self.analytics.queries().runquery(queryId=query_id).execute()

        get_query_data = self.analytics.queries().getquery(queryId=query_id).execute()
        while get_query_data.get('googleCloudStoragePathForLatestReport', False) is False:
            get_query_data = self.analytics.queries().getquery(queryId=query_id).execute()
        download_url = get_query_data['googleCloudStoragePathForLatestReport']
        report = requests.get(download_url)
        report_df = pd.read_csv(io.StringIO(report.content.decode("utf8")), skipfooter=10)

#
#
# # Запрос на создание Query
# # Список всех доступных параметров запроса https://developers.google.com/bid-manager/v1.1/queries?hl=ru
# title = f"{datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}"
# body = {
#  "kind": "doubleclickbidmanager#query",
#  "metadata": {
#   "title": title,
#   "dataRange": "LAST_30_DAYS",
#   "format": "CSV",
#   "running": True,
#   "sendNotification": False
#  },
#  "params": {
#   "type": "TYPE_GENERAL",
#   "groupBys": [
#    "FILTER_ADVERTISER_CURRENCY",
#    "FILTER_LINE_ITEM",
#    "FILTER_DATE",
#    "FILTER_INSERTION_ORDER",
#    "FILTER_LINE_ITEM_NAME",
#    "FILTER_INSERTION_ORDER_NAME"
#   ],
#   "metrics": [
#    "METRIC_IMPRESSIONS",
#    "METRIC_CLICKS",
#    "METRIC_TOTAL_MEDIA_COST_ADVERTISER"
#   ]
#  },
# #  "schedule": {
# #   "frequency": "DAILY"
# #  }
# }
# querie_data = analytics.queries().createquery(body=body).execute()
# queryId = querie_data['queryId']
#
# # Получить список запросов
# queries = analytics.queries().listqueries(pageSize=25).execute()
#
# queries_dict = {}
# for query in queries['queries']:
#     queries_dict[query['queryId']] = query['metadata']
#     queries_dict[query['queryId']]["group_by_len"] = len(query['params']['groupBys'])
#
#
# report = requests.get(queries_dict[queryId]['googleCloudStoragePathForLatestReport'])
# report_df = pd.read_csv(io.StringIO(report.content.decode("utf8")), skipfooter=7+queries_dict[queryId]['group_by_len'])
