import time, json
from apiclient.discovery import build
from google.oauth2 import service_account
import socket
from googleapiclient.errors import HttpError
from connectors.connectors._Utils import create_fields_ga
from pprint import pprint


class GAnalytics:
    def __init__(self, path_to_json, view_id, client_name):
        self.KEY_FILE_LOCATION = path_to_json
        self.SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]
        self.VIEW_ID = view_id
        self.credentials = service_account.Credentials.from_service_account_file(self.KEY_FILE_LOCATION)
        self.scoped_credentials = self.credentials.with_scopes(self.SCOPES)
        self.analytics = build('analyticsreporting', 'v4', credentials=self.scoped_credentials)

        self.report_dict = {
            "General": {
                "metrics": {
                    "ga_users": {"type": "INTEGER", "mode": "NULLABLE", "description": "Users :INTEGER"},
                    "ga_newUsers": {"type": "INTEGER", "mode": "NULLABLE", "description": "New users :INTEGER"},
                    "ga_sessions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Sessions :INTEGER"},
                    "ga_bounces": {"type": "INTEGER", "mode": "NULLABLE", "description": "Bounces :INTEGER"},
                    "ga_sessionDuration": {"type": "FLOAT", "mode": "NULLABLE", "description": "Session duration :FLOAT"},
                    "ga_pageviews": {"type": "INTEGER", "mode": "NULLABLE", "description": "Page views :INTEGER"},
                    "ga_hits": {"type": "INTEGER", "mode": "NULLABLE", "description": "Hits :INTEGER"}},
                "dimensions": {
                    "ga_campaign": {"type": "STRING", "mode": "NULLABLE", "description": "UTMCampaign :STRING"},
                    "ga_sourceMedium": {"type": "STRING", "mode": "NULLABLE", "description": "UTMSource / UTMMedium :STRING"},
                    "ga_keyword": {"type": "STRING", "mode": "NULLABLE", "description": "UTMKeyword :STRING"},
                    "ga_adContent": {"type": "STRING", "mode": "NULLABLE", "description": "UTMContent :STRING"},
                    "ga_date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "ga_deviceCategory": {"type": "STRING", "mode": "NULLABLE", "description": "Device category :STRING"}}},
            "GeneralByClientID": {
                "metrics": {
                    "ga_pageviews": {"type": "INTEGER", "mode": "NULLABLE", "description": "Page views :INTEGER"},
                    "ga_sessionDuration": {"type": "FLOAT", "mode": "NULLABLE", "description": "Session duration :FLOAT"},
                    "ga_hits": {"type": "INTEGER", "mode": "NULLABLE", "description": "Hits :INTEGER"},
                    "ga_bounces": {"type": "INTEGER", "mode": "NULLABLE", "description": "Bounces :INTEGER"},
                    "ga_goalCompletionsAll": {"type": "INTEGER", "mode": "NULLABLE", "description": "All goals :INTEGER"}},
                "dimensions": {
                    "ga_campaign": {"type": "STRING", "mode": "NULLABLE", "description": "UTMCampaign :STRING"},
                    "ga_sourceMedium": {"type": "STRING", "mode": "NULLABLE", "description": "UTMSource / UTMMedium :STRING"},
                    "ga_keyword": {"type": "STRING", "mode": "NULLABLE", "description": "UTMKeyword :STRING"},
                    "ga_adContent": {"type": "STRING", "mode": "NULLABLE", "description": "UTMContent :STRING"},
                    "ga_date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "ga_deviceCategory": {"type": "STRING", "mode": "NULLABLE", "description": "Device category :STRING"},
                    "ga_userType": {"type": "STRING", "mode": "NULLABLE", "description": "User type :STRING"},
                    "ga_city": {"type": "STRING", "mode": "NULLABLE", "description": "City :STRING"},
                    "ga_ClientId": {"type": "STRING", "mode": "NULLABLE", "description": "ClientID :STRING"}}},
            "Goal1to10ByClientID": {
                "metrics": {
                    "ga_goal1Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal1 :INTEGER"},
                    "ga_goal2Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal2 :INTEGER"},
                    "ga_goal3Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal3 :INTEGER"},
                    "ga_goal4Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal4 :INTEGER"},
                    "ga_goal5Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal5 :INTEGER"},
                    "ga_goal6Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal6 :INTEGER"},
                    "ga_goal7Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal7 :INTEGER"},
                    "ga_goal8Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal8 :INTEGER"},
                    "ga_goal9Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal9 :INTEGER"},
                    "ga_goal10Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal10 :INTEGER"}
                },
                "dimensions": {
                    "ga_campaign": {"type": "STRING", "mode": "NULLABLE", "description": "UTMCampaign :STRING"},
                    "ga_sourceMedium": {"type": "STRING", "mode": "NULLABLE", "description": "UTMSource / UTMMedium :STRING"},
                    "ga_keyword": {"type": "STRING", "mode": "NULLABLE", "description": "UTMKeyword :STRING"},
                    "ga_adContent": {"type": "STRING", "mode": "NULLABLE", "description": "UTMContent :STRING"},
                    "ga_date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "ga_deviceCategory": {"type": "STRING", "mode": "NULLABLE", "description": "Device category :STRING"},
                    "ga_userType": {"type": "STRING", "mode": "NULLABLE", "description": "User type :STRING"},
                    "ga_city": {"type": "STRING", "mode": "NULLABLE", "description": "City :STRING"},
                    "ga_ClientId": {"type": "STRING", "mode": "NULLABLE", "description": "ClientID :STRING"}}},
            "Goal11to20ByClientID": {
                "metrics": {
                    "ga_goal11Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal11 :INTEGER"},
                    "ga_goal12Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal12 :INTEGER"},
                    "ga_goal13Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal13 :INTEGER"},
                    "ga_goal14Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal14 :INTEGER"},
                    "ga_goal15Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal15 :INTEGER"},
                    "ga_goal16Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal16 :INTEGER"},
                    "ga_goal17Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal17 :INTEGER"},
                    "ga_goal18Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal18 :INTEGER"},
                    "ga_goal19Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal19 :INTEGER"},
                    "ga_goal20Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal20 :INTEGER"}
                },
                "dimensions": {
                    "ga_campaign": {"type": "STRING", "mode": "NULLABLE", "description": "UTMCampaign :STRING"},
                    "ga_sourceMedium": {"type": "STRING", "mode": "NULLABLE", "description": "UTMSource / UTMMedium :STRING"},
                    "ga_keyword": {"type": "STRING", "mode": "NULLABLE", "description": "UTMKeyword :STRING"},
                    "ga_adContent": {"type": "STRING", "mode": "NULLABLE", "description": "UTMContent :STRING"},
                    "ga_date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "ga_deviceCategory": {"type": "STRING", "mode": "NULLABLE", "description": "Device category :STRING"},
                    "ga_userType": {"type": "STRING", "mode": "NULLABLE", "description": "User type :STRING"},
                    "ga_city": {"type": "STRING", "mode": "NULLABLE", "description": "City :STRING"},
                    "ga_ClientId": {"type": "STRING", "mode": "NULLABLE", "description": "ClientID :STRING"}}},
            "Goal1to10": {
                "metrics": {
                    "ga_goal1Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal1 :INTEGER"},
                    "ga_goal2Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal2 :INTEGER"},
                    "ga_goal3Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal3 :INTEGER"},
                    "ga_goal4Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal4 :INTEGER"},
                    "ga_goal5Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal5 :INTEGER"},
                    "ga_goal6Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal6 :INTEGER"},
                    "ga_goal7Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal7 :INTEGER"},
                    "ga_goal8Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal8 :INTEGER"},
                    "ga_goal9Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal9 :INTEGER"},
                    "ga_goal10Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal10 :INTEGER"}},
                "dimensions": {
                    "ga_campaign": {"type": "STRING", "mode": "NULLABLE", "description": "UTMCampaign :STRING"},
                    "ga_sourceMedium": {"type": "STRING", "mode": "NULLABLE", "description": "UTMSource / UTMMedium :STRING"},
                    "ga_keyword": {"type": "STRING", "mode": "NULLABLE", "description": "UTMKeyword :STRING"},
                    "ga_adContent": {"type": "STRING", "mode": "NULLABLE", "description": "UTMContent :STRING"},
                    "ga_date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "ga_deviceCategory": {"type": "STRING", "mode": "NULLABLE", "description": "Device category :STRING"}}},
            "Goal11to20": {
                "metrics": {
                    "ga_goal11Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal1 :INTEGER"},
                    "ga_goal12Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal12 :INTEGER"},
                    "ga_goal13Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal13 :INTEGER"},
                    "ga_goal14Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal14 :INTEGER"},
                    "ga_goal15Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal15 :INTEGER"},
                    "ga_goal16Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal16 :INTEGER"},
                    "ga_goal17Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal17 :INTEGER"},
                    "ga_goal18Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal18 :INTEGER"},
                    "ga_goal19Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal19 :INTEGER"},
                    "ga_goal20Completions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Goal20 :INTEGER"}},
                "dimensions": {
                    "ga_campaign": {"type": "STRING", "mode": "NULLABLE", "description": "UTMCampaign :STRING"},
                    "ga_sourceMedium": {"type": "STRING", "mode": "NULLABLE", "description": "UTMSource / UTMMedium :STRING"},
                    "ga_keyword": {"type": "STRING", "mode": "NULLABLE", "description": "UTMKeyword :STRING"},
                    "ga_adContent": {"type": "STRING", "mode": "NULLABLE", "description": "UTMContent :STRING"},
                    "ga_date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "ga_deviceCategory": {"type": "STRING", "mode": "NULLABLE", "description": "Device category :STRING"}}}}

        self.tables_with_schema, self.fields = create_fields_ga(client_name, "GAnalytics", self.report_dict, view_id)

    def convert_data(self, dimension_list, metric_list, response_data_list):
        columns = dimension_list + metric_list
        total_data_list = []
        for element in response_data_list:
            for one_dict in element:
                total_data_list.append(one_dict['dimensions']+one_dict['metrics'][0]['values'])
        return columns, total_data_list

    def request(self, body):
        try:
            response = self.analytics.reports().batchGet(body=body).execute()
        except socket.timeout:
            time.sleep(2)
            return self.request(body)
        except ConnectionResetError:
            time.sleep(2)
            self.analytics = build('analyticsreporting', 'v4', credentials=self.scoped_credentials)
            return self.request(body)
        except HttpError as http_error:
            http_error = json.loads(http_error.content.decode("utf8"))
            code = int(http_error['code'])
            message = http_error['message']
            status = http_error['status']
            if code > 500:
                time.sleep(30)
                self.analytics = build('analyticsreporting', 'v4', credentials=self.scoped_credentials)
                return self.request(body)
            else:
                raise Exception(f"code - {code}. status - {status}.\n {message}")
        return response

    def create_params(self, list_of_params, type_of_metric):
        params_dict = []
        if type_of_metric == "metrics":
            key = 'expression'
        elif type_of_metric == "dimensions":
            key = 'name'
        else:
            raise Exception("Not supported type")
        for param in list_of_params:
            params_dict.append({key: param})
        return params_dict

    def create_body(self, date_from, date_to, metric, dimension, page_token='', dimension_filter=None):
        if dimension_filter is None:
            dimension_filter = []
        body = {
            "reportRequests":
                [{
                    "viewId": self.VIEW_ID,
                    "dateRanges": [{"startDate": date_from, "endDate": date_to}],
                    "metrics": metric,
                    "dimensions": dimension,
                    "dimensionFilterClauses": dimension_filter,
                    "samplingLevel": "LARGE",
                    "pageSize": 50000,
                    "pageToken": page_token
                }]
        }
        return body

    def get_request(self, date_from, date_to, metric_list, dimension_list, dimension_filter=None):
        metric = self.create_params(metric_list, 'metrics')
        dimension = self.create_params(dimension_list, 'dimensions')
        response_data_list = []

        body = self.create_body(date_from, date_to, metric, dimension, dimension_filter=dimension_filter)

        response = self.request(body)
        if response['reports'][0]['data'].get("rows", False):
            response_data_list.append(response['reports'][0]['data']['rows'])

            while response['reports'][0].get('nextPageToken') is not None:
                page_token = response['reports'][0]['nextPageToken']
                body = self.create_body(date_from, date_to, metric, dimension, page_token=page_token,
                                        dimension_filter=dimension_filter)
                response = self.request(body)
                response_data_list.append(response['reports'][0]['data']['rows'])
                time.sleep(2)
            columns, result_list_of_data = self.convert_data(dimension_list, metric_list, response_data_list)
            return columns, result_list_of_data
        else:
            return []
