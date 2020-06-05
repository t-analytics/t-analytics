import requests, time
import pandas as pd
from connectors.connectors._Utils import create_fields, slice_date_on_period
from connectors.connectors._BigQuery import BigQuery


class YMLogs:
    def __init__(self, counter, access_token, client_name, path_to_bq, date_from, date_to):
        self.__request_url = f"https://api-metrika.yandex.net/management/v1/counter/{counter}/"
        self.__source = 'visits'
        self.date_from = date_from
        self.date_to = date_to
        self.counter_id = counter
        self.client_name = client_name
        self.data_set_id = f"{client_name}_YMLogs_{counter}"
        self.path_to_bq = path_to_bq
        self.bq = BigQuery(path_to_bq)
        self.__access_token = access_token
        self.date_range = slice_date_on_period(date_from, date_to, 1)
        self.fields = 'ym:s:visitID,ym:s:counterID,ym:s:date,ym:s:isNewUser,ym:s:clientID,ym:s:pageViews,' \
                      'ym:s:visitDuration,ym:s:bounce,ym:s:regionCity,ym:s:UTMCampaign,ym:s:UTMContent,' \
                      'ym:s:UTMMedium,ym:s:UTMSource,ym:s:UTMTerm,ym:s:deviceCategory'

        self.report_dict = {
            "YMLogs": {
                "fields": {
                    "ym_s_visitID": {"type": "INTEGER", "mode": "NULLABLE", "description": "Visit ID :INTEGER"},
                    "ym_s_counterID": {"type": "INTEGER", "mode": "NULLABLE", "description": "Counter ID :INTEGER"},
                    "ym_s_date": {"type": "DATE", "mode": "NULLABLE", "description": "Date :DATE"},
                    "ym_s_isNewUser": {"type": "INTEGER", "mode": "NULLABLE", "description": "Is new user :INTEGER"},
                    "ym_s_clientID": {"type": "STRING", "mode": "NULLABLE", "description": "YA client ID :STRING"},
                    "ym_s_pageViews": {"type": "INTEGER", "mode": "NULLABLE", "description": "Page views :INTEGER"},
                    "ym_s_visitDuration": {"type": "FLOAT", "mode": "NULLABLE", "description": "Visit duration :FLOAT"},
                    "ym_s_bounces": {"type": "INTEGER", "mode": "NULLABLE", "description": "Bounce :INTEGER"},
                    "ym_s_regionCity": {"type": "STRING", "mode": "NULLABLE", "description": "Region city :STRING"},
                    "ym_s_UTMCampaign": {"type": "STRING", "mode": "NULLABLE", "description": "UTMCampaign :STRING"},
                    "ym_s_UTMContent": {"type": "STRING", "mode": "NULLABLE", "description": "UTMContent :STRING"},
                    "ym_s_UTMMedium": {"type": "STRING", "mode": "NULLABLE", "description": "UTMMedium :STRING"},
                    "ym_s_UTMSource": {"type": "STRING", "mode": "NULLABLE", "description": "UTMSource :STRING"},
                    "ym_s_UTMTerm": {"type": "STRING", "mode": "NULLABLE", "description": "UTMTerm :STRING"}}}}

        self.tables_with_schema, self.fields = create_fields(client_name, "YMLogs", self.report_dict, counter)

        self.bq.check_or_create_data_set(self.data_set_id)
        self.bq.check_or_create_tables(self.tables_with_schema, self.data_set_id)

    def __request(self, method, request_type, **kwargs):
        headers = {"Authorization": 'OAuth ' + self.__access_token,
                   "Host": 'api-metrika.yandex.net',
                   'Content-Type': 'application/x-yametrika+json',
                   'date1': self.date_from}
        params = kwargs
        if 'oauth_token' not in params:
            params['oauth_token'] = self.__access_token
        response = requests.request(request_type, self.__request_url + method, params=params, headers=headers)
        return response

    def evaluate(self):
        method = "logrequests/evaluate/"
        evaluate_response = self.__request(method, request_type="GET", date1=self.date_from, date2=self.date_to,
                                           fields=self.fields, source=self.__source).json()
        return evaluate_response

    def get_request(self, date_from, date_to):
        self.date_from = date_from
        self.date_to = date_to
        request_id = self.logrequestID()
        parts = self.log_requests(request_id)
        data = self.download(request_id, parts)
        return data

    def logrequestID(self):
        method = "logrequests/"
        log_request_id_response = self.__request(method, request_type="POST", date1=self.date_from, date2=self.date_to,
                                                 fields=self.fields, source=self.__source).json()
        return log_request_id_response['log_request']['request_id']

    def log_requests(self, request_id):
        method = f"logrequest/{request_id}"
        log_requests_response = self.__request(method, request_type="GET").json()
        if log_requests_response['log_request']['status'] == 'created':
            time.sleep(30)
            return self.log_requests(request_id)
        return log_requests_response['log_request']['parts']

    def download(self, request_id, parts):
        all_data = []
        for part in parts:
            part_id = part['part_number']
            method = f"logrequest/{request_id}/part/{part_id}/download/"
            download = self.__request(method, request_type="GET")
            download_data = self.__get_data(download)
            all_data += download_data
        return all_data

    def __get_data(self, response):
        data_in_string = response.text.split('\n')
        get_data_list = []
        for string in data_in_string:
            get_data_list.append(string.split('\t'))
        df = pd.DataFrame(get_data_list[1:-1], columns=get_data_list[0])
        result = list(df.T.to_dict().values())
        return result

    def get_report(self):
        for date_from, date_to in self.date_range:
            request_result = self.get_request(date_from, date_to)
            request_result_df = pd.DataFrame(request_result).fillna(0)
            insert_data = self.bq.data_to_insert(request_result_df, self.fields, self.data_set_id,
                                                 f"{self.client_name}_YMLogs_{self.counter_id}_YMLogs", "%Y-%m-%d")
            assert insert_data == [], "Data not insert"
        return []
