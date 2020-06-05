import re
import sys
from datetime import datetime, timedelta
from pprint import pprint
import pandas as pd

import requests, json, time
from connectors.connectors._Utils import create_fields, my_slice, expand_dict


class Facebook:
    def __init__(self, token, account, client_name):
        self.token = token
        self.account = account

        self.report_dict = {

            "ADS_STAT": {
                "fields": {
                    "clicks": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "impressions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "spend": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "video_p100_watched_actions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "video_p25_watched_actions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "video_p50_watched_actions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "video_30_sec_watched_actions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "video_p75_watched_actions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "video_p95_watched_actions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "video_thruplay_watched_actions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "video_avg_time_watched_actions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "conversions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "ad_id": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "ad_name": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "campaign_id": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "campaign_name": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "adset_id": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "adset_name": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "date_start": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"}}},

            "CAMPAIGN_STAT": {
                "fields": {
                    "clicks": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "impressions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "reach": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "spend": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "date_start": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "campaign_id": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "campaign_name": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "video_p100_watched_actions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "video_p25_watched_actions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "video_p50_watched_actions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "video_30_sec_watched_actions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "video_p75_watched_actions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "video_p95_watched_actions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "video_thruplay_watched_actions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "video_avg_time_watched_actions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "conversions": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"}}},

            "ADSETS_STAT": {
                "fields": {
                    "reach": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "ad_name": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "adset_id": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "adset_name": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "campaign_id": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "campaign_name": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "ad_id": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "frequency": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"},
                    "date_start": {"type": "STRING", "mode": "NULLABLE", "description": "Copies :STRING"}}}}

    def check_headers(self, _check_headers):
        if (_check_headers['total_cputime'] >= 50) or (_check_headers['total_time'] >= 50):
            print("Пришло время для сна.")
            time.sleep(60*60)

    def get_statistics(self, date_from, date_to):
        gs_result = []
        gs_fields = "clicks,impressions,spend,video_p100_watched_actions,video_p25_watched_actions," \
                 "video_p50_watched_actions,video_30_sec_watched_actions,video_p75_watched_actions," \
                 "video_p95_watched_actions,video_thruplay_watched_actions,video_avg_time_watched_actions," \
                 "conversions,ad_id,ad_name,campaign_id,campaign_name,adset_id,adset_name,actions,unique_actions"

        gs_url_method = f"v6.0/{self.account}/insights?"

        date_from_dt = datetime.strptime(date_from, "%Y-%m-%d")
        date_to_dt = datetime.strptime(date_to, "%Y-%m-%d")
        gs_list_of_days = [datetime.strftime(date_from_dt + timedelta(days=x), "%Y-%m-%d") for x in
                           range(0, (date_to_dt - date_from_dt).days + 1)]
        gs_list_of_days_slice = my_slice(gs_list_of_days, 30)

        gs_count_list_of_days_slice = len(gs_list_of_days_slice)
        for gs_num, gs_days in enumerate(gs_list_of_days_slice, 1):
            gs_batch = []
            for gs_day in gs_days:
                get_statistic_time_range = "{'since':'%s','until':'%s'}" % (gs_day, gs_day)
                gs_batch.append(self.create_betch(gs_url_method, time_range=get_statistic_time_range, fields=gs_fields,
                                                  limit=50, level="ad", time_increment=1))

            gs_result_list, headers = self.get_batch_data(gs_batch, [], gs_url_method)
            for gs_element in gs_result_list:
                gs_middle_list = {}
                for gs_element_key, gs_element_value in gs_element.items():
                    if "video_" in gs_element_key:
                        gs_middle_list[gs_element_key] = gs_element_value[0]['value']
                    elif "actions" in gs_element_key:
                        for gs_action_key in gs_element_value:
                            gs_action_key_re = re.sub('[.]', '_', gs_action_key['action_type'])
                            gs_middle_list[gs_action_key_re] = gs_action_key['value']
                    else:
                        gs_middle_list[gs_element_key] = gs_element_value
                gs_result.append(gs_middle_list)
            if gs_num != gs_count_list_of_days_slice:
                self.check_headers(headers)
        return gs_result

    def get_paging_data(self, gpd_result_data, gpd_headers):
        gpd_list = []
        gpd_result_list = []
        for gpd_result in gpd_result_data:
            gpd_middle_data = json.loads(gpd_result['body'])
            for header in gpd_result['headers']:
                if header['name'] == 'X-Business-Use-Case-Usage':
                    gpd_headers = json.loads(header['value'])[self.account[4:]][0]

            try:
                gpd_md = gpd_middle_data['data']
            except KeyError:
                sys.exit(f"middle_data ->{gpd_middle_data}<-, result_data ->{gpd_result_data}<-, result ->{gpd_result}<-")
            if gpd_md:
                for gpd_element in gpd_middle_data['data']:
                    gpd_list.append(gpd_element)
                gpd_result_list.append(gpd_middle_data['paging'])
            else:
                continue

        return gpd_list, gpd_result_list, gpd_headers

    def create_next_paging_request(self, cnpr_result_paging, cnpr_url_method):
        cnpr_batch = []
        for cnpr_next_link in cnpr_result_paging:
            if "next" in cnpr_next_link:
                cnpr_next_params = cnpr_next_link['next'].split('?')[1]
                cnpr_batch.append({"method": "GET", "relative_url": cnpr_url_method + cnpr_next_params})
        return cnpr_batch

    def get_batch_request(self, gbr_batch):
        gbr_jBatch = json.dumps(gbr_batch)
        gbr_params = {"access_token": self.token, "batch": gbr_jBatch}
        gbr_result_data = requests.post("https://graph.facebook.com/", params=gbr_params).json()
        return gbr_result_data

    def get_data_with_filtering(self, gdwf_campaign_ids_slice, method, fields, filter_by, operator, limit=500):
        gdwf_result_list = []
        gdwf_total_result = []
        gdwf_batch = []
        gdwf_url_method = f"v6.0/{self.account}/{method}?"
        gdwf_count_campaign_ids_slice = len(gdwf_campaign_ids_slice)
        for gdwf_num, gdwf_ids_slice in enumerate(gdwf_campaign_ids_slice, 1):
            for gdwf_ids in gdwf_ids_slice:
                gdwf_filtering = "[{field:'%s',operator:'%s',value:%s}]" % (filter_by, operator, gdwf_ids)
                gdwf_batch.append(self.create_betch(gdwf_url_method, filtering=gdwf_filtering, limit=limit,
                                                    fields=fields))

            gdwf_result_list, gdwf_headers = self.get_batch_data(gdwf_batch, gdwf_result_list, gdwf_url_method)
            for gdwf_one in gdwf_result_list:
                gdwf_total_result.append(gdwf_one)
            if gdwf_num != gdwf_count_campaign_ids_slice:
                self.check_headers(gdwf_headers)

        return gdwf_total_result

    def create_betch(self, cb_url_method, **kwargs):
        cb_params = []
        for cb_key, cb_value in kwargs.items():
            cb_params.append(f'{cb_key}={cb_value}')
        params_in_string = '&'.join(cb_params)
        return {"method": "GET", "relative_url": cb_url_method + params_in_string}

    def get_batch_data(self, gbd_batch, gbd_batch_result_list, gbd_url_method):
        gbd_jBatch = json.dumps(gbd_batch)
        gbd_params = {"access_token": self.token, "batch": gbd_jBatch}
        gbd_result = requests.post("https://graph.facebook.com/", params=gbd_params).json()
        gbd_middle_list, gbd_result_paging, gbd_headers = self.get_paging_data(gbd_result, gpd_headers={})
        for gbd_element in gbd_middle_list:
            gbd_batch_result_list.append(gbd_element)
        if gbd_result_paging:
            gbd_batch_result_list, gbd_headers = self.get_other_data(gbd_batch_result_list, gbd_result_paging,
                                                                     gbd_url_method, gbd_headers)
        return gbd_batch_result_list, gbd_headers

    def get_data_no_filtering(self, method, **kwargs):
        result_list = []
        url_method = f"v6.0/{self.account}/{method}?"
        batch = [self.create_betch(url_method, **kwargs)]
        result_list = self.get_batch_data(batch, result_list, url_method)
        return result_list

    def get_other_data(self, other_result_list, result_paging, url_method, headers=None):
        if headers is None:
            headers = {}
        next_pages_batch = self.create_next_paging_request(result_paging, url_method)
        result_data = self.get_batch_request(next_pages_batch)
        middle_list, result_paging, headers = self.get_paging_data(result_data, headers)
        print(headers)
        for element in middle_list:
            other_result_list.append(element)
        if result_paging:
            return self.get_other_data(other_result_list, result_paging, url_method, headers)
        return other_result_list, headers


fb = Facebook("EAAdragdgdwYBAM0wkTum2zFOpBoTnV8Ls4ZCu8ng3b4LU0mdQgcRtGDkJU7uWEdBjBZATVScxArrW9cJxrunyAZBV2EerpIA2pF5EtmoRJxnpFjPoBnDpiYg2XXP9FZBzEk58pcrZAkQTZC96bYZAluaf8EszMmsvIbJh2NB2eP9wZDZD", 'act_384916288880080', "bla")
data = fb.get_statistics("2020-03-16", "2020-03-22")
pprint(data)
