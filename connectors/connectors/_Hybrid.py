import requests, json, sys
from datetime import datetime, timedelta
from connectors.connectors._Utils import create_fields
from connectors.connectors._BigQuery import BigQuery
import pandas as pd


class Hybrid:
    def __init__(self, client_id, client_secret, client_name, path_to_json, path_to_bq, date_from, date_to):
        self.client_id = client_id
        self.client_secret = client_secret
        self.client_name = client_name
        self.path_to_json = path_to_json
        self.data_set_id = f"{client_name}_Hybrid"
        self.path_to_bq = path_to_bq
        self.bq = BigQuery(path_to_bq)
        self.date_from = date_from
        self.date_to = date_to
        self.url = "https://api.hybrid.ru/"
        
        self.report_dict = {
            "CAMPAIGNS": {
                "fields": {
                    "Id": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignID :STRING"},
                    "Name": {"type": "STRING", "mode": "NULLABLE", "description": "Campaign name :STRING"}
                }
            },

            "CAMPAIGN_STAT": {
                "fields": {
                    "Day": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "ImpressionCount": {"type": "INTEGER", "mode": "NULLABLE", "description": "Impressions :INTEGER"},
                    "ClickCount": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"},
                    "Reach": {"type": "INTEGER", "mode": "NULLABLE", "description": "Reach :INTEGER"},
                    "CTR": {"type": "FLOAT", "mode": "NULLABLE", "description": "CTR :FLOAT"},
                    "id": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignID :STRING"}
                }
            },

            "ADVERTISER_STAT": {
                "fields": {
                    "Day": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "ImpressionCount": {"type": "INTEGER", "mode": "NULLABLE", "description": "Impressions :INTEGER"},
                    "ClickCount": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"},
                    "Reach": {"type": "INTEGER", "mode": "NULLABLE", "description": "Reach :INTEGER"},
                    "CTR": {"type": "FLOAT", "mode": "NULLABLE", "description": "CTR :FLOAT"}
                }
            }
        }
        self.tables_with_schema, self.fields = create_fields(client_name, "Hybrid", self.report_dict, client_id)

        self.bq.check_or_create_data_set(self.data_set_id)
        self.bq.check_or_create_tables(self.tables_with_schema, self.data_set_id)
        
    def hybrid_auth(self, **kwargs):
        body = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret
                }
        body.update(kwargs)
        keys = requests.post(self.url + 'token', body).json()
        keys['token_life'] = datetime.timestamp(datetime.now() + timedelta(seconds=keys['expires_in']))
        json.dump(keys, open(self.path_to_json + f"{self.client_name}.json", "w"))
        return keys['access_token']
    
    def check_token_expires(self):
        try:
            keys = json.load(open(self.path_to_json + f"{self.client_name}.json", "r"))
            token_life_remaining = (datetime.fromtimestamp(keys['token_life']) - datetime.now()).seconds
            if token_life_remaining <= 60:
                access_token = self.hybrid_auth(refresh_token=keys['refresh_token'])
                return access_token
            else:
                return keys['access_token']
        except FileNotFoundError:
            access_token = self.hybrid_auth()
            return access_token
    
    def get_request(self, method, **kwargs):
        access_token = self.check_token_expires()
        headers = {"Authorization": f"Bearer {access_token}"}
        response = requests.get(self.url + method, headers=headers, params=kwargs).json()
        return response
    
    def get_campaigns(self):
        campaigns = self.get_request("v3.0/advertiser/campaigns")
        return campaigns
    
    def get_campaign_stat(self, campaign_ids_list):
        total_stat = []
        for ids in campaign_ids_list:
            params = {"from": self.date_from, "to": self.date_to, "campaignId": ids}
            stat = self.get_request("v3.0/campaign/Day", **params)
            if list(stat.keys()) != ['Statistic', 'Total']:
                sys.exit(list(stat.keys()))
            for element in stat['Statistic']:
                element['id'] = ids
                total_stat.append(element)
        return total_stat
    
    def get_advertiser_stat(self):
        params = {"from": self.date_from, "to": self.date_to}
        stat = self.get_request("v3.0/advertiser/Day", **params)
        if list(stat.keys()) != ['Statistic', 'Total']:
            sys.exit(list(stat.keys()))
        return stat['Statistic']

    def get_hybrid_report(self):
        campaigns = self.get_campaigns()
        campaign_df = pd.DataFrame(campaigns)
        campaign_ids = campaign_df['Id'].tolist()
        self.bq.insert_difference(campaign_df, self.fields, self.data_set_id,
                                  f"{self.client_name}_Hybrid_CAMPAIGNS", 'id', 'id', "%Y-%m-%d")

        campaign_stat = self.get_campaign_stat(campaign_ids)
        campaign_stat_df = pd.DataFrame(campaign_stat)
        self.bq.data_to_insert(campaign_stat_df, self.fields, self.data_set_id,
                               f"{self.client_name}_Hybrid_CAMPAIGN_STAT", "%Y-%m-%d")

        advertiser_stat = self.get_advertiser_stat()
        advertiser_stat_df = pd.DataFrame(advertiser_stat)
        self.bq.data_to_insert(advertiser_stat_df, self.fields, self.data_set_id,
                               f"{self.client_name}_Hybrid_ADVERTISER_STAT", "%Y-%m-%d")

        return []

