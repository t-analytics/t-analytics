from apiclient.discovery import build
from google.oauth2 import service_account
import time, io
import pandas as pd
from datetime import datetime
from connectors.connectors._Utils import create_fields_ga
from pprint import pprint


class DoubleClickCManager:
    def __init__(self, path_to_json, client_name, account_id):
        self.account_id = account_id
        self.KEY_FILE_LOCATION = path_to_json
        self.SCOPES = ['https://www.googleapis.com/auth/dfareporting', 'https://www.googleapis.com/auth/dfatrafficking',
                       'https://www.googleapis.com/auth/ddmconversions']
        self.credentials = service_account.Credentials.from_service_account_file(self.KEY_FILE_LOCATION)
        self.scoped_credentials = self.credentials.with_scopes(self.SCOPES)
        self.analytics = build('dfareporting', 'v3.3', credentials=self.scoped_credentials)

        self.report_dict = {
            "CONVERSIONS_BY_CREATIVE":
                {
                    "metrics": {
                        "dfa:clicks": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"},
                        "dfa:impressions": {"type": "INTEGER", "mode": "NULLABLE",
                                            "description": "Impressions :INTEGER"},
                        "dfa:activityClickThroughConversions": {"type": "INTEGER", "mode": "NULLABLE",
                                                                "description":
                                                                    "Activity Click Through Conversions :INTEGER"},
                        "dfa:activityViewThroughConversions": {"type": "INTEGER", "mode": "NULLABLE",
                                                               "description":
                                                                   "Activity View Through Conversions :INTEGER"},
                        "dfa:totalConversions": {"type": "INTEGER", "mode": "NULLABLE",
                                                 "description": "Total Conversions :INTEGER"}
                    },
                    "dimensions": {
                        "dfa:date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                        "dfa:advertiserId": {"type": "STRING", "mode": "NULLABLE",
                                             "description": "Advertiser Id :STRING"},
                        "dfa:advertiser": {"type": "STRING", "mode": "NULLABLE", "description": "Advertiser :STRING"},
                        "dfa:campaignId": {"type": "STRING", "mode": "NULLABLE", "description": "Campaign Id :STRING"},
                        "dfa:campaign": {"type": "STRING", "mode": "NULLABLE", "description": "Campaign :STRING"},
                        "dfa:siteId": {"type": "STRING", "mode": "NULLABLE", "description": "Site Id :STRING"},
                        "dfa:site": {"type": "STRING", "mode": "NULLABLE", "description": "Site :STRING"},
                        "dfa:placementId": {"type": "STRING", "mode": "NULLABLE",
                                            "description": "Placement Id :STRING"},
                        "dfa:placement": {"type": "STRING", "mode": "NULLABLE", "description": "Placement :STRING"},
                        "dfa:creative": {"type": "STRING", "mode": "NULLABLE", "description": "Creative :STRING"},
                        "dfa:creativeId": {"type": "STRING", "mode": "NULLABLE", "description": "Creative Id :STRING"},
                        "dfa:activity": {"type": "STRING", "mode": "NULLABLE", "description": "Activity :STRING"},
                        "dfa:adType": {"type": "STRING", "mode": "NULLABLE", "description": "Ad Type :STRING"},
                        "dfa:adId": {"type": "STRING", "mode": "NULLABLE", "description": "Ad Id :STRING"},
                        "dfa:ad": {"type": "STRING", "mode": "NULLABLE", "description": "Ad :STRING"},
                    }
                },
            "CROSS_DEVICE_CONVERSIONS_BY_AD":
                {
                    "metrics": {
                        "dfa:clicks": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"},
                        "dfa:impressions": {"type": "INTEGER", "mode": "NULLABLE",
                                            "description": "Impressions :INTEGER"},
                        "dfa:crossDeviceClickThroughConversions": {"type": "INTEGER", "mode": "NULLABLE",
                                                                   "description":
                                                                       "Cross Device Click Through Conversions :INTEGER"
                                                                   },
                        "dfa:crossDeviceTotalConversions": {"type": "INTEGER", "mode": "NULLABLE",
                                                            "description": "Cross Device Total Conversions :INTEGER"},
                        "dfa:crossDeviceViewThroughConversions": {"type": "INTEGER", "mode": "NULLABLE",
                                                                  "description":
                                                                      "Cross Device View Through Conversions :INTEGER"},
                        "dfa:activityClickThroughConversions": {"type": "INTEGER", "mode": "NULLABLE",
                                                                "description":
                                                                    "Activity Click Through Conversions :INTEGER"},
                        "dfa:activityViewThroughConversions": {"type": "INTEGER", "mode": "NULLABLE",
                                                               "description":
                                                                   "Activity View Through Conversions :INTEGER"},
                        "dfa:totalConversions": {"type": "INTEGER", "mode": "NULLABLE",
                                                 "description": "Total Conversions :INTEGER"}
                    },
                    "dimensions": {
                        "dfa:date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                        "dfa:advertiserId": {"type": "STRING", "mode": "NULLABLE",
                                             "description": "Advertiser Id :STRING"},
                        "dfa:advertiser": {"type": "STRING", "mode": "NULLABLE",
                                           "description": "Advertiser :STRING"},
                        "dfa:campaignId": {"type": "STRING", "mode": "NULLABLE", "description": "Campaign Id :STRING"},
                        "dfa:campaign": {"type": "STRING", "mode": "NULLABLE", "description": "Campaign :STRING"},
                        "dfa:siteId": {"type": "STRING", "mode": "NULLABLE", "description": "Site Id :STRING"},
                        "dfa:site": {"type": "STRING", "mode": "NULLABLE", "description": "Site :STRING"},
                        "dfa:placementId": {"type": "STRING", "mode": "NULLABLE",
                                            "description": "Placement Id :STRING"},
                        "dfa:placement": {"type": "STRING", "mode": "NULLABLE", "description": "Placement :STRING"},
                        "dfa:activity": {"type": "STRING", "mode": "NULLABLE", "description": "Activity :STRING"},
                        "dfa:adType": {"type": "STRING", "mode": "NULLABLE", "description": "Ad Type :STRING"},
                        "dfa:adId": {"type": "STRING", "mode": "NULLABLE", "description": "Ad Id :STRING"},
                        "dfa:ad": {"type": "STRING", "mode": "NULLABLE", "description": "Ad :STRING"},
                    }
                },
            "REACH_AND_FREQUENCY_BY_CAMPAIGN":
                {
                    "metrics": {
                        "dfa:cookieReachImpressionReach": {"type": "INTEGER", "mode": "NULLABLE",
                                                           "description": "Cookie Reach Impression Reach :INTEGER"},
                        "dfa:cookieReachAverageImpressionFrequency": {"type": "FLOAT", "mode": "NULLABLE",
                                                                      "description":
                                                                          "Cookie Reach Average Impression Frequency "
                                                                          ":FLOAT"},
                        "dfa:cookieReachClickReach": {"type": "INTEGER", "mode": "NULLABLE",
                                                      "description": "Cookie Reach Click Reach :INTEGER"},
                        "dfa:cookieReachTotalReach": {"type": "INTEGER", "mode": "NULLABLE",
                                                      "description": "Cookie Reach Total Reach :INTEGER"}
                    },
                    "dimensions": {
                        "dfa:date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                        "dfa:advertiserId": {"type": "STRING", "mode": "NULLABLE",
                                             "description": "Advertiser Id :STRING"},
                        "dfa:advertiser": {"type": "STRING", "mode": "NULLABLE", "description": "Advertiser :STRING"},
                        "dfa:campaignId": {"type": "STRING", "mode": "NULLABLE",
                                           "description": "Campaign Id :STRING"},
                        "dfa:campaign": {"type": "STRING", "mode": "NULLABLE", "description": "Campaign :STRING"}
                    }
                },
            "REACH_AND_FREQUENCY_BY_PLACEMENT":
                {
                    "metrics": {
                        "dfa:clicks": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"},
                        "dfa:impressions": {"type": "INTEGER", "mode": "NULLABLE",
                                            "description": "Impressions :INTEGER"},
                        "dfa:cookieReachImpressionReach": {"type": "INTEGER", "mode": "NULLABLE",
                                                           "description": "Cookie Reach Impression Reach :INTEGER"},
                        "dfa:cookieReachAverageImpressionFrequency": {"type": "FLOAT", "mode": "NULLABLE",
                                                                      "description":
                                                                          "Cookie Reach Average Impression Frequency "
                                                                          ":FLOAT"},
                        "dfa:cookieReachClickReach": {"type": "INTEGER", "mode": "NULLABLE",
                                                      "description": "Cookie Reach Click Reach :INTEGER"},
                        "dfa:cookieReachTotalReach": {"type": "INTEGER", "mode": "NULLABLE",
                                                      "description": "Cookie Reach Total Reach :INTEGER"}
                    },
                    "dimensions": {
                        "dfa:date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                        "dfa:advertiser": {"type": "STRING", "mode": "NULLABLE", "description": "Advertiser :STRING"},
                        "dfa:advertiserId": {"type": "STRING", "mode": "NULLABLE",
                                             "description": "Advertiser Id :STRING"},
                        "dfa:campaign": {"type": "STRING", "mode": "NULLABLE", "description": "Campaign :STRING"},
                        "dfa:campaignId": {"type": "STRING", "mode": "NULLABLE", "description": "Campaign Id :STRING"},
                        "dfa:placement": {"type": "STRING", "mode": "NULLABLE", "description": "Placement :STRING"},
                        "dfa:placementId": {"type": "STRING", "mode": "NULLABLE", "description": "Placement Id :STRING"
                                            },
                        "dfa:siteId": {"type": "STRING", "mode": "NULLABLE", "description": "Site Id :STRING"},
                        "dfa:site": {"type": "STRING", "mode": "NULLABLE", "description": "Site :STRING"}
                    }
                }
        }

        # self.tables_with_schema, self.fields = create_fields_ga(client_name, "DCampaignManager", self.report_dict,
        #                                                         account_id)

    def create_body(self, date_from, date_to, metric_param, dimension_param, user_profiles_id, report_type='STANDARD',
                    dimension_filter=None):
        if dimension_filter is None:
            dimension_filter = []
            dimension_param = self.create_params(dimension_param)
        criteria = "criteria"
        body = {
            "name": datetime.strftime(datetime.now(), '%Y%m%dT%H%M%S'),
            "fileName": datetime.strftime(datetime.now(), '%Y%m%dT%H%M%S'),
            "format": "CSV",
            "kind": "dfareporting#report",
            "accountId": self.account_id,
            "ownerProfileId": user_profiles_id
        }
        if report_type == "REACH":
            criteria = 'reachCriteria'
        body.update({criteria: {
            "dimensionFilters": dimension_filter,
            "dimensions": dimension_param,
            "dateRange": {
                "endDate": date_to,
                "startDate": date_from
                    },
            "metricNames": metric_param}})
        body['type'] = report_type
        return body

    def create_params(self, list_of_params):
        params_dict = []
        for param in list_of_params:
            params_dict.append({"name": param})
        return params_dict

    def get_advertisers(self, user_profiles_id, next_page_token='', advertisers_dict=None):

        if advertisers_dict is None:
            advertisers_dict = {}
        advertisers = self.analytics.advertisers().list(profileId=user_profiles_id, pageToken=next_page_token).execute()
        if advertisers['advertisers']:
            for advertiser in advertisers['advertisers']:
                advertisers_dict.setdefault(advertiser['name'], [])
                advertisers_dict[advertiser['name']].append({
                    "id": advertiser['id'],
                    "floodlightConfigurationId": advertiser['floodlightConfigurationId'],
                    "accountId": advertiser['accountId'],
                    "advertiserGroupId": advertiser.get('advertiserGroupId', "<not set>")})

            if advertisers.get('nextPageToken', False):
                next_page_token = advertisers['nextPageToken']
                return self.get_advertisers(user_profiles_id, next_page_token=next_page_token,
                                            advertisers_dict=advertisers_dict)
        return advertisers_dict

    def delete_report(self, user_profiles_id, report_id):
        r = self.analytics.reports().delete(profileId=user_profiles_id, reportId=report_id).execute()
        return r

    def get_user_profiles(self):
        user_profiles = self.analytics.userProfiles().list().execute()
        user_profiles_dict = {}
        user_profiles = user_profiles['items']

        for user in user_profiles:
            user_profiles_dict.setdefault(user['userName'], [])
            user_profiles_dict[user['userName']].append({"profileId": user['profileId'], "accountId": user['accountId'],
                                                         "accountName": user['accountName']})

        return user_profiles_dict

    def get_accounts(self, user_profiles_id):
        accounts = self.analytics.accounts().list(profileId=user_profiles_id).execute()
        accounts_dict = {}
        accounts = accounts['accounts']

        for account in accounts:
            accounts_dict[account['name']] = account['id']

        return accounts_dict

    def get_campaigns(self, user_profiles_id, advertiser_ids_list, next_page_token='', campaigns_dict=None):
        if campaigns_dict is None:
            campaigns_dict = {}
        campaigns = self.analytics.campaigns().list(profileId=user_profiles_id, pageToken=next_page_token,
                                                    advertiserIds=advertiser_ids_list).execute()
        if campaigns['campaigns']:
            for campaign in campaigns['campaigns']:
                campaigns_dict.setdefault(campaign['name'], [])
                campaigns_dict[campaign['name']].append({"id": campaign['id'], "accountId": campaign['accountId'],
                                                         "advertiserId": campaign['advertiserId'],
                                                         "advertiserGroupId":
                                                             campaign.get('advertiserGroupId', "<not set>")})

            if campaigns.get('nextPageToken', False):
                next_page_token = campaigns['nextPageToken']
                return self.get_campaigns(user_profiles_id, advertiser_ids_list,
                                          next_page_token=next_page_token, campaigns_dict=campaigns_dict)
        return campaigns_dict

    def get_file_id(self, user_profiles_id, report_id):
        files = self.analytics.reports().files().list(profileId=user_profiles_id, reportId=report_id).execute()
        if files['items'][0]['status'] != "REPORT_AVAILABLE":
            time.sleep(5)
            return self.get_file_id(user_profiles_id, report_id)
        file_id = files['items'][0]['id']
        return file_id

    def get_media(self, dimension_filter, file_id, user_profiles_id, report_id):
        skip_rows = 10
        if dimension_filter is not None:
            skip_rows = 11
        media = self.analytics.reports().files().get_media(fileId=file_id, profileId=user_profiles_id,
                                                           reportId=report_id).execute()
        report_df = pd.read_csv(io.StringIO(media.decode("utf8")), skiprows=skip_rows, skipfooter=1)
        return report_df

    def get_report(self, user_profiles_id, date_from, date_to, metric, dimension, report_type='STANDARD',
                   dimension_filter=None):

        body = self.create_body(date_from, date_to, metric, dimension, user_profiles_id, report_type, dimension_filter)
        report = self.analytics.reports().insert(profileId=user_profiles_id, body=body).execute()
        self.analytics.reports().run(profileId=user_profiles_id, reportId=report['id']).execute()
        file_id = self.get_file_id(user_profiles_id, report['id'])
        media_data = self.get_media(dimension_filter, file_id, user_profiles_id, report['id'])
        self.delete_report(user_profiles_id, report['id'])
        return media_data
