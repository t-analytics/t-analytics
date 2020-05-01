from connectors._Utils import my_slice, create_fields
import requests, time


class VKApp:
    def __init__(self, access_token, account_id, client_id, client_name):
        self.__access_token = access_token
        self.__v = "5.101"
        self.__method_url = "https://api.vk.com/method/"
        self.account_id = account_id
        self.client_id = client_id

        self.report_dict = {
            "CAMPAIGNS": {
                "fields": {
                    "id": {"type": "STRING", "mode": "NULLABLE", "description": "Campaign ID :STRING"},
                    "type": {"type": "STRING", "mode": "NULLABLE", "description": "Campaign type :STRING"},
                    "name": {"type": "STRING", "mode": "NULLABLE", "description": "Campaign name :STRING"}}},

            "ADS": {
                "fields": {
                    "id": {"type": "STRING", "mode": "NULLABLE", "description": "Ad ID :STRING"},
                    "campaign_id": {"type": "STRING", "mode": "NULLABLE", "description": "Campaign ID :STRING"},
                    "goal_type": {"type": "STRING", "mode": "NULLABLE", "description": "Goal type :STRING"},
                    "cost_type": {"type": "STRING", "mode": "NULLABLE", "description": "Cost type :STRING"},
                    "category1_id": {"type": "STRING", "mode": "NULLABLE", "description": "Category1 ID :STRING"},
                    "category2_id": {"type": "STRING", "mode": "NULLABLE", "description": "Category2 ID :STRING"},
                    "age_restriction": {"type": "STRING", "mode": "NULLABLE", "description": "Age restriction :STRING"},
                    "name": {"type": "STRING", "mode": "NULLABLE", "description": "Ad name :STRING"},
                    "ad_format": {"type": "STRING", "mode": "NULLABLE", "description": "Ad format :STRING"},
                    "ad_platform": {"type": "STRING", "mode": "NULLABLE", "description": "Ad platform :STRING"}}},

            "CAMPAIGN_STAT": {
                "fields": {
                    "day": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "spent": {"type": "FLOAT", "mode": "NULLABLE", "description": "Spent :FLOAT"},
                    "impressions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Impressions :INTEGER"},
                    "clicks": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"},
                    "reach": {"type": "INTEGER", "mode": "NULLABLE", "description": "Reach :INTEGER"},
                    "join_rate": {"type": "INTEGER", "mode": "NULLABLE", "description": "Join to group :INTEGER"},
                    "campaign_id": {"type": "STRING", "mode": "NULLABLE", "description": "Campaign ID :STRING"},
                    'lead_form_sends': {"type": "INTEGER", "mode": "NULLABLE", "description": "Lead form :INTEGER"},
                    'goals': {"type": "INTEGER", "mode": "NULLABLE", "description": "Goals :INTEGER"}}},

            "ADS_STAT": {
                "fields": {
                    "ad_id": {"type": "STRING", "mode": "NULLABLE", "description": "Ad ID :STRING"},
                    "clicks": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"},
                    "day": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "impressions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Impressions :INTEGER"},
                    "join_rate": {"type": "INTEGER", "mode": "NULLABLE", "description": "Join to group :INTEGER"},
                    "reach": {"type": "INTEGER", "mode": "NULLABLE", "description": "Reach :INTEGER"},
                    "spent": {"type": "FLOAT", "mode": "NULLABLE", "description": "Spent :FLOAT"},
                    'lead_form_sends': {"type": "INTEGER", "mode": "NULLABLE", "description": "Lead form :INTEGER"},
                    'goals': {"type": "INTEGER", "mode": "NULLABLE", "description": "Goals :INTEGER"}}},

            "SEX_STAT": {
                "fields": {
                    "impressions_rate": {"type": "FLOAT", "mode": "NULLABLE", "description": "Imp rate :FLOAT"},
                    "clicks_rate": {"type": "FLOAT", "mode": "NULLABLE", "description": "Clicks rate :FLOAT"},
                    "value": {"type": "STRING", "mode": "NULLABLE", "description": "Param value :STRING"},
                    "ad_id": {"type": "STRING", "mode": "NULLABLE", "description": "Ad ID :STRING"},
                    "day": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"}}},

            "AGE_STAT": {
                "fields": {
                    "impressions_rate": {"type": "FLOAT", "mode": "NULLABLE", "description": "Imp rate :FLOAT"},
                    "clicks_rate": {"type": "FLOAT", "mode": "NULLABLE", "description": "Clicks rate :FLOAT"},
                    "value": {"type": "STRING", "mode": "NULLABLE", "description": "Param value :STRING"},
                    "ad_id": {"type": "STRING", "mode": "NULLABLE", "description": "Ad ID :STRING"},
                    "day": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"}}},

            "SEX_AGE_STAT": {
                "fields": {
                    "impressions_rate": {"type": "FLOAT", "mode": "NULLABLE", "description": "Imp rate :FLOAT"},
                    "clicks_rate": {"type": "FLOAT", "mode": "NULLABLE", "description": "Clicks rate :FLOAT"},
                    "value": {"type": "STRING", "mode": "NULLABLE", "description": "Param value :STRING"},
                    "ad_id": {"type": "STRING", "mode": "NULLABLE", "description": "Ad ID :STRING"},
                    "day": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"}}},

            "CITIES_STAT": {
                "fields": {
                    "impressions_rate": {"type": "FLOAT", "mode": "NULLABLE", "description": "Imp rate :FLOAT"},
                    "clicks_rate": {"type": "FLOAT", "mode": "NULLABLE", "description": "Clicks rate :FLOAT"},
                    "value": {"type": "STRING", "mode": "NULLABLE", "description": "Param value :STRING"},
                    "ad_id": {"type": "STRING", "mode": "NULLABLE", "description": "Ad ID :STRING"},
                    "day": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "name": {"type": "STRING", "mode": "NULLABLE", "description": "City name :STRING"}}}}

        self.tables_with_schema, self.fields = create_fields(client_name, "VKontakte", self.report_dict, client_id)

    def __request(self, method, request_type='get', **kwargs):
        if "access_token" not in kwargs:
            params = {'access_token': self.__access_token, 'v': self.__v}
            for key, value in kwargs.items():
                params[key] = value
        else:
            params = kwargs
        request_data = requests.request(request_type, self.__method_url + method, params=params)
        return self.__get_errors(request_data)

    def __get_errors(self, response):
        if response.status_code == 200:
            if 'error' in response.json():
                error_msg = response.json()['error']['error_msg']
                error_code = response.json()['error']['error_code']
                raise Exception(f"Error code: {error_code}. Error msg: {error_msg}")
            elif 'response' in response.json():
                return response.json()['response']
            else:
                raise Exception("Это что-то новое")

        else:
            raise Exception("Status code not 200", response.status_code, response.content)

    def pop_keys(self, keys, data):
        result = []
        for one_dict in data:
            middle = one_dict.copy()
            for element in one_dict.keys():
                if element not in keys:
                    middle.pop(element)
            result.append(middle)

        return result

    def get_campaigns(self):
        campaigns = self.__request('ads.getCampaigns', request_type='get', account_id=self.account_id,
                                   include_deleted=1, client_id=self.client_id)
        keys = ["id", "type", "name"]
        campaigns = self.pop_keys(keys, campaigns)

        return campaigns

    def get_ads(self, campaign_ids):
        campaign_ids = my_slice(campaign_ids, 100)
        ads_list = []
        for campaign_ids_list in campaign_ids:
            campaign_ids_string = ",".join([str(x) for x in campaign_ids_list])
            ads = self.__request('ads.getAds', request_type='get', account_id=self.account_id,
                                 campaign_ids=f"[{campaign_ids_string}]", client_id=self.client_id)
            ads_list += ads
            time.sleep(2)
        keys = ["id", "campaign_id", "goal_type", "cost_type", "category1_id", "category2_id", "age_restriction",
                "name", "ad_format", "ad_platform"]
        ads = self.pop_keys(keys, ads_list)
        return ads

    def get_demographics(self, demographics_list_ids, date_from, date_to, limit=2000):
        data_keys = {"sex": [], "age": [], "sex_age": [], "cities": []}
        demographics_list = my_slice(demographics_list_ids, limit)
        for demographics_id_list in demographics_list:
            demographics_ids_string = ",".join([str(x) for x in demographics_id_list])
            demographics_response = self.__request('ads.getDemographics', request_type='get',
                                                   account_id=self.account_id,
                                                   ids_type="ad", ids=demographics_ids_string, period="day",
                                                   date_from=date_from, date_to=date_to)

            for one in demographics_response:
                for element in one['stats']:
                    for key, value in element.items():
                        if key in ['sex', 'age', 'sex_age', 'cities']:
                            for one_element in value:
                                arr = one_element.copy()
                                arr[one['type'] + '_id'] = one['id']
                                arr['day'] = element['day']
                                data_keys[key].append(arr)

        return data_keys['sex'], data_keys['age'], data_keys['sex_age'], data_keys['cities']

    def get_day_stats(self, ids_type, list_of_ids, date_from, date_to, limit=2000):
        day_stat_list = []
        ids_list = my_slice(list_of_ids, limit)
        for ids_stat_list in ids_list:
            ids_stat_string = ",".join([str(x) for x in ids_stat_list])
            day_stats = self.__request('ads.getStatistics', request_type='get', account_id=self.account_id,
                                       ids_type=ids_type, ids=ids_stat_string, period="day", date_from=date_from,
                                       date_to=date_to)
            for DayStat in day_stats:
                for stat in DayStat['stats']:
                    stat[DayStat['type'] + "_id"] = DayStat['id']
                    day_stat_list.append(stat)
            time.sleep(2)
        return day_stat_list
