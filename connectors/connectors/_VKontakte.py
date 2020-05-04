from datetime import datetime, timedelta

from connectors.connectors._Utils import my_slice, create_fields
import requests, time


class VKontakte:
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
                    "name": {"type": "STRING", "mode": "NULLABLE", "description": "City name :STRING"}}},

            "POST_REACH": {
                "fields": {
                    "id": {"type": "STRING", "mode": "NULLABLE", "description": "Ad ID :STRING"},
                    "reach_subscribers": {"type": "INTEGER", "mode": "NULLABLE", "description": "Reach subscribers :INTEGER"},
                    "reach_total": {"type": "INTEGER", "mode": "NULLABLE", "description": "Reach total :INTEGER"},
                    "links": {"type": "INTEGER", "mode": "NULLABLE", "description": "Link :INTEGER"},
                    "to_group": {"type": "INTEGER", "mode": "NULLABLE", "description": "To group :INTEGER"},
                    "join_group": {"type": "INTEGER", "mode": "NULLABLE", "description": "Join group :INTEGER"},
                    "report": {"type": "INTEGER", "mode": "NULLABLE", "description": "Report :INTEGER"},
                    "hide": {"type": "INTEGER", "mode": "NULLABLE", "description": "Hide :INTEGER"},
                    "unsubscribe": {"type": "INTEGER", "mode": "NULLABLE", "description": "Unsubscribe :INTEGER"},
                    "video_views_start": {"type": "INTEGER", "mode": "NULLABLE", "description": "Video start :INTEGER"},
                    "video_views_3s": {"type": "INTEGER", "mode": "NULLABLE", "description": "Video 3s :INTEGER"},
                    "video_views_10s": {"type": "INTEGER", "mode": "NULLABLE", "description": "Video 10s :INTEGER"},
                    "video_views_25p": {"type": "INTEGER", "mode": "NULLABLE", "description": "Video 25p :INTEGER"},
                    "video_views_50p": {"type": "INTEGER", "mode": "NULLABLE", "description": "Video 50p :INTEGER"},
                    "video_views_75p": {"type": "INTEGER", "mode": "NULLABLE", "description": "Video 75p :INTEGER"},
                    "video_views_100p": {"type": "INTEGER", "mode": "NULLABLE", "description": "Video 100p :INTEGER"}}},

            "LEAD_FORMS": {
                "fields": {
                    "form_id": {"type": "STRING", "mode": "NULLABLE", "description": "Form ID :STRING"},
                    "group_id": {"type": "STRING", "mode": "NULLABLE", "description": "Group ID :STRING"},
                    "name": {"type": "STRING", "mode": "NULLABLE", "description": "Name :STRING"},
                    "title": {"type": "STRING", "mode": "NULLABLE", "description": "Title :STRING"},
                    "description": {"type": "STRING", "mode": "NULLABLE", "description": "Description :STRING"},
                    "site_link_url": {"type": "STRING", "mode": "NULLABLE", "description": "Site link URL :STRING"},
                    "url": {"type": "STRING", "mode": "NULLABLE", "description": "URL :STRING"}}},

            "LEADS": {
                "fields": {
                    "lead_id": {"type": "STRING", "mode": "NULLABLE", "description": "Lead ID :STRING"},
                    "user_id": {"type": "STRING", "mode": "NULLABLE", "description": "User ID :STRING"},
                    "date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "first_name": {"type": "STRING", "mode": "NULLABLE", "description": "First name :STRING"},
                    "phone_number": {"type": "STRING", "mode": "NULLABLE", "description": "Phone number :STRING"},
                    "patronymic_name": {"type": "STRING", "mode": "NULLABLE", "description": "Patronymic name :STRING"},
                    "last_name": {"type": "STRING", "mode": "NULLABLE", "description": "Last name :STRING"},
                    "email": {"type": "STRING", "mode": "NULLABLE", "description": "Email :STRING"}}},

            "GROUP_ACTIVITY": {
                "fields": {
                    "copies": {"type": "INTEGER", "mode": "NULLABLE", "description": "Copies :INTEGER"},
                    "hidden": {"type": "INTEGER", "mode": "NULLABLE", "description": "Hidden :INTEGER"},
                    "likes": {"type": "INTEGER", "mode": "NULLABLE", "description": "Likes :INTEGER"},
                    "subscribed": {"type": "INTEGER", "mode": "NULLABLE", "description": "Subscribed :INTEGER"},
                    "unsubscribed": {"type": "INTEGER", "mode": "NULLABLE", "description": "Unsubscribed :INTEGER"},
                    "day": {"type": "DATE", "mode": "NULLABLE", "description": "Site link URL :DATE"}}},

            "GROUP_AGE_STAT": {
                "fields": {
                    "value": {"type": "STRING", "mode": "NULLABLE", "description": "Age param :STRING"},
                    "count": {"type": "INTEGER", "mode": "NULLABLE", "description": "Count of users :INTEGER"},
                    "day": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"}}},

            "GROUP_CITIES_STAT": {
                "fields": {
                    "count": {"type": "INTEGER", "mode": "NULLABLE", "description": "Count of users :INTEGER"},
                    "name": {"type": "STRING", "mode": "NULLABLE", "description": "City name :STRING"},
                    "value": {"type": "STRING", "mode": "NULLABLE", "description": "City ID :STRING"},
                    "day": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"}}},

            "GROUP_COUNTRIES_STAT": {
                "fields": {
                    "code": {"type": "STRING", "mode": "NULLABLE", "description": "Country code :STRING"},
                    "count": {"type": "INTEGER", "mode": "NULLABLE", "description": "Count of users :INTEGER"},
                    "name": {"type": "STRING", "mode": "NULLABLE", "description": "Country name :STRING"},
                    "value": {"type": "STRING", "mode": "NULLABLE", "description": "Country ID :STRING"},
                    "day": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"}}},

            "GROUP_SEX_STAT": {
                "fields": {
                    "value": {"type": "STRING", "mode": "NULLABLE", "description": "Gender param :STRING"},
                    "count": {"type": "INTEGER", "mode": "NULLABLE", "description": "Count of users :INTEGER"},
                    "day": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"}}},

            "GROUP_SEX_AGE_STAT": {
                "fields": {
                    "value": {"type": "STRING", "mode": "NULLABLE", "description": "Age & Gender param :STRING"},
                    "count": {"type": "INTEGER", "mode": "NULLABLE", "description": "Count of users :INTEGER"},
                    "day": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"}}}}

        self.tables_with_schema, self.fields = create_fields(client_name, "VKontakte", self.report_dict, client_id)

        "https://oauth.vk.com/authorize?client_id=7446867&display=page&redirect_uri=http://localhost:8000/auth/vkontakte&scope=pages,ads,offline,groups,stats,email&response_type=code&v=5.103"
        "https://oauth.vk.com/access_token?client_id=7446867&client_secret=3yU3omb66HfxRlujOKxJ&redirect_uri=http://localhost:8000/auth/vkontakte&code=26ffc758fd72e8f81c"
    def __request(self, method, request_type='get', **kwargs):
        if "access_token" not in kwargs:
            params = {'access_token': self.__access_token, 'v': self.__v}
            for key, value in kwargs.items():
                params[key] = value
        else:
            params = kwargs
        request_data = requests.request(request_type, self.__method_url + method, params=params)
        return self.__get_errors(request_data, method, request_type, params)

    def __get_errors(self, response, method, request_type, params):
        if response.status_code == 200:
            if 'error' in response.json():
                if response.json()['error']['error_code'] == 100:
                    for param in response.json()['error']['request_params']:
                        if param['value'] == 'ads.getPostsReach':
                            print("error")
                            time.sleep(3)
                            return self.__request(method, request_type, **params)
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

    def get_leads_forms(self, group_id):
        lead_forms = self.__request("leadForms.list", request_type='get', group_id=group_id)
        keys = ["form_id", "group_id", "name", "title", "description", "site_link_url", "url"]
        lead_forms = self.pop_keys(keys, lead_forms)
        return lead_forms

    def get_all_leads(self, group_id):
        result = []
        lead_forms = self.get_leads_forms(group_id)
        forms = [form['form_id'] for form in lead_forms]
        for form_id in forms:

            leads = self.get_leads_form_id(group_id, form_id)
            time.sleep(1)
            for lead in leads:
                middle_list = {}
                for lead_key, lead_value in lead.items():
                    if lead_key == 'answers':
                        for element in lead_value:
                            middle_list[element['key']] = element['answer']['value']
                    elif lead_key == 'date':
                        middle_list[lead_key] = datetime.strftime(datetime.fromtimestamp(int(lead_value)), "%Y-%m-%d")
                    else:
                        middle_list[lead_key] = lead_value
                result.append(middle_list)

        keys = ["lead_id", "user_id", "date", "first_name", "phone_number", "patronymic_name", "last_name", "email",]
        result = self.pop_keys(keys, result)
        return result

    def get_leads_form_id(self, group_id, form_id, next_page_token=None, result_list=None):
        if result_list is None:
            result_list = []
        params = {"group_id": group_id, "form_id": form_id, "limit": 1000}
        if next_page_token is not None:
            params['next_page_token'] = next_page_token
        leads = self.__request("leadForms.getLeads", request_type='get', **params)
        result_list += leads['leads']
        if "next_page_token" in leads:
            next_page_token = leads['next_page_token']
            time.sleep(3)
            return self.get_leads_form_id(group_id, form_id, next_page_token=next_page_token)
        return result_list

    def get_group_stat(self, group_id, date_from, date_to):
        date_from = (datetime.strptime(date_from, "%Y-%m-%d") + timedelta(hours=0, minutes=0, seconds=0)).timestamp()
        date_to = (datetime.strptime(date_to, "%Y-%m-%d") + timedelta(hours=23, minutes=59, seconds=59)).timestamp()
        stats = self.__request("stats.get", request_type='get', group_id=group_id, timestamp_from=date_from,
                               timestamp_to=date_to, extended=1, stats_groups="activity,visitors")

        result = {"activity": [], "age": [], "cities": [], "countries": [], "sex": [], "sex_age": []}
        for stat in stats:
            day = datetime.strftime(datetime.fromtimestamp(stat['period_from']), "%Y-%m-%d")
            stat['activity']['day'] = day
            result['activity'].append(stat['activity'])
            stat = stat['visitors']
            for visitor_stat_key, visitor_stat_value in stat.items():
                if visitor_stat_key in ["age", "cities", "countries", "sex", "sex_age"]:
                    for element in visitor_stat_value:
                        element['day'] = day
                    result[visitor_stat_key] += visitor_stat_value

        return result["age"], result["cities"], result["countries"], result["sex"], result["sex_age"], result["activity"]

    def get_groups(self, list_of_groups=None, offset=0):
        if list_of_groups is None:
            list_of_groups = []
        groups = self.__request('groups.get', request_type='get', filter="admin,editor,moder,advertiser", count=1000,
                                offset=offset, extended=1)
        count = groups['count']
        list_of_groups += groups['items']
        if len(list_of_groups) < count:
            offset += 1000
            return self.get_groups(list_of_groups=list_of_groups, offset=offset)
        return list_of_groups

    def get_my_user_id(self):
        my_user_id = self.__request('users.get', request_type='get')
        my_user_id = my_user_id[0]
        return my_user_id['id'], my_user_id['first_name'], my_user_id['last_name']

    def post_reach(self, ids_type, list_of_ids):
        day_stat_list = []
        while list_of_ids:
            day_stats = self.__request('ads.getPostsReach', request_type='get', account_id=self.account_id,
                                       ids_type=ids_type, ids=list_of_ids.pop(0))

            day_stat_list += day_stats
            time.sleep(3)
        return day_stat_list
