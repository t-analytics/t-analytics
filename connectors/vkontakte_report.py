from connectors.connectors._BigQuery import BigQuery
from connectors.connectors._VKontakte import VKontakte
from connectors.connectors._Utils import slice_date_on_period
import pandas as pd


class VKReport:
    def __init__(self, path_to_bq, client_name, date_from, date_to, access_token, account_id, client_id):
        self.client_name = client_name
        self.client_id = client_id
        self.date_from = date_from
        self.date_to = date_to
        self.data_set_id = f"{self.client_name}_VKontakte_{client_id}"
        self.date_range = slice_date_on_period(date_from, date_to, 30)
        self.bq = BigQuery(path_to_bq)
        self.vk = VKontakte(access_token, account_id, client_id, client_name)
        self.bq.check_or_create_data_set(self.data_set_id)
        self.bq.check_or_create_tables(self.vk.tables_with_schema, self.data_set_id)

    def _report_lead_forms(self, group_id):
        leads_forms = self.vk.get_leads_forms(group_id)
        leads_forms_df = pd.DataFrame(leads_forms).fillna(0)
        table_id = f"{self.client_name}_VKontakte_{self.client_id}_LEAD_FORMS"
        self.bq.get_delete_query(f"DELETE FROM `{self.data_set_id}.{table_id}` WHERE form_id != ''")

        self.bq.data_to_insert(leads_forms_df, self.vk.fields, self.data_set_id, table_id, "%Y-%m-%d")

    def _report_leads(self, group_id):
        leads = self.vk.get_all_leads(group_id)
        leads_df = pd.DataFrame(leads).fillna(0)
        self.bq.data_to_insert(leads_df, self.vk.fields, self.data_set_id,
                               f"{self.client_name}_VKontakte_{self.client_id}_LEADS", "%Y-%m-%d")

    def _report_post_reach(self, ads_df):
        post_ad_ids = ads_df[ads_df['ad_format'] == 9]['id'].tolist()

        post_reach = self.vk.post_reach("ad", post_ad_ids.copy())
        post_reach_df = pd.DataFrame(post_reach).fillna(0)
        table_id = f"{self.client_name}_VKontakte_{self.client_id}_POST_REACH"

        self.bq.delete_and_insert(post_reach_df, self.vk.fields, self.data_set_id, table_id, "%Y-%m-%d", id=post_ad_ids)

    def _report_group_stat(self, group_id):
        age, cities, countries, sex, sex_age, activity = self.vk.get_group_stat(group_id, self.date_from, self.date_to)
        sex_df = pd.DataFrame(sex).fillna(0)

        self.bq.data_to_insert(sex_df, self.vk.fields, self.data_set_id,
                               f"{self.client_name}_VKontakte_{self.client_id}_GROUP_SEX_STAT", "%Y-%m-%d")

        age_df = pd.DataFrame(age).fillna(0)
        self.bq.data_to_insert(age_df, self.vk.fields, self.data_set_id,
                               f"{self.client_name}_VKontakte_{self.client_id}_GROUP_AGE_STAT", "%Y-%m-%d")

        sex_age_df = pd.DataFrame(sex_age).fillna(0)
        self.bq.data_to_insert(sex_age_df, self.vk.fields, self.data_set_id,
                               f"{self.client_name}_VKontakte_{self.client_id}_GROUP_SEX_AGE_STAT", "%Y-%m-%d")

        cities_df = pd.DataFrame(cities).fillna(0)
        self.bq.data_to_insert(cities_df, self.vk.fields, self.data_set_id,
                               f"{self.client_name}_VKontakte_{self.client_id}_GROUP_CITIES_STAT", "%Y-%m-%d")

        activity_df = pd.DataFrame(activity).fillna(0)
        self.bq.data_to_insert(activity_df, self.vk.fields, self.data_set_id,
                               f"{self.client_name}_VKontakte_{self.client_id}_GROUP_ACTIVITY", "%Y-%m-%d")

        countries_df = pd.DataFrame(countries).fillna(0)
        self.bq.data_to_insert(countries_df, self.vk.fields, self.data_set_id,
                               f"{self.client_name}_VKontakte_{self.client_id}_GROUP_COUNTRIES_STAT", "%Y-%m-%d")

    def _report_demographics(self, ads_ids):
        for date_from, date_to in self.date_range:
            sex, age, sex_age, cities = self.vk.get_demographics(ads_ids, date_from, date_to, 100)
            sex_df = pd.DataFrame(sex).fillna(0)
            self.bq.data_to_insert(sex_df, self.vk.fields, self.data_set_id,
                                   f"{self.client_name}_VKontakte_{self.client_id}_SEX_STAT", "%Y-%m-%d")

            age_df = pd.DataFrame(age).fillna(0)
            self.bq.data_to_insert(age_df, self.vk.fields, self.data_set_id,
                                   f"{self.client_name}_VKontakte_{self.client_id}_AGE_STAT", "%Y-%m-%d")

            sex_age_df = pd.DataFrame(sex_age).fillna(0)
            self.bq.data_to_insert(sex_age_df, self.vk.fields, self.data_set_id,
                                   f"{self.client_name}_VKontakte_{self.client_id}_SEX_AGE_STAT", "%Y-%m-%d")

            cities_df = pd.DataFrame(cities).fillna(0)
            self.bq.data_to_insert(cities_df, self.vk.fields, self.data_set_id,
                                   f"{self.client_name}_VKontakte_{self.client_id}_CITIES_STAT", "%Y-%m-%d")

    def _report_campaigns(self):
        campaigns = self.vk.get_campaigns()
        campaign_ids = [campaign_id['id'] for campaign_id in campaigns]
        campaigns_df = pd.DataFrame(campaigns).fillna(0)
        self.bq.insert_difference(campaigns_df, self.vk.fields, self.data_set_id,
                                  f"{self.client_name}_VKontakte_{self.client_id}_CAMPAIGNS", 'id', 'id', "%Y-%m-%d")

        return campaign_ids

    def _report_campaigns_stat(self, campaign_ids):
        campaign_ids_with_stat = []
        for date_from, date_to in self.date_range:
            campaign_stat = self.vk.get_day_stats("campaign", campaign_ids, date_from, date_to, 100)
            campaign_ids_with_stat += [campaign_id['campaign_id'] for campaign_id in campaign_stat]
            campaign_stat_df = pd.DataFrame(campaign_stat).fillna(0)
            self.bq.data_to_insert(campaign_stat_df, self.vk.fields, self.data_set_id,
                                   f"{self.client_name}_VKontakte_{self.client_id}_CAMPAIGN_STAT", "%Y-%m-%d")

        return campaign_ids_with_stat

    def _report_ads(self, campaign_ids_with_stat):
        ads = self.vk.get_ads(campaign_ids_with_stat)
        ads_ids = [ad_id['id'] for ad_id in ads]
        ads_df = pd.DataFrame(ads).fillna(0)
        self.bq.insert_difference(ads_df, self.vk.fields, self.data_set_id,
                                  f"{self.client_name}_VKontakte_{self.client_id}_ADS", 'id', 'id', "%Y-%m-%d")

        return ads_ids, ads_df

    def _report_ads_stat(self, ads_ids_with_stat):
        for date_from, date_to in self.date_range:
            ads_stat = self.vk.get_day_stats("ad", ads_ids_with_stat, date_from, date_to, 100)
            ads_stat_df = pd.DataFrame(ads_stat).fillna(0)
            self.bq.data_to_insert(ads_stat_df, self.vk.fields, self.data_set_id,
                                   f"{self.client_name}_VKontakte_{self.client_id}_ADS_STAT", "%Y-%m-%d")
