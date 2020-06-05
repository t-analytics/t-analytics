from connectors._Utils import slice_date_on_period
from connectors._BigQuery import BigQuery
from connectors._Calltouch import Calltouch
# from analytics.connectors._Facebook import Facebook
# from analytics.connectors._GAnalytics import GAnalytics
# from analytics.connectors._GAnalyticsUpload import GAnalyticsUpload
# from analytics.connectors._Hybrid import Hybrid
# from analytics.connectors._MyTarget import MyTarget
from connectors._VKontakte import VKApp
# from analytics.connectors._YandexDirectReports import YandexDirectReports
# from analytics.connectors._YandexDirect import YandexDirect
# from analytics.connectors._YandexMetrica import YandexMetrica

import pandas as pd
import re, sys, imaplib, email
from email.header import decode_header
from datetime import datetime


class Report:
    def __init__(self, client_name, path_to_bq, date_from, date_to):
        self.client_name = client_name
        self.date_from = date_from
        self.date_to = date_to
        self.path_to_bq = path_to_bq
        self.bq = BigQuery(path_to_bq)



    # def get_email_report(self, email_user, email_password, email_from, db_name, table_name, subject_filter,
    # 					path_to_save, since, before, host='imap.yandex.ru', port=993):
    #
    # 	mail = imaplib.IMAP4_SSL(host, port)
    # 	mail.login(email_user, email_password)
    # 	mail.select('INBOX')
    #
    # 	# since = datetime.strftime(datetime.today(), "%d-%b-%Y")
    # 	# before = datetime.strftime(datetime.today() + timedelta(days=1), "%d-%b-%Y")
    #
    # 	status_search, search_result = mail.uid('search', None, f'(FROM "{email_from}")', f'(SINCE "{since}")',
    # 											f'(BEFORE "{before}")')
    # 	list_of_ids_str = search_result[0].decode("utf-8").split(" ")
    # 	if list_of_ids_str[0] == '':
    # 		sys.exit(f"От {email_from} нет писем за {since}.")
    # 	else:
    # 		list_of_ids_int = [int(num) for num in list_of_ids_str]
    # 		list_of_ids_in_one = ','.join(list_of_ids_str)
    # 		ids_in_bq = self.bq.get_query(
    # 			f"SELECT * FROM `{db_name}.{table_name}` WHERE letter_id IN ({list_of_ids_in_one})")
    # 		list_for_download = list(set(list_of_ids_int).difference(set([ids[0] for ids in ids_in_bq])))
    #
    # 		if list_for_download:
    # 			sys.exit(f"От {email_from} нет писем для скачивания.")
    #
    # 		for letter_id in list_for_download:
    # 			mail.select('INBOX')
    # 			status_result, letter_data = mail.uid('fetch', str(letter_id), '(RFC822)')
    # 			raw_email = letter_data[0][1]
    # 			email_message = email.message_from_bytes(raw_email)
    # 			subject_list = decode_header(email_message['Subject'])
    # 			code = subject_list[0][1]
    # 			subject = subject_list[0][0].decode(code)
    #
    # 			if subject_filter in subject:
    # 				list_of_file_names = []
    # 				for part in email_message.walk():
    # 					if part.get_content_maintype() == 'multipart':
    # 						continue
    # 					if part.get('Content-Disposition') is None:
    # 						continue
    # 					file_name_list = decode_header(part.get_filename())
    # 					file_name = file_name_list[0][0]
    # 					code = file_name_list[0][1]
    # 					if code != None:
    # 						file_name = file_name_list[0][0].decode(code)
    # 					fp = open(f"{path_to_save}{file_name}", 'wb')
    # 					fp.write(part.get_payload(decode=True))
    # 					fp.close()
    # 					list_of_file_names.append(file_name)
    # 				mail.close()
    # 		return list_of_file_names
    #
    # def get_facebook_report(self, access_token, account):
    # 	fb = Facebook(access_token, account, self.client_name)
    #
    # 	data_set_id = f"{self.client_name}_Facebook_{account[4:]}"
    #
    # 	self.bq.check_or_create_data_set(data_set_id)
    # 	self.bq.check_or_create_tables(fb.tables_with_schema, data_set_id)
    #
    # 	ads_stat = fb.get_ads_or_campaign_statistics(self.date_from, self.date_to, "ad")
    #
    # 	if not ads_stat:
    # 		return f"За {self.date_from} - {self.date_to} нет статистики"
    #
    # 	ads_stat_df = pd.DataFrame(ads_stat).fillna(0)
    # 	self.bq.data_to_insert(ads_stat_df, fb.integer_fields, fb.float_fields, fb.string_fields, data_set_id,
    # 							f"{self.client_name}_Facebook_ADS_STAT")
    #
    # 	ads_reach = fb.get_ads_adset_campaign_reach_statistics(self.date_from, self.date_to, "ad")
    # 	ads_reach_df = pd.DataFrame(ads_reach).fillna(0)
    # 	self.bq.data_to_insert(ads_reach_df, fb.integer_fields, fb.float_fields, fb.string_fields, data_set_id,
    # 							f"{self.client_name}_Facebook_ADS_REACH_STAT")
    #
    # 	campaign_reach = fb.get_ads_adset_campaign_reach_statistics(self.date_from, self.date_to, "campaign")
    # 	campaign_reach_df = pd.DataFrame(campaign_reach).fillna(0)
    # 	self.bq.data_to_insert(campaign_reach_df, fb.integer_fields, fb.float_fields, fb.string_fields, data_set_id,
    # 							f"{self.client_name}_Facebook_CAMPAIGN_REACH_STAT")
    #
    # 	adset_reach = fb.get_ads_adset_campaign_reach_statistics(self.date_from, self.date_to, "adset")
    # 	adset_reach_df = pd.DataFrame(adset_reach).fillna(0)
    # 	self.bq.data_to_insert(adset_reach_df, fb.integer_fields, fb.float_fields, fb.string_fields, data_set_id,
    # 							f"{self.client_name}_Facebook_ADSET_REACH_STAT")
    #
    # 	campaign_stat = fb.get_ads_or_campaign_statistics(self.date_from, self.date_to, "campaign")
    # 	campaign_stat_df = pd.DataFrame(campaign_stat).fillna(0)
    # 	self.bq.data_to_insert(campaign_stat_df, fb.integer_fields, fb.float_fields, fb.string_fields, data_set_id,
    # 							f"{self.client_name}_Facebook_CAMPAIGN_STAT")
    #
    # 	campaigns_ids = campaign_stat_df['campaign_id'].tolist()
    #
    # 	campaigns, header = fb.get_campaigns([])
    # 	campaigns_df = pd.DataFrame(campaigns)
    # 	self.bq.insert_difference(campaigns_df, fb.integer_fields, fb.float_fields, fb.string_fields, data_set_id,
    # 								f"{self.client_name}_Facebook_CAMPAIGNS", 'id', 'id')
    #
    # 	# ads_basic = fb.get_ads_or_adsets(campaigns_ids, "ads-basic")
    # 	# ads_basic_df = pd.DataFrame(ads_basic).fillna("<not set>")
    # 	# self.bq.insert_difference(ads_basic_df, fb.integer_fields, fb.float_fields, fb.string_fields, data_set_id,
    # 	# 							f"{self.client_name}_Facebook_ADS_BASIC", 'id', 'id')
    # 	#
    # 	# ads_layout = fb.get_ads_or_adsets(campaigns_ids, "ads-layout")
    # 	# ads_layout_df = pd.DataFrame(ads_layout).fillna("<not set>")
    # 	# self.bq.insert_difference(ads_layout_df, fb.integer_fields, fb.float_fields, fb.string_fields, data_set_id,
    # 	# 							f"{self.client_name}_Facebook_ADS_LAYOUT", 'id', 'id')
    #
    # 	ad_sets = fb.get_ads_or_adsets(campaigns_ids, "adsets")
    # 	ad_sets_df = pd.DataFrame(ad_sets).fillna("<not set>")
    # 	self.bq.insert_difference(ad_sets_df, fb.integer_fields, fb.float_fields, fb.string_fields, data_set_id,
    # 								f"{self.client_name}_Facebook_ADSETS", 'id', 'id')



    # def get_analytics_report(self, view_id, path_to_ga, report_range):
    # 	ga = GAnalytics(path_to_ga, view_id, self.client_name)
    #
    # 	data_set_id = f"{self.client_name}_GAnalytics_{view_id}"
    #
    # 	self.bq.check_or_create_data_set(data_set_id)
    # 	self.bq.check_or_create_tables(ga.tables_with_schema, data_set_id)
    #
    # 	date_range = slice_date_on_period(self.date_from, self.date_to, 1)
    #
    # 	for report in ga.report_dict:
    # 		if report in report_range:
    # 			for date_from, date_to in date_range:
    # 				metric_list = [re.sub('[_]', ':', field) for field in list(ga.report_dict[report]['metrics'].keys())]
    # 				dimension_list = [re.sub('[_]', ':', field) for field in
    # 									list(ga.report_dict[report]['dimensions'].keys())]
    #
    # 				report_data = ga.get_request(date_from, date_to, metric_list, dimension_list)
    # 				columns = [re.sub('[:]', '_', field) for field in report_data[0]]
    #
    # 				report_data_df = pd.DataFrame(report_data[1], columns=columns)
    #
    # 				self.bq.data_to_insert(report_data_df, ga.integer_fields, ga.float_fields, ga.string_fields,
    # 										data_set_id, f"{self.client_name}_GAnalytics_{report}")

    # def get_yandex_report(self, client_login, access_token, period, report_range):
    #
    # 	"""
    #
    # 	:param report_type: "KEYWORD" or "CAMPAIGN" or "AD"
    #
    # 	"""
    # 	client_login_re = re.sub('[.-]', '_', client_login)
    # 	yandex = YandexDirectReports(access_token, client_login, self.client_name)
    #
    # 	data_set_id = f"{self.client_name}_YandexDirect_{client_login_re}"
    #
    # 	date_range = slice_date_on_period(self.date_from, self.date_to, period)
    #
    # 	self.bq.check_or_create_data_set(data_set_id)
    # 	self.bq.check_or_create_tables(yandex.tables_with_schema, data_set_id)
    #
    # 	for report in yandex.report_dict:
    # 		if report in report_range:
    # 			for date_from, date_to in date_range:
    # 				report_data = yandex.get_report(f"{report}",
    # 												f"{report} {datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')}",
    # 												date_from, date_to)
    # 				report_data_split = report_data.text.split('\n')
    # 				data = [x.split('\t') for x in report_data_split]
    # 				stat = pd.DataFrame(data[1:-1], columns=data[:1][0])
    # 				self.bq.data_to_insert(stat, yandex.integer_fields, yandex.float_fields, yandex.string_fields,
    # 										data_set_id, f"{self.client_name}_YandexDirect_{report}")

    # def get_mytarget_report(self, access_token, period, client_login):
    # 	mt = MyTarget(access_token, self.client_name)
    #
    # 	date_range = slice_date_on_period(self.date_from, self.date_to, period)
    #
    # 	data_set_id = f"{self.client_name}_MyTarget_{client_login}"
    #
    # 	self.bq.check_or_create_data_set(data_set_id)
    # 	self.bq.check_or_create_tables(mt.tables_with_schema, data_set_id)
    #
    # 	campaigns = mt.get_campaigns()
    # 	campaigns_df = pd.DataFrame(campaigns)
    # 	self.bq.insert_difference(campaigns_df, mt.integer_fields, mt.float_fields, mt.string_fields, data_set_id,
    # 						f"{self.client_name}_MyTarget_CAMPAIGNS", 'id', 'id')
    #
    # 	ads = mt.get_ads()
    # 	ads_df = pd.DataFrame(ads)
    # 	self.bq.insert_difference(ads_df, mt.integer_fields, mt.float_fields, mt.string_fields, data_set_id,
    # 						f"{self.client_name}_MyTarget_ADS", 'id', 'id')
    #
    # 	for date_from, date_to in date_range:
    # 		stat_banner = mt.get_banner_stat(date_from, date_to)
    # 		if stat_banner == []:
    # 			continue
    # 		stat_banner_df = pd.DataFrame(stat_banner)
    # 		self.bq.data_to_insert(stat_banner_df, mt.integer_fields, mt.float_fields, mt.string_fields, data_set_id,
    # 						f"{self.client_name}_MyTarget_BANNER_STAT")
    #
    # 		banner_reach = mt.get_banner_reach_stat(date_from, date_to)
    # 		banner_reach_df = pd.DataFrame(banner_reach)
    # 		self.bq.data_to_insert(banner_reach_df, mt.integer_fields, mt.float_fields, mt.string_fields, data_set_id,
    # 								f"{self.client_name}_MyTarget_BANNER_REACH")
    #
    # 		campaign_reach = mt.get_campaign_reach_stat(date_from, date_to)
    # 		campaign_reach_df = pd.DataFrame(campaign_reach)
    # 		self.bq.data_to_insert(campaign_reach_df, mt.integer_fields, mt.float_fields, mt.string_fields, data_set_id,
    # 								f"{self.client_name}_MyTarget_CAMPAIGN_REACH")
    #
    # 		stat_campaigns = mt.get_campaigns_stat(date_from, date_to)
    # 		stat_campaigns_df = pd.DataFrame(stat_campaigns)
    # 		self.bq.data_to_insert(stat_campaigns_df, mt.integer_fields, mt.float_fields, mt.string_fields, data_set_id,
    # 						f"{self.client_name}_MyTarget_CAMPAIGN_STAT")

    def get_vkontakte_report(self, access_token, account_id, client_id, report_dict):
        vkontakte = VKApp(access_token, account_id, client_id, self.client_name)

        data_set_id = f"{self.client_name}_VKontakte_{client_id}"

        self.bq.check_or_create_data_set(data_set_id)
        self.bq.check_or_create_tables(vkontakte.tables_with_schema, data_set_id)

        campaigns = vkontakte.get_campaigns()
        campaign_ids = [campaign_id['id'] for campaign_id in campaigns]
        campaigns_df = pd.DataFrame(campaigns).fillna(0)
        self.bq.insert_difference(campaigns_df, vkontakte.fields, data_set_id,
                                  f"{self.client_name}_VKontakte_{client_id}_CAMPAIGNS", 'id', 'id', "%Y-%m-%d")

        campaign_stat = vkontakte.get_day_stats("campaign", campaign_ids, self.date_from, self.date_to, 100)
        campaign_ids = [campaign_id['campaign_id'] for campaign_id in campaign_stat]
        campaign_stat_df = pd.DataFrame(campaign_stat).fillna(0)
        self.bq.data_to_insert(campaign_stat_df, vkontakte.fields, data_set_id,
                               f"{self.client_name}_VKontakte_{client_id}_CAMPAIGN_STAT", "%Y-%m-%d")

        ads = vkontakte.get_ads(campaign_ids)
        ads_ids = [ad_id['id'] for ad_id in ads]
        ads_df = pd.DataFrame(ads).fillna(0)
        self.bq.insert_difference(ads_df, vkontakte.fields, data_set_id,
                                  f"{self.client_name}_VKontakte_{client_id}_ADS", 'id', 'id', "%Y-%m-%d")

        ads_stat = vkontakte.get_day_stats("ad", ads_ids, self.date_from, self.date_to, 100)
        ads_stat_df = pd.DataFrame(ads_stat).fillna(0)
        self.bq.data_to_insert(ads_stat_df, vkontakte.fields, data_set_id,
                               f"{self.client_name}_VKontakte_{client_id}_ADS_STAT", "%Y-%m-%d")

        post_ad_ids = ads_df[ads_df['ad_format'] == 9]['id'].tolist()

        post_reach = vkontakte.post_reach("ad", post_ad_ids.copy())
        post_reach_df = pd.DataFrame(post_reach).fillna(0)
        table_id = f"{self.client_name}_VKontakte_{client_id}_POST_REACH"
        for ad in post_ad_ids:
            self.bq.get_delete_query(f"DELETE FROM `{data_set_id}.{table_id}` WHERE id = '{ad}'")
        self.bq.data_to_insert(post_reach_df, vkontakte.fields, data_set_id, table_id, "%Y-%m-%d")

        sex, age, sex_age, cities = vkontakte.get_demographics(ads_ids, self.date_from, self.date_to, 100)
        sex_df = pd.DataFrame(sex).fillna(0)
        self.bq.data_to_insert(sex_df, vkontakte.fields, data_set_id,
                               f"{self.client_name}_VKontakte_{client_id}_SEX_STAT", "%Y-%m-%d")

        age_df = pd.DataFrame(age).fillna(0)
        self.bq.data_to_insert(age_df, vkontakte.fields, data_set_id,
                               f"{self.client_name}_VKontakte_{client_id}_AGE_STAT", "%Y-%m-%d")

        sex_age_df = pd.DataFrame(sex_age).fillna(0)
        self.bq.data_to_insert(sex_age_df, vkontakte.fields, data_set_id,
                               f"{self.client_name}_VKontakte_{client_id}_SEX_AGE_STAT", "%Y-%m-%d")

        cities_df = pd.DataFrame(cities).fillna(0)
        self.bq.data_to_insert(cities_df, vkontakte.fields, data_set_id,
                               f"{self.client_name}_VKontakte_{client_id}_CITIES_STAT", "%Y-%m-%d")
    #
    # def upload_data_to_analytics(self, data_frame_to_insert, account_id, web_property_id, custom_data_source_id,
    # 							file_name, path_to_csv):
    #
    # 	ga_upload = GAnalyticsUpload(self.path_to_ga, account_id, web_property_id, custom_data_source_id)
    # 	status, message = ga_upload.upload_data(data_frame_to_insert, path_to_csv, file_name)
    # 	return status, message
    #
    # def get_yandex_direct_agency_clients(self, access_token):
    # 	yandex = YandexDirect(access_token, self.client_name, "")
    # 	data_set_id = f"{self.client_name}_YandexDirect_"
    # 	self.bq.check_or_create_data_set(data_set_id)
    # 	self.bq.check_or_create_tables(yandex.tables_with_schema, data_set_id)
    #
    # 	agency_clients, agency_clients_list = yandex.get_agency_clients()
    # 	agency_clients_df = pd.DataFrame(agency_clients)
    # 	self.bq.insert_difference(agency_clients_df, yandex.integer_fields, yandex.float_fields, yandex.string_fields,
    # 								data_set_id, f"{self.client_name}_YandexDirect_CLIENTS", "Login", "Login")
    # 	return agency_clients_list
    #
    # def get_vkontakte_agency_clients(self):
    # 	pass
    #
    # def get_yandex_direct_objects(self, access_token, client_login, campaign_ids=None):
    # 	client_login_re = re.sub('[.-]', '_', client_login)
    # 	yandex = YandexDirect(access_token, self.client_name, client_login_re)
    # 	data_set_id = f"{self.client_name}_YandexDirect_{client_login_re}"
    #
    # 	self.bq.check_or_create_data_set(data_set_id)
    # 	self.bq.check_or_create_tables(yandex.tables_with_schema, data_set_id)
    #
    # 	campaigns = yandex.get_campaigns()
    # 	campaigns_df = pd.DataFrame(campaigns)
    # 	self.bq.insert_difference(campaigns_df, yandex.integer_fields, yandex.float_fields, yandex.string_fields,
    # 								data_set_id, f"{self.client_name}_YandexDirect_CAMPAIGNS", 'Id', 'Id')
    # 	if campaign_ids is None:
    # 		campaign_ids = [campaign_id['Id'] for campaign_id in campaigns]
    #
    # 	adsets = yandex.get_adsets(campaign_ids)
    # 	adsets_df = pd.DataFrame(adsets)
    # 	self.bq.insert_difference(adsets_df, yandex.integer_fields, yandex.float_fields, yandex.string_fields,
    # 								data_set_id, f"{self.client_name}_YandexDirect_ADGROUPS", 'Id', 'Id')
    #
    # 	ads = yandex.get_ads(campaign_ids)
    # 	ads_df = pd.DataFrame(ads)
    # 	self.bq.insert_difference(ads_df, yandex.integer_fields, yandex.float_fields, yandex.string_fields,
    # 								data_set_id, f"{self.client_name}_YandexDirect_ADS", 'Id', 'Id')
    #
    # 	keywords = yandex.get_keywords(campaign_ids)
    # 	keywords_df = pd.DataFrame(keywords)
    # 	self.bq.insert_difference(keywords_df, yandex.integer_fields, yandex.float_fields, yandex.string_fields,
    # 								data_set_id, f"{self.client_name}_YandexDirect_KEYWORD", 'Id', 'Id')
    #
    # def get_metrica_report(self, access_token, view_id, report_range, conversions_list):
    # 	metrica = YandexMetrica(access_token, self.client_name, view_id)
    # 	data_set_id = f"{self.client_name}_YandexMetrica_{view_id}"
    #
    # 	conversion_schema = metrica.create_conv_schema(conversions_list)
    # 	metrica.report_dict['CONVERSIONS']['metrics'] = conversion_schema
    # 	metrica.integer_fields = metrica.integer_fields + list(conversion_schema.keys())
    # 	metrica.tables_with_schema[f"{self.client_name}_YandexMetrica_CONVERSIONS"].update(conversion_schema)
    #
    # 	self.bq.check_or_create_data_set(data_set_id)
    # 	self.bq.check_or_create_tables(metrica.tables_with_schema, data_set_id)
    #
    # 	date_range = slice_date_on_period(self.date_from, self.date_to, 90)
    #
    # 	for report in metrica.report_dict:
    # 		if report in report_range:
    # 			for date_from, date_to in date_range:
    # 				metric_list = [re.sub('[_]', ':', field) for field in
    # 								list(metrica.report_dict[report]['metrics'].keys())]
    # 				dimension_list = [re.sub('[_]', ':', field) for field in
    # 									list(metrica.report_dict[report]['dimensions'].keys())]
    #
    # 				report_data = metrica.get_report(date_from, date_to, metric_list, dimension_list)
    # 				names = [re.sub('[:]', '_', field) for field in report_data[0]]
    # 				report_data_df = pd.DataFrame(report_data[1], columns=names)
    # 				# return report_data_df.info()
    #
    # 				self.bq.data_to_insert(report_data_df, metrica.integer_fields, metrica.float_fields,
    # 										metrica.string_fields, data_set_id,
    # 										f"{self.client_name}_YandexMetrica_{report}")
