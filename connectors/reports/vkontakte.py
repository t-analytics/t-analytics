import os, json
from datetime import datetime, timedelta
from connectors.connectors._VKontakte import VKontakte
from connectors import access_data

date_from = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
date_to = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
client_name = "BSPB"
client = json.load(open(os.path.join(os.path.split(os.getcwd())[0], "json_files", "clients.json"), "r"))
vk_params = client[client_name].get('vkontakte', None)
access_token = access_data.vkontakte_token_general

if vk_params:
    project = client[client_name]['bigquery']['project']
    path_to_bq = os.path.join(access_data.path_to_json,
                              access_data.name_json_files['project'][project]['path_to_bq'])

    for params in vk_params:
        report = VKontakte(access_token, params['account_id'], params['client_id'], client_name, path_to_bq, date_from,
                           date_to)
        campaigns_ids, campaigns_df = report.get_campaigns()
        campaign_ids_with_stat = report.report_campaigns_stat(campaigns_ids)
        ads_ids, ads_df = report.report_ads(campaign_ids_with_stat)
        report.report_ads_stat(ads_ids)
        report.report_demographics(ads_ids)
        report.report_post_reach(ads_df)
        if "LEADS" in params['report_range']:
            report.report_lead_forms(params['group_id'])
            report.report_leads(params['group_id'])
        elif "GROUP" in params['report_range']:
            report.report_group_stat(params['group_id'])
