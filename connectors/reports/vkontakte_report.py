import os, json
from datetime import datetime, timedelta
from connectors.vkontakte_report import VKReport
from connectors import access_data

date_from = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
date_to = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")

clients = json.load(open(os.path.join(os.path.split(os.getcwd())[0], "json_files", "clients.json"), "r"))
access_token = access_data.vkontakte_token_general
for client, placement in clients.items():
    vk_params = placement.get('vkontakte', None)
    if vk_params:
        project = placement['bigquery']['project']
        path_to_bq = os.path.join(access_data.path_to_json,
                                  access_data.name_json_files['project'][project]['path_to_bq'])

        for params in vk_params:
            report = VKReport(path_to_bq, client, date_from, date_to, access_token, params['account_id'],
                              params['client_id'])
            campaigns_ids = report._report_campaigns()
            campaign_ids_with_stat = report._report_campaigns_stat(campaigns_ids)
            ads_ids, ads_df = report._report_ads(campaign_ids_with_stat)
            report._report_ads_stat(ads_ids)
            report._report_demographics(ads_ids)
            report._report_post_reach(ads_df)
            for report_id in params['report_range']:
                if report_id == "LEADS":
                    report._report_lead_forms(params['group_id'])
                    report._report_leads(params['group_id'])
                elif report_id == "GROUP":
                    report._report_group_stat(params['group_id'])
