import os, json
from datetime import datetime, timedelta
from connectors.connectors._YLogs import YMLogs
from connectors import access_data

date_from = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
date_to = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")

client = json.load(open(os.path.join(os.path.split(os.getcwd())[0], "json_files", "clients.json"), "r"))
access_token = access_data.yandex_token_general

client_name = ""
ym_logs_params = client[client_name].get('metrica_logs', None)

if ym_logs_params:
    project = client[client_name]['bigquery']['project']
    path_to_bq = os.path.join(access_data.path_to_json,
                              access_data.name_json_files['project'][project]['path_to_bq'])

    for params in ym_logs_params:
        report = YMLogs(params['counter_id'], access_token, client, path_to_bq, date_from, date_to)
        result = report.get_report()
        assert result == [], "Something wrong"
