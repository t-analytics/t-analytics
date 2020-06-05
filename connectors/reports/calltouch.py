import os, json, re
from datetime import datetime, timedelta
from connectors.connectors._Calltouch import Calltouch
from connectors import access_data

date_from = datetime.strftime(datetime.today() - timedelta(days=1), "%d/%m/%Y")
date_to = datetime.strftime(datetime.today() - timedelta(days=1), "%d/%m/%Y")

client_name = ""
client = json.load(open(os.path.join(os.path.split(os.getcwd())[0], "json_files", "clients.json"), "r"))
calltouch_params = client[client_name].get('calltouch', None)

if calltouch_params:
    project = client[client_name]['bigquery']['project']
    path_to_bq = os.path.join(access_data.path_to_json,
                              access_data.name_json_files['project'][project]['path_to_bq'])

    for params in calltouch_params:
        site_name_re = re.sub('[.-]', '_', params['name'])
        token = access_data.Calltouch[params['site_id']]

        report = Calltouch(params['site_id'], token, client_name, path_to_bq, date_from, date_to, params['report_range'])
        report.get_calltouch_report()


