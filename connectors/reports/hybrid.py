import os, json
from datetime import datetime, timedelta
from connectors.connectors._Hybrid import Hybrid
from connectors import access_data

date_from = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")
date_to = datetime.strftime(datetime.today() - timedelta(days=1), "%Y-%m-%d")

path_to_json = ""
client_name = ""
client = json.load(open(os.path.join(os.path.split(os.getcwd())[0], "json_files", "clients.json"), "r"))
hybrid_params = client[client_name].get('hybrid', None)

if hybrid_params:
    client_id = access_data.hybrid[client_name]['client_id']
    client_secret = access_data.hybrid[client_name]['client_secret']

    project = client[client_name]['bigquery']['project']
    path_to_bq = os.path.join(access_data.path_to_json,
                              access_data.name_json_files['project'][project]['path_to_bq'])

    for params in hybrid_params:

        report = Hybrid(client_id, client_secret, client_name, path_to_json, path_to_bq, date_from, date_to)

        result = report.get_hybrid_report()
        assert result == [], "Error!"
