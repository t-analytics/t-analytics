import os, json
from datetime import datetime, timedelta
from connectors.connectors._Email import get_email
from connectors import access_data

client_name = "BSPB"
client = json.load(open(os.path.join(os.path.split(os.getcwd())[0], "json_files", "clients.json"), "r"))
email_params = client[client_name].get('email', None)
access_token = access_data.email_param_data['client_name']
login = access_token['login']
password = access_token['password']
path_to_save = ''

if email_params:
    project = client[client_name]['bigquery']['project']
    path_to_bq = os.path.join(access_data.path_to_json,
                              access_data.name_json_files['project'][project]['path_to_bq'])

    for params in email_params:
        report = get_email(login, password, params['email_from'], params['subject_filter'], path_to_save, path_to_bq,
                           client_name, params['placement'], report_dict)
