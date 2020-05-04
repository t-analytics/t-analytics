from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import render, redirect
import requests
# Create your views here.


def oauth_vkontakte(request, client_id):
    # message = "Not get param"
    # if request.method == 'GET':
    #     message = request.GET.get("hello")
    # return HttpResponse(f"{message} - {client_id}")
    https_proxy1 = "https://54.164.133.248:3128"
    https_proxy2 = "https://163.172.219.130:443"
    https_proxy3 = "https://151.253.165.70:8080"

    access_token = "1074300704:AAGCu4LpIOk0Wq2Z9gzkUTiSjbt_wT0MxeQ"

    proxyDict = {
        "https": https_proxy1,
        "https": https_proxy2,
        "https": https_proxy3
    }
    """Responds to any HTTP request.
    Args:
        request (flask.Request): HTTP request object.
    Returns:
        The response text or any set of values that can be turned into a
        Response object using
        `make_response <http://flask.pocoo.org/docs/1.0/api/#flask.Flask.make_response>`.
    """
    request_json = request.get_json()
    if request.args and 'type' in request.args:
        if request.args.get('type') == 'confirmation':
            return '10a829f5'
        elif request.args.get('type') == 'group_join':
            requests.get(f"https://api.telegram.org/bot{access_token}/sendMessage", proxies=proxyDict, params=
            {"chat_id": -419647885, "text": request.args.get('object')['user_id']})
            return
        elif request.args.get('type') == 'lead_forms_new':
            my_str = ''
            for element in request.args.get('object')['answers']:
                my_str += element['question'] + ": " + element['answer'] + "\n"
            requests.get(f"https://api.telegram.org/bot{access_token}/sendMessage", proxies=proxyDict, params=
            {"chat_id": -419647885, "text": my_str})
            return
    elif request_json and 'type' in request_json:
        if request_json['type'] == 'confirmation':
            return '3d28d2bd'
        elif request_json['type'] == 'group_join':
            requests.get(f"https://api.telegram.org/bot{access_token}/sendMessage", proxies=proxyDict, params=
            {"chat_id": -419647885, "text": request_json['object']['user_id']})
            return
        elif request_json['type'] == 'lead_forms_new':
            my_str = ''
            for element in request_json['object']['answers']:
                my_str += element['question'] + ": " + element['answer'] + "\n"
            requests.get(f"https://api.telegram.org/bot{access_token}/sendMessage", proxies=proxyDict, params=
            {"chat_id": -419647885, "text": my_str})
            return
    elif request_json and 'google_key' in request_json:
        string = ""
        for key, value in request_json.items():
            if key == 'user_column_data':
                for element in request_json[key]:
                    column_name = element['column_name']
                    string_value = element['string_value']
                    string += column_name + ": " + string_value + "\n"
                continue
            else:
                string += key + ": " + str(value) + "\n"
        requests.get(f"https://api.telegram.org/bot{access_token}/sendMessage", proxies=proxyDict, params=
        {"chat_id": -419647885, "text": string})
        return
    else:
        return "blablaBomber"
