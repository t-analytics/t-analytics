from django.http import HttpResponse, HttpResponseRedirect
from django.views.decorators.csrf import csrf_exempt
from django.shortcuts import render, redirect
import requests, json
# Create your views here.


@csrf_exempt
def webhook_vkontakte(request, client_id):
    https_proxy1 = "https://54.164.133.248:3128"
    https_proxy2 = "https://163.172.219.130:443"
    https_proxy3 = "https://151.253.165.70:8080"

    access_token = "1074300704:AAGCu4LpIOk0Wq2Z9gzkUTiSjbt_wT0MxeQ"

    proxyDict = {
        "https": https_proxy1,
        "https": https_proxy2,
        "https": https_proxy3
    }
    if request.method == 'GET':
        return HttpResponse('Hello from GET method vk webhook')

    elif request.method == 'POST':
        data = json.loads(request.body)

        if data['type'] == 'confirmation':
            return HttpResponse("10a829f5", content_type="text/plain", status=200)

        elif data['type'] == 'group_join':
            requests.get(f"https://api.telegram.org/bot{access_token}/sendMessage", proxies=proxyDict,
                         params={"chat_id": -419647885, "text": "Join"})
            return HttpResponse('', content_type="text/plain", status=200)

        elif data['type'] == 'lead_forms_new':
            my_str = ''
            # for element in data['object']['answers']:
            #     my_str += element['question'] + ": " + element['answer'] + "\n"
            requests.get(f"https://api.telegram.org/bot{access_token}/sendMessage", proxies=proxyDict,
                         params={"chat_id": -419647885, "text": "Lead"})
            return HttpResponse('', content_type="text/plain", status=200)

    else:
        return HttpResponse('Hello from vk webhook')
