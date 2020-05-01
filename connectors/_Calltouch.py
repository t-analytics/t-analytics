import requests
from connectors._Utils import create_fields


class Calltouch:
    def __init__(self, ct_site_id, ct_token, client_name):
        self.__ct_token = ct_token
        "https://api-node9.calltouch.ru/calls-service/RestAPI/requests/"
        self.__url = f'http://api.calltouch.ru/calls-service/RestAPI/{ct_site_id}/'
        self.report_dict = {
            "CALLS": {
                "fields": {
                    'date': {"type": "DATE", "mode": "NULLABLE", "description": "Call day :DATE"},
                    'callUrl': {"type": "STRING", "mode": "NULLABLE", "description": "Call URL :STRING"},
                    'uniqueCall': {"type": "BOOLEAN", "mode": "NULLABLE", "description": "Unique Call :BOOLEAN"},
                    'utmContent': {"type": "STRING", "mode": "NULLABLE", "description": "UTM Content :STRING"},
                    'source': {"type": "STRING", "mode": "NULLABLE", "description": "Source :STRING"},
                    'waitingConnect': {"type": "FLOAT", "mode": "NULLABLE", "description": "Waiting connect :FLOAT"},
                    'ctCallerId': {"type": "STRING", "mode": "NULLABLE", "description": "Calltouch Caller ID :STRING"},
                    'keyword': {"type": "STRING", "mode": "NULLABLE", "description": "Keyword :STRING"},
                    'utmSource': {"type": "STRING", "mode": "NULLABLE", "description": "UTM Source :STRING"},
                    'sipCallId': {"type": "STRING", "mode": "NULLABLE", "description": "Sip Call ID :STRING"},
                    'utmCampaign': {"type": "STRING", "mode": "NULLABLE", "description": "UTM Campaign :STRING"},
                    'phoneNumber': {"type": "STRING", "mode": "NULLABLE", "description": "Phone number :STRING"},
                    'uniqTargetCall': {"type": "BOOLEAN", "mode": "NULLABLE", "description": "Uniq Call :BOOLEAN"},
                    'utmMedium': {"type": "STRING", "mode": "NULLABLE", "description": "UTM Medium :BOOLEAN"},
                    'city': {"type": "STRING", "mode": "NULLABLE", "description": "City :STRING"},
                    'yaClientId': {"type": "STRING", "mode": "NULLABLE", "description": "Yandex Client_ID :STRING"},
                    'medium': {"type": "STRING", "mode": "NULLABLE", "description": "Medium :STRING"},
                    'duration': {"type": "FLOAT", "mode": "NULLABLE", "description": "Call duration :FLOAT"},
                    'callbackCall': {"type": "BOOLEAN", "mode": "NULLABLE", "description": "Callback call :BOOLEAN"},
                    'successful': {"type": "BOOLEAN", "mode": "NULLABLE", "description": "Successful :BOOLEAN"},
                    'callId': {"type": "STRING", "mode": "NULLABLE", "description": "Call ID :STRING"},
                    'clientId': {"type": "STRING", "mode": "NULLABLE", "description": "Google Client_ID :STRING"},
                    'callerNumber': {"type": "STRING", "mode": "NULLABLE", "description": "Caller number :STRING"},
                    'utmTerm': {"type": "STRING", "mode": "NULLABLE", "description": "UTM Term :STRING"},
                    'sessionId': {"type": "STRING", "mode": "NULLABLE", "description": "Session ID :STRING"},
                    'targetCall': {"type": "BOOLEAN", "mode": "NULLABLE", "description": "Target Call :BOOLEAN"},
                    'AUTO_PR': {"type": "STRING", "mode": "NULLABLE", "description": "AUTO Tags :STRING"},
                    'MANUAL': {"type": "STRING", "mode": "NULLABLE", "description": "MANUAL Tags :STRING"}
                }}}

        self.tables_with_schema, self.fields = create_fields(client_name, "Calltouch", self.report_dict, ct_site_id)

    def __get_pages(self, date_from, date_to):
        params = {'clientApiId': self.__ct_token, 'dateFrom': date_from, 'dateTo': date_to, 'page': 1, 'limit': 1000}
        pages = requests.get(self.__url, params=params).json()['pageTotal']
        return pages

    def get_node_id(self):
        response = requests.get(self.__url + "getnodeid/", params={'clientApiId': self.__ct_token})
        return response.json()['nodeId']

    def get_forms(self, date_from, date_to):
        node_id = self.get_node_id()
        url = f"https://api-node{node_id}.calltouch.ru/calls-service/RestAPI/requests"
        params = {'clientApiId': self.__ct_token, 'dateFrom': date_from, 'dateTo': date_to}
        list_of_calls = requests.get(url, params=params).json()
        # TODO: {'errorCode': '500', 'message': 'При обработке вашего запроса произошла ошибка. Обратитесь в службу технической поддержки.'}
        return list_of_calls

    def get_calls(self, date_from, date_to):
        i = 1
        total_result = []
        pages = self.__get_pages(date_from, date_to)
        keys = ['callId', 'callerNumber', 'date', 'waitingConnect', 'duration', 'phoneNumber', 'successful',
                'uniqueCall', 'targetCall', 'uniqTargetCall', 'callbackCall', 'city', 'source', 'medium', 'keyword',
                'callUrl', 'utmSource', 'utmMedium', 'utmCampaign', 'utmContent', 'utmTerm', 'sessionId', 'ctCallerId',
                'clientId', 'yaClientId', 'sipCallId', 'callTags', 'callUrl']

        while i <= pages:
            params = {'clientApiId': self.__ct_token, 'dateFrom': date_from, 'dateTo': date_to, 'page': i,
                      'limit': 1000, 'withCallTags': True}
            list_of_calls = requests.get(self.__url + "calls-diary/calls", params=params).json()['records']
            i += 1
            for call in list_of_calls:
                data = call.copy()
                for key, values in call.items():
                    if key not in keys:
                        data.pop(key)
                    elif key == 'callTags':
                        for one in values:
                            if one['type'] == 'AUTO-PR':
                                data["AUTO_PR"] = ",".join(one['names'])
                            elif one['type'] == 'MANUAL':
                                data["MANUAL"] = ",".join(one['names'])
                        data.pop("callTags")
                total_result.append(data)

        return total_result
