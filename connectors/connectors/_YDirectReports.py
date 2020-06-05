from connectors.connectors._Utils import create_fields
import requests, json, time, re


class YandexDirectReports:
    def __init__(self, access_token, client_login, client_name):
        self.url = "https://api.direct.yandex.com/json/v5/"
        self.headers_report = {
           "Authorization": "Bearer " + access_token,
           "Client-Login": client_login,
           "Accept-Language": "ru",
           "processingMode": "auto",
           "returnMoneyInMicros": "false",
           "skipReportHeader": "true",
           "skipReportSummary": "true"}

        self.report_dict = {
            "CAMPAIGN_STAT": {
                "type":
                    "CUSTOM_REPORT",
                "fields": {
                    "Date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "CampaignId": {"type": "INTEGER", "mode": "NULLABLE", "description": "CampaignId :INTEGER"},
                    "CampaignName": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignName :STRING"},
                    "CampaignType": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignType :STRING"},
                    "Impressions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Impressions :INTEGER"},
                    "Cost": {"type": "FLOAT", "mode": "NULLABLE", "description": "Cost :FLOAT"},
                    "Clicks": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"}}},
            "CAMPAIGN_DEVICE_AND_PLACEMENT_STAT": {
                "type":
                    "CUSTOM_REPORT",
                "fields": {
                    "Date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "CampaignId": {"type": "INTEGER", "mode": "NULLABLE", "description": "CampaignId :INTEGER"},
                    "CampaignType": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignType :STRING"},
                    "CampaignName": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignName :STRING"},
                    "AdNetworkType": {"type": "STRING", "mode": "NULLABLE", "description": "AdNetworkType :STRING"},
                    "Device": {"type": "STRING", "mode": "NULLABLE", "description": "Device :STRING"},
                    "Impressions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Impressions :INTEGER"},
                    "Cost": {"type": "FLOAT", "mode": "NULLABLE", "description": "Cost :FLOAT"},
                    "Clicks": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"}}},
            "CAMPAIGN_GEO_STAT": {
                "type":
                    "CAMPAIGN_PERFORMANCE_REPORT",
                "fields": {
                    "Date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "CampaignId": {"type": "INTEGER", "mode": "NULLABLE", "description": "CampaignId :INTEGER"},
                    "CampaignName": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignName :STRING"},
                    "CampaignType": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignType :STRING"},
                    "LocationOfPresenceName": {"type": "STRING", "mode": "NULLABLE",
                                               "description": "LocationOfPresenceName :STRING"},
                    "TargetingLocationId": {"type": "INTEGER", "mode": "NULLABLE",
                                            "description": "TargetingLocationId :INTEGER"},
                    "Impressions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Impressions :INTEGER"},
                    "Cost": {"type": "FLOAT", "mode": "NULLABLE", "description": "Cost :FLOAT"},
                    "Clicks": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"}}},
            "CAMPAIGN_PLACEMENT_STAT": {
                "type":
                    "CAMPAIGN_PERFORMANCE_REPORT",
                "fields": {
                    "Date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "CampaignId": {"type": "INTEGER", "mode": "NULLABLE", "description": "CampaignId :INTEGER"},
                    "CampaignName": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignName :STRING"},
                    "CampaignType": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignType :STRING"},
                    "Placement": {"type": "STRING", "mode": "NULLABLE", "description": "Placement :STRING"},
                    "Impressions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Impressions :INTEGER"},
                    "Cost": {"type": "FLOAT", "mode": "NULLABLE", "description": "Cost :FLOAT"},
                    "Clicks": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"}}},
            "CAMPAIGN_SOCDEM_DEVICE_STAT": {
                "type":
                    "CAMPAIGN_PERFORMANCE_REPORT",
                "fields": {
                    "Date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "CampaignId": {"type": "INTEGER", "mode": "NULLABLE", "description": "CampaignId :INTEGER"},
                    "CampaignName": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignName :STRING"},
                    "Device": {"type": "STRING", "mode": "NULLABLE", "description": "Device :STRING"},
                    "CarrierType": {"type": "STRING", "mode": "NULLABLE", "description": "CarrierType :STRING"},
                    "CampaignType": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignType :STRING"},
                    "MobilePlatform": {"type": "STRING", "mode": "NULLABLE", "description": "MobilePlatform :STRING"},
                    "Age": {"type": "STRING", "mode": "NULLABLE", "description": "Age :STRING"},
                    "Gender": {"type": "STRING", "mode": "NULLABLE", "description": "Gender :STRING"},
                    "Impressions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Impressions :INTEGER"},
                    "Cost": {"type": "FLOAT", "mode": "NULLABLE", "description": "Cost :FLOAT"},
                    "Clicks": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"}}},
            "AD_STAT": {
                "type":
                    "AD_PERFORMANCE_REPORT",
                "fields": {
                    "Date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "CampaignId": {"type": "INTEGER", "mode": "NULLABLE", "description": "CampaignId :INTEGER"},
                    "CampaignName": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignName :STRING"},
                    "CampaignType": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignType :STRING"},
                    "AdId": {"type": "INTEGER", "mode": "NULLABLE", "description": "AdId :INTEGER"},
                    "AdFormat": {"type": "STRING", "mode": "NULLABLE", "description": "AdFormat :STRING"},
                    "AdGroupId": {"type": "INTEGER", "mode": "NULLABLE", "description": "AdGroupId :INTEGER"},
                    "AdGroupName": {"type": "STRING", "mode": "NULLABLE", "description": "AdGroupName :STRING"},
                    "Impressions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Impressions :INTEGER"},
                    "Cost": {"type": "FLOAT", "mode": "NULLABLE", "description": "Cost :FLOAT"},
                    "Clicks": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"}}},
            "AD_DEVICE_STAT": {
                "type":
                    "AD_PERFORMANCE_REPORT",
                "fields": {
                    "Date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "CampaignId": {"type": "INTEGER", "mode": "NULLABLE", "description": "CampaignId :INTEGER"},
                    "CampaignName": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignName :STRING"},
                    "CampaignType": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignType :STRING"},
                    "AdId": {"type": "INTEGER", "mode": "NULLABLE", "description": "AdId :INTEGER"},
                    "AdFormat": {"type": "STRING", "mode": "NULLABLE", "description": "AdFormat :STRING"},
                    "AdGroupId": {"type": "INTEGER", "mode": "NULLABLE", "description": "AdGroupId :INTEGER"},
                    "AdGroupName": {"type": "STRING", "mode": "NULLABLE", "description": "AdGroupName :STRING"},
                    "Device": {"type": "STRING", "mode": "NULLABLE", "description": "Device :STRING"},
                    "AdNetworkType": {"type": "STRING", "mode": "NULLABLE", "description": "AdNetworkType :STRING"},
                    "Impressions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Impressions :INTEGER"},
                    "Cost": {"type": "FLOAT", "mode": "NULLABLE", "description": "Cost :FLOAT"},
                    "Clicks": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"}}},
            "REACH_AND_FREQUENCY_STAT": {
                "type":
                    "REACH_AND_FREQUENCY_PERFORMANCE_REPORT",
                "fields": {
                    "Date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "CampaignId": {"type": "INTEGER", "mode": "NULLABLE", "description": "CampaignId :INTEGER"},
                    "CampaignName": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignName :STRING"},
                    "CampaignType": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignType :STRING"},
                    "AdGroupId": {"type": "INTEGER", "mode": "NULLABLE", "description": "AdGroupId :INTEGER"},
                    "AdGroupName": {"type": "STRING", "mode": "NULLABLE", "description": "AdGroupName :STRING"},
                    "AdId": {"type": "INTEGER", "mode": "NULLABLE", "description": "AdId :INTEGER"},
                    "Impressions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Impressions :INTEGER"},
                    "ImpressionReach": {"type": "INTEGER", "mode": "NULLABLE",
                                        "description": "ImpressionReach :INTEGER"},
                    "AvgImpressionFrequency": {"type": "FLOAT", "mode": "NULLABLE",
                                               "description": "AvgImpressionFrequency :FLOAT"},
                    "Cost": {"type": "FLOAT", "mode": "NULLABLE", "description": "Cost :FLOAT"},
                    "Clicks": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"}}},
            "KEYWORD_AD_STAT": {
                "type":
                    "CUSTOM_REPORT",
                "fields": {
                    "Date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "CampaignId": {"type": "INTEGER", "mode": "NULLABLE", "description": "CampaignId :INTEGER"},
                    "CampaignName": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignName :STRING"},
                    "CampaignType": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignType :STRING"},
                    "AdGroupId": {"type": "INTEGER", "mode": "NULLABLE", "description": "AdGroupId :INTEGER"},
                    "AdGroupName": {"type": "STRING", "mode": "NULLABLE", "description": "AdGroupName :STRING"},
                    "AdId": {"type": "INTEGER", "mode": "NULLABLE", "description": "AdId :INTEGER"},
                    "Impressions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Impressions :INTEGER"},
                    "Clicks": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"},
                    "Cost": {"type": "FLOAT", "mode": "NULLABLE", "description": "Cost :FLOAT"},
                    "CriterionId": {"type": "INTEGER", "mode": "NULLABLE", "description": "CriterionId :INTEGER"},
                    "Criterion": {"type": "STRING", "mode": "NULLABLE", "description": "Criterion :STRING"},
                    "CriteriaType": {"type": "STRING", "mode": "NULLABLE", "description": "CriteriaType :STRING"}}},
            "KEYWORD_SOCDEM_STAT": {
                "type":
                    "CUSTOM_REPORT",
                "fields": {
                    "Date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "CampaignId": {"type": "INTEGER", "mode": "NULLABLE", "description": "CampaignId :INTEGER"},
                    "CampaignName": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignName :STRING"},
                    "CampaignType": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignType :STRING"},
                    "AdGroupId": {"type": "INTEGER", "mode": "NULLABLE", "description": "AdGroupId :INTEGER"},
                    "AdGroupName": {"type": "STRING", "mode": "NULLABLE", "description": "AdGroupName :STRING"},
                    "AdId": {"type": "INTEGER", "mode": "NULLABLE", "description": "AdId :INTEGER"},
                    "CriterionId": {"type": "INTEGER", "mode": "NULLABLE", "description": "CriterionId :INTEGER"},
                    "Criterion": {"type": "STRING", "mode": "NULLABLE", "description": "Criterion :STRING"},
                    "CriteriaType": {"type": "STRING", "mode": "NULLABLE", "description": "CriteriaType :STRING"},
                    "Slot": {"type": "STRING", "mode": "NULLABLE", "description": "Slot :STRING"},
                    "Age": {"type": "STRING", "mode": "NULLABLE", "description": "Age :STRING"},
                    "Gender": {"type": "STRING", "mode": "NULLABLE", "description": "Gender :STRING"},
                    "Impressions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Impressions :INTEGER"},
                    "Cost": {"type": "FLOAT", "mode": "NULLABLE", "description": "Cost :FLOAT"},
                    "Clicks": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"}}},
            "KEYWORD_DEVICE_STAT": {
                "type":
                    "CRITERIA_PERFORMANCE_REPORT",
                "fields": {
                    "Date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "CampaignId": {"type": "INTEGER", "mode": "NULLABLE", "description": "CampaignId :INTEGER"},
                    "CampaignName": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignName :STRING"},
                    "CampaignType": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignType :STRING"},
                    "Device": {"type": "STRING", "mode": "NULLABLE", "description": "Device :STRING"},
                    "Impressions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Impressions :INTEGER"},
                    "Cost": {"type": "FLOAT", "mode": "NULLABLE", "description": "Cost :FLOAT"},
                    "Clicks": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"},
                    "CriterionId": {"type": "INTEGER", "mode": "NULLABLE", "description": "CriterionId :INTEGER"},
                    "Criterion": {"type": "STRING", "mode": "NULLABLE", "description": "Criterion :STRING"},
                    "CriteriaType": {"type": "STRING", "mode": "NULLABLE", "description": "CriteriaType :STRING"}}},
            "KEYWORD_DEVICE_AD_STAT": {
                "type":
                    "CUSTOM_REPORT",
                "fields": {
                    "Date": {"type": "DATE", "mode": "NULLABLE", "description": "Day :DATE"},
                    "CampaignId": {"type": "INTEGER", "mode": "NULLABLE", "description": "CampaignId :INTEGER"},
                    "CampaignName": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignName :STRING"},
                    "CampaignType": {"type": "STRING", "mode": "NULLABLE", "description": "CampaignType :STRING"},
                    "AdGroupId": {"type": "INTEGER", "mode": "NULLABLE", "description": "AdGroupId :INTEGER"},
                    "AdGroupName": {"type": "STRING", "mode": "NULLABLE", "description": "AdGroupName :STRING"},
                    "AdId": {"type": "INTEGER", "mode": "NULLABLE", "description": "AdId :INTEGER"},
                    "Impressions": {"type": "INTEGER", "mode": "NULLABLE", "description": "Impressions :INTEGER"},
                    "Cost": {"type": "FLOAT", "mode": "NULLABLE", "description": "Cost :FLOAT"},
                    "Clicks": {"type": "INTEGER", "mode": "NULLABLE", "description": "Clicks :INTEGER"},
                    "CriterionId": {"type": "INTEGER", "mode": "NULLABLE", "description": "CriterionId :INTEGER"},
                    "Criterion": {"type": "STRING", "mode": "NULLABLE", "description": "Criterion :STRING"},
                    "CriterionType": {"type": "STRING", "mode": "NULLABLE", "description": "CriterionType :STRING"},
                    "AdNetworkType": {"type": "STRING", "mode": "NULLABLE", "description": "AdNetworkType :STRING"},
                    "Device": {"type": "STRING", "mode": "NULLABLE", "description": "Device :STRING"},
                    "Slot": {"type": "STRING", "mode": "NULLABLE", "description": "Slot :STRING"},
                    "Placement": {"type": "STRING", "mode": "NULLABLE", "description": "Placement :STRING"},
                    "TargetingLocationId": {"type": "INTEGER", "mode": "NULLABLE",
                                            "description": "TargetingLocationId :INTEGER"},
                    "TargetingLocationName": {"type": "STRING", "mode": "NULLABLE",
                                              "description": "TargetingLocationName :STRING"}}}
        }
        self.client_id = re.sub('[.-]', '_', client_login)
        self.tables_with_schema, self.fields = create_fields(client_name, "YaDirect", self.report_dict, self.client_id)

    def __create_body(self, selection_criteria, field_names, report_name, report_type):
        body = {
            "params": {
                "SelectionCriteria": selection_criteria,
                "FieldNames": field_names,
                "ReportName": (report_name),
                "ReportType": report_type,
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "NO",
                "IncludeDiscount": "NO"
            }
        }
        jsonBody = json.dumps(body, ensure_ascii=False).encode('utf8')
        return jsonBody

    def __request(self, selection_criteria, field_names, report_name, report_type, method):
        jsonBody = self.__create_body(selection_criteria, field_names, report_name, report_type)
        try:
            data = requests.post(self.url+method, jsonBody, headers=self.headers_report)
        except requests.exceptions.ConnectionError as error:
            print(error)
            data = requests.post(self.url + method, jsonBody, headers=self.headers_report)
        if data.status_code in [201, 202]:
            time.sleep(60)
            return self.__request(selection_criteria, field_names, report_name, report_type, method)
        return data

    def get_report(self, report_type, report_name, date_from, date_to):
        """
        report_name - report_type - fields:
         - CAMPAIGN_STAT - CUSTOM_REPORT
         - CAMPAIGN_DEVICE_AND_PLACEMENT_STAT - CUSTOM_REPORT
         - CAMPAIGN_GEO_STAT - CAMPAIGN_PERFORMANCE_REPORT
         - CAMPAIGN_PLACEMENT_STAT - CAMPAIGN_PERFORMANCE_REPORT
         - CAMPAIGN_SOCDEM_DEVICE_STAT - CAMPAIGN_PERFORMANCE_REPORT
         - AD_STAT - AD_PERFORMANCE_REPORT
         - AD_DEVICE_STAT - AD_PERFORMANCE_REPORT
         - REACH_AND_FREQUENCY_STAT - REACH_AND_FREQUENCY_PERFORMANCE_REPORT
         - KEYWORD_AD_STAT - CUSTOM_REPORT
         - KEYWORD_SOCDEM_STAT - CUSTOM_REPORT
         - KEYWORD_DEVICE_STAT - CRITERIA_PERFORMANCE_REPORT
         - KEYWORD_DEVICE_AD_STAT - CUSTOM_REPORT

         date format: "YYYY-MM-DD"

        """

        get_data_params = self.report_dict.get(report_type, False)
        if get_data_params:
            selection_criteria = {"DateFrom": date_from, "DateTo": date_to}
            field_names = list(get_data_params['fields'].keys())
            data = self.__request(selection_criteria, field_names, report_name, get_data_params['type'], "reports")
            return data
        else:
            return "Указан недопустимый тип отчета."
