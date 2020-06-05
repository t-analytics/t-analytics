import imaplib, email, os
from email.header import decode_header
from datetime import datetime, timedelta
from connectors.connectors._BigQuery import BigQuery
from connectors.connectors._Utils import create_fields


class Email:
    def __init__(self, path_to_bq, client_name, placement, login, password, params_for_report_dict, path_to_save):
        self.path_to_save = path_to_save
        self.email_password = password
        self.email_user = login
        self.placement = placement
        self.client_name = client_name
        self.bq = BigQuery(path_to_bq)
        self.data_set_id = f"{client_name}_Email_{placement}"

        """
        params_for_report_dict: {"REPORT": [{"type": "STRING", "description": "desk", "param": "Impressions"}, 
                                            {"type": "FLOAT", "description": "desk", "param": "clicks"}]}
        """

        self.report_dict = {}
        for report_name, params in params_for_report_dict.items():
            self.report_dict.setdefault(report_name, {})
            for element in params:
                self.report_dict[report_name].update(
                    {
                        element['param']: {
                            "type": element['type'], "mode": "NULLABLE", "description": element["description"]
                        }
                    })

        self.tables_with_schema, self.fields = create_fields(client_name, "Email", self.report_dict, placement)

        self.bq.check_or_create_data_set(self.data_set_id)
        self.bq.check_or_create_tables(self.tables_with_schema, self.data_set_id)

    def get_email(self, email_from, subject_filter, host='imap.yandex.ru', port=993):

        mail = imaplib.IMAP4_SSL(host, port)
        mail.login(self.email_user, self.email_password)
        mail.select('INBOX')

        date_from = datetime.strftime(datetime.today() - timedelta(days=1), "%d-%b-%Y")
        date_to = datetime.strftime(datetime.today(), "%d-%b-%Y")

        status_search, search_result = mail.uid('search', None, f'(FROM "{email_from}")', f'(SINCE "{date_from}")',
                                                f'(BEFORE "{date_to}")')
        list_of_ids_str = search_result[0].decode("utf-8").split(" ")
        if list_of_ids_str[0] == '':
            return []
        else:
            list_of_ids_int = [int(num) for num in list_of_ids_str]
            list_for_download = list(set(list_of_ids_int).difference(set(list_of_ids_int)))
            list_of_file_names = []
            for letter_id in list_for_download:
                mail.select('INBOX')
                status_result, letter_data = mail.uid('fetch', letter_id, '(RFC822)')
                raw_email = letter_data[0][1]
                email_message = email.message_from_bytes(raw_email)
                subject_list = decode_header(email_message['Subject'])
                code = subject_list[0][1]
                subject = subject_list[0][0].decode(code)

                if subject_filter in subject:
                    for part in email_message.walk():
                        if part.get_content_maintype() == 'multipart':
                            continue
                        if part.get('Content-Disposition') is None:
                            continue
                        file_name = part.get_filename()
                        fp = open(f"{self.path_to_save}{file_name}", 'wb')
                        fp.write(part.get_payload(decode=True))
                        fp.close()
                        list_of_file_names.append(file_name)
                    mail.close()
            return list_of_file_names

    def insert_email_to_bq(self, list_of_file_names, list_name):
        for file_name in list_of_file_names:
            pass
        return []

    def delete_download_files(self, list_of_file_names):
        for file_name in list_of_file_names:
            os.remove(self.path_to_save+file_name)
        return []


