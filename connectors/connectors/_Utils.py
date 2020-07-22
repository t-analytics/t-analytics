from datetime import datetime, timedelta


def pop_keys(keys, data):
    result = []
    for one_dict in data:
        middle = one_dict.copy()
        for element in one_dict.keys():
            if element not in keys:
                middle.pop(element)
        result.append(middle)

    return result


def my_slice(slice_ids, limit=5, slice_list=None):
    if slice_list is None:
        slice_list = []
    count = len(slice_ids)
    if count > limit:
        slice_list.append(slice_ids[:limit])
        return my_slice(slice_ids[limit:], limit, slice_list=slice_list)
    else:
        slice_list.append(slice_ids)
        return slice_list


def slice_date_on_period(date_from, date_to, period):
    date_range = []
    date_from_dt = datetime.strptime(date_from, "%Y-%m-%d")
    date_to_dt = datetime.strptime(date_to, "%Y-%m-%d")
    time_delta = (date_to_dt - date_from_dt).days
    if time_delta > period:
        while date_from_dt <= date_to_dt:
            if (date_to_dt - date_from_dt).days >= period - 1:

                date_range.append((datetime.strftime(date_from_dt, "%Y-%m-%d"),
                                   datetime.strftime(date_from_dt + timedelta(days=period - 1), "%Y-%m-%d")))
                date_from_dt += timedelta(days=period)

            else:
                dif_days = (date_to_dt - date_from_dt).days

                date_range.append((datetime.strftime(date_from_dt, "%Y-%m-%d"),
                                   datetime.strftime(date_from_dt + timedelta(days=dif_days), "%Y-%m-%d")))
                date_from_dt += timedelta(days=dif_days + 1)
        return date_range
    else:
        return [(date_from, date_to)]


def convert_data_frame_as_type(data_frame, fields, datetime_format=None, date_format=None, time_format=None):
    columns_name = list(data_frame.columns)
    for field_type, field_list in fields.items():
        fields_to_convert = list(set(columns_name).intersection(set(field_list)))
        if field_type in ["int", "str", "float", "bool"]:
            data_frame = data_frame.astype({x: field_type for x in fields_to_convert})

        elif field_type == 'datetime':
            for column in fields_to_convert:
                data_frame[column] = data_frame[column].apply(lambda x:
                                                              datetime.strftime(datetime.strptime(x, datetime_format),
                                                                                "%Y-%m-%d %H:%M:%S"))

        elif field_type == 'date':
            for column in fields_to_convert:
                data_frame[column] = data_frame[column].apply(lambda x:
                                                              datetime.strftime(datetime.strptime(x, date_format),
                                                                                "%Y-%m-%d"))
        elif field_type == 'time':
            for column in fields_to_convert:
                data_frame[column] = data_frame[column].apply(lambda x:
                                                              datetime.strftime(datetime.strptime(x, time_format),
                                                                                "%H:%M:%S"))

    return data_frame


def prepare_fields_type(report_dict):
    result = {"date": [], "datetime": [], "time": [], "int": [], "str": [],
              "float": [], "bool": []}

    for report_name, value in report_dict.items():
        value = value['fields']
        for name, params in value.items():
            if params['type'] == "RECORD":
                params = params['content']
                for row in params:
                    result = check_types(row['name'], row, result)
            else:
                result = check_types(name, params, result)
    result = {key: list(set(value)) for key, value in result.items()}
    return result


def check_types(name, params, result):
    if params['type'] == "INTEGER":
        result['int'].append(name)
    elif params['type'] == "STRING":
        result['str'].append(name)
    elif params['type'] == "BOOLEAN":
        result['bool'].append(name)
    elif params['type'] == "TIME":
        result['time'].append(name)
    elif params['type'] == "DATE":
        result['date'].append(name)
    elif params['type'] == "DATETIME":
        result['datetime'].append(name)
    elif params['type'] == "FLOAT":
        result['float'].append(name)
    else:
        raise Exception("Unknown type")
    return result


def create_fields(client_name, platform, report_dict, client_id):
    if client_id is not None:
        tables_with_schema = {f"{client_name}_{platform}_{client_id}_{report_name}": report_dict[report_name]['fields']
                              for report_name in list(report_dict.keys())}
    else:
        tables_with_schema = {f"{client_name}_{platform}_{report_name}": report_dict[report_name]['fields']
                              for report_name in list(report_dict.keys())}

    fields = prepare_fields_type(report_dict)
    return tables_with_schema, fields


def create_fields_ga(client_name, platform, report_dict, client_id):
    result = {"date": [], "datetime": [], "time": [], "int": [], "str": [],
              "float": [], "bool": []}

    for report_name, value in report_dict.items():
        metrics = value['metrics']
        dimensions = value['dimensions']
        metrics.update(dimensions)
        for name, params in metrics.items():
            if params['type'] == "RECORD":
                params = params['content']
                for row in params:
                    result = check_types(row['name'], row, result)
            else:
                result = check_types(name, params, result)
    result = {key: list(set(value)) for key, value in result.items()}

    tables_with_schema = {f"{client_name}_{platform}_{client_id}_{report_name}":
                              dict(list(report_dict[report_name]['metrics'].items()) +
                                   list(report_dict[report_name]['dimensions'].items()))
                          for report_name in list(report_dict.keys())}

    return tables_with_schema, result


def expand_dict(data_to_expand, dict_with_keys, dict_with_data):
    if isinstance(data_to_expand, dict):
        for key, value in data_to_expand.items():
            if isinstance(value, str) or isinstance(value, int) or isinstance(value, float):
                if key in dict_with_keys.keys():
                    dict_with_data[dict_with_keys[key]] = value
                else:
                    dict_with_data[key] = value
            else:
                dict_with_data = expand_dict(value, dict_with_keys, dict_with_data)
    elif isinstance(data_to_expand, list):
        for element_of_list in data_to_expand:
            dict_with_data = expand_dict(element_of_list, dict_with_keys, dict_with_data)
    return dict_with_data

