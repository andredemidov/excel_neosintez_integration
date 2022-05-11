import requests
import json

def authentification(url, aut_string):  # функция возвращает токен для атуентификации. Учетные данные берет из файла
    req_url = url + 'connect/token'
    payload = aut_string  # строка вида grant_type=password&username=????&password=??????&client_id=??????&client_secret=??????
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    response = requests.post(req_url, data=payload, headers=headers)
    if response.status_code == 200:
        token = json.loads(response.text)['access_token']
    else:
        token = ''
    return token

def put_attributes(url, token, req_body, neosintez_id):  # функция обновляет атрибуты объекта в неосинтез и возвращает весь ответ
    req_url = url + f'api/objects/{neosintez_id}/attributes'  # id сущности, в которой меняем атрибут
    payload = json.dumps(req_body)  # тело запроса в виде списка/словаря

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json-patch+json'
    }
    response = requests.put(req_url, headers=headers, data=payload)
    if response.status_code != 200:
        # print(req_body)
        # print(response.text)
        pass
    return response


def find_item(*, url, token, attribute_value=None, item_name, folder_id, class_id, attribute_id=None, operator=1):  # возвращает ответ поискового запроса целиком
    req_url = url + 'api/objects/search?take=30000'
    condition_dict = {}
    condition_dict['Value'] = item_name # названия
    condition_dict['Operator'] = operator  # оператор сравнения. 1 - равно
    if attribute_id is not None:
        condition_dict['Value'] = attribute_value # значение атрибута
        condition_dict['Type'] = 1  # тип поиска
        condition_dict['Attribute'] = attribute_id # id атрибута в Неосинтез
    else:
        condition_dict['Type'] = 2
    conditions = [condition_dict]

    payload = json.dumps({
        "Filters": [
            {
                "Type": 4,
                "Value": folder_id  # id узла поиска в Неосинтез
            },
            {
                "Type": 5,
                "Value": class_id  # id класса в Неосинтез
            }
        ],
        "Conditions": conditions  # условия для поиска в Неосинтез
    })
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json-patch+json',
        'X-HTTP-Method-Override': 'GET'
    }
    response = requests.post(req_url, headers=headers, data=payload)  # поисковый запрос с десериализацией ответа
    return response


def create_item(*, url, token, attribute_value=None, attribute_id=None, folder_id, item_name, class_id):  # возвращает neosintez_id созданной сущности
    req_url = url + f'api/objects?parent={folder_id}'
    payload = json.dumps({
        "Id": "00000000-0000-0000-0000-000000000000",
        "Name": item_name,
        "Entity": {
            "Id": class_id,
            "Name": "forvalidation"
        }
    })
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json-patch+json'
    }
    response = requests.post(req_url, headers=headers, data=payload)  # создание объекта
    response_text = json.loads(response.text)  # создание объекта с десериализацией ответа

    if response.status_code == 200:
        neosintez_id = response_text['Id']
        if attribute_value != None:  # если передан так же атрибут для заполнения, то его заполнить у созданного объекта
            req_body = [{"Name": "forvalidation", "Value": attribute_value, "Type": 2, "Id": attribute_id}]
            put_attributes(url=url,token=token, req_body=req_body, neosintez_id=neosintez_id) # заполнить атрибут
    else:
        neosintez_id = ''  # возвращает пустую строку в случае ошибки запроса

    return neosintez_id, response

def delete_item(url, token, neosintez_id, bin):
    req_url = url + f'api/objects/{neosintez_id}/parent?parentId={bin}'
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json-patch+json'
    }
    response = requests.put(req_url, headers=headers)
    return response