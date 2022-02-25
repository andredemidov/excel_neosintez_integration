import requests
import json
import pandas as pd
from datetime import datetime
import os
from time import ctime

url = 'http://construction.irkutskoil.ru/'
xl_directory = open('xl_directory.txt', encoding='utf-8').read()
atr_data = pd.read_excel('default_attributes.xlsx')  # дата фрейм для мэпинга атрибутов и колонок эксель файла
folder_class_id = '3b417b9a-bd8e-ec11-911d-005056b6948b'  # класс папки, в которую идет импорт данных
mvz_folder_class_id = '288a12fc-ad8f-ec11-911d-005056b6948b'  # класс промежуточной папки создаваемой по каждому мвз
class_id = 'b0379bb3-cc70-e911-8115-817c3f53a992'  # класс для каждой записи импортируемого файла
attribute_id = '4903a891-f402-eb11-9110-005056b6948b'  # id атрибуто по которому осуществляется поиск. Здесь это номер потребности
mvz_attribute_id = '626370d8-ad8f-ec11-911d-005056b6948b'
start_time = datetime.now()


def authentification():  # функция возвращает токен для атуентификации. Учетные данные берет из файла
    req_url = url + 'connect/token'
    f = open('auth_data.txt')
    payload = f.read()
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    token = json.loads(requests.post(req_url, data=payload, headers=headers).text)['access_token']

    return token


def get_req_body(row):  # получене тела PUT запроса для строки файла эксель
    row_body = []

    for j, atr in atr_data.iterrows():
        atr_value = str(row[atr['name']])
        if atr_value == 'nan':
            continue
        atr_id = atr['id']
        atr_type = atr['type']
        if atr_type == 1:
            atr_value = float(atr_value)
        elif atr_type == 3 or atr_type == 5: # обработка значений типа дата и дата и время
            atr_value = datetime.strptime(atr_value, '%d.%m.%Y')
            atr_value = atr_value.strftime("%Y-%m-%d")
        elif atr_type == 8 and atr['name'] == 'ЕИ':  # предусмотрена только обработка ЕИ
            atr_value = atr_value.replace('.', '')
            response = find_item(item_number=atr_value, attribute_id='ec653d26-8375-e911-8115-817c3f53a992', folder_id='df0921c1-f46f-e911-8115-817c3f53a992', class_id='0e1d8277-d859-e911-8115-817c3f53a992')
            if response['Total'] == 1:
                n_id = response['Result'][0]['Object']['Id']  # извлечение id из ответа на поисковый запрос
                atr_value = {'Id': n_id, 'Name': 'forvalidation'}
            else:
                continue
        elif atr_type == 8:
            continue

        atr_body = {}
        atr_body['Name'], atr_body['Value'], atr_body['Type'], atr_body[
            'Id'] = 'forvalidation', atr_value, atr_type, atr_id
        row_body.append(atr_body)

    return row_body


def import_excel_to_folder(folder_id, xl_data):
    counter_success = 0
    counter_exception = 0
    for i, row in xl_data.iterrows():
        item_number = row['Потребность.Номер']  # номер потребности по строке
        item_name = row['Номенклатурная позиция']
        neosintez_id = get_neosintez_id(item_number, attribute_id, folder_id, item_name, class_id)
        req_body = get_req_body(row)
        result = put_attributes(req_body, neosintez_id)
        if result == 200:
            counter_success += 1
        else:
            counter_exception += 1
        # print(f'запрос по обновлению потребности {item_number} выполнен со статусом {result}')  # для дебага
    return counter_success, counter_exception


def put_attributes(req_body, neosintez_id):
    req_url = url + f'api/objects/{neosintez_id}/attributes'
    payload = json.dumps(req_body)

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json-patch+json'
    }
    response = requests.put(req_url, headers=headers, data=payload)
    if response.status_code != 200:
        print(req_body)
        print(response.text)
        pass
    return response.status_code


def find_item(item_number, attribute_id, folder_id, class_id):  # возвращает ответ поискового запроса целиком
    req_url = url + 'api/objects/search?take=30'
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
        "Conditions": [  # условия для поиска в Неосинтез
            {
                "Type": 1,  # тип атрибута 1 - строка
                "Attribute": attribute_id,  # id атрибута в Неосинтез
                "Operator": 1,  # оператор сравнения. 1 - равно
                "Value": item_number  # значение атрибута в Неосинтез
            }
        ]
    })
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json-patch+json',
        'X-HTTP-Method-Override': 'GET'
    }
    response = json.loads(
        requests.post(req_url, headers=headers, data=payload).text)  # поисковый запрос с десериализацией ответа
    return response


def create_item(item_number, attribute_id, folder_id, item_name, class_id):  # возвращает neosintez_id созданной записи или папки
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
    response = json.loads(
        requests.post(req_url, headers=headers, data=payload).text)  # создание объекта с десериализацией ответа
    neosintez_id = response['Id']

    req_body = [{"Name": "forvalidation", "Value": item_number, "Type": 2, "Id": attribute_id}]
    put_attributes(req_body, neosintez_id) # заполнить атрибут с номером МВЗ или номером потребности

    return neosintez_id


def get_neosintez_id(item_number, attribute_id, folder_id,
                     item_name, class_id):  # функция ищет существующую потребность по номеру и создает новую если не находит. Возвращает id из Неосинтеза
    response = find_item(item_number, attribute_id, folder_id, class_id)
    total = response['Total']  # в ответе total - это количество найденных результатов по условию поиска
    if total == 1:
        neosintez_id = response['Result'][0]['Object']['Id']  # извлечение id из ответа на поисковый запрос
        # print('объект найден')
    elif total == 0:
        neosintez_id = create_item(item_number, attribute_id, folder_id, item_name, class_id)
        # print('объект будет создан')
    else:
        pass  # это ошибка. Потребностей с одним и тем же номером быть не должно
    return neosintez_id


def get_MTO_folders_dict():
    folders_dict = {}
    req_url = url + 'api/objects/search?take=100'
    payload = json.dumps({
        "Filters": [
            {
                "Type": 5,
                "Value": folder_class_id  # id класса в Неосинтез
            }
        ],
        "Conditions": [  # условия для поиска в Неосинтез
            {
                "Type": 1,  # тип условия 1 - атрибут
                "Attribute": 'bfbd61bc-bd8e-ec11-911d-005056b6948b',  # id атрибута в Неосинтез
                "Operator": 7  # оператор сравнения. 7 - существует
            }
        ]
    })
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json-patch+json',
        'X-HTTP-Method-Override': 'GET'
    }
    response = json.loads(
        requests.post(req_url, headers=headers, data=payload).text)  # поисковый запрос с десериализацией ответа
    for folder in response['Result']:
        folders_dict[folder['Object']['Id']] = folder['Object']['Attributes']['bfbd61bc-bd8e-ec11-911d-005056b6948b']['Value'].split(';')


    return folders_dict


def get_xl_data(mvz):
    f_list = [f for f in os.listdir(path=xl_directory) if mvz in f and 'ЗО' in f]
    f_date = [ctime(os.path.getctime(xl_directory+f)) for f in f_list]
    f_path = xl_directory + f_list[f_date.index(max(f_date))]
    xl_data = pd.read_excel(f_path, sheet_name='TDSheet', converters={'Код (НСИ)': str, 'Потребность.Номер': str})
    return xl_data


try:
    token = authentification()  # токен для подключения к api
except:
    print('Ошибка аутентификации')


def integration(): # главный процесс

    folders_dict = get_MTO_folders_dict()  # словарь с id папок в неосинтезе и со списком мвз по каждой папке
    print(folders_dict)
    counter = 0
    for folder in folders_dict:
        print(f'Количество МВЗ по текущей папке {len(folders_dict[folder])}') # количество МВЗ по текущей папке
        for mvz in folders_dict[folder]:

            print(f'Начат импорт МВЗ {mvz} в папку {folder}. ', end='')
            try:
                xl_data = get_xl_data(mvz)  # получить дата фрейм из файла эксель по нужным мвз
                print(f'Файл {mvz} найден. Строк в эксель всего {len(xl_data.index)}')
            except:
                print(f'Файл {mvz} не найден')
                continue

            counter += 1

            folder_id = get_neosintez_id(item_number=mvz, attribute_id=mvz_attribute_id, folder_id=folder, item_name=mvz, class_id=mvz_folder_class_id)

            counter_success , counter_exception = import_excel_to_folder(folder_id, xl_data)
            print(f'{counter}. Успешно обновлено {counter_success} строк, ошибок {counter_exception}')

    print(f'Обработано файлов {counter}')

integration()

print(datetime.now() - start_time)


