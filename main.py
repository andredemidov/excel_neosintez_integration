import json
from datetime import datetime
import os
from time import ctime
import re
import shutil
import requests
import pandas as pd
import neosintez  # собственный модуль


url = 'http://construction.irkutskoil.ru/'

folder_class_id = '3b417b9a-bd8e-ec11-911d-005056b6948b'  # класс папки, в которую идет импорт данных
mvz_folder_class_id = '288a12fc-ad8f-ec11-911d-005056b6948b'  # класс промежуточной папки создаваемой по каждому мвз
sub_folder_class_id = '07a6ecdd-89a1-ea11-9103-005056b6e70e' # класс папки подобъекта
class_id = 'b0379bb3-cc70-e911-8115-817c3f53a992'  # класс для каждой записи импортируемого файла
attribute_id = '4903a891-f402-eb11-9110-005056b6948b'  # id атрибуто по которому осуществляется поиск. Здесь это номер потребности
mvz_attribute_id = '626370d8-ad8f-ec11-911d-005056b6948b'
bad_symbol = '«»³−–'
bin = 'f624e3ef-08d1-ec11-912c-005056b6948b' # корзина для удаления дублей и лишних. Переност занимает в 3 раза меньше времени, чем удаление

start_time = datetime.now()


def get_by_re(text, regexp):
    match = re.search(regexp, text)
    if match:
        result = match.group(1)
    else:
        result = ''
    return result


def float_atr(*, value, atr):
    return float(value)

def date_atr(*, value, atr):
    value = datetime.strptime(value, '%d.%m.%Y')
    value = value.strftime("%Y-%m-%d")
    return value

def ref_atr(*, value, atr):
    value = value.replace('.', '')
    folder_id = atr['folder']
    class_id = atr['class']
    response = neosintez.find_item(url=url, token=token, item_name=value, folder_id=folder_id, class_id=class_id)
    response = json.loads(response.text)
    if response['Total'] == 1:
        ref_id = response['Result'][0]['Object']['Id']  # извлечение id из ответа на поисковый запрос
        return {'Id': ref_id, 'Name': 'forvalidation'}
    else:
        return None

def str_atr(*, value, atr):
    return value

def del_bad_symbol(text:str):
    """удалаяет из названия подобъекта плохие символы"""
    for c in bad_symbol:
        text = text.replace(c, '')
    return text

def get_req_body(row):  # получене тела PUT запроса для строки файла эксель
    row_body = []
    func_dict = {
        1: float_atr,
        2: str_atr,
        3: date_atr,
        5: date_atr,
        8: ref_atr
    }

    for j, atr in atr_data.iterrows(): # основные атрибуты
        atr_value = str(row[atr['name']])
        atr_id = atr['id']
        atr_type = atr['type']
        if atr_value == 'nan':  # пропустить если значение пустое
           continue
        #  если указано регулярное выражение, то обработать строку с его помощью
        if str(atr['regexp']) != 'nan':
            atr_value = get_by_re(atr_value, str(atr['regexp']))

        atr_value = func_dict.get(atr_type, 2)(value=atr_value, atr=atr)

        if atr_value is None:  # пропустить если значение пустое
           continue

        atr_body = {'Name': 'forvalidation', 'Value': atr_value, 'Type': atr_type, 'Id': atr_id}
        row_body.append(atr_body)

    return row_body


def import_excel_to_folder(folder_id, xl_data):
    counter_success = 0
    counter_exception = 0
    for i, row in xl_data.iterrows():
        item_number = row['Потребность.Номер']  # номер потребности по строке
        item_name = row['Номенклатурная позиция']
        sub_folder = del_bad_symbol(row['Подобъект'])
        sub_folder_id = get_neosintez_id(folder_id=folder_id, item_name=sub_folder, class_id=sub_folder_class_id)
        neosintez_id = get_neosintez_id(attribute_value=item_number, attribute_id=attribute_id, folder_id=folder_id, item_name=item_name, class_id=class_id, sub_folder_id=sub_folder_id)
        req_body = get_req_body(row)
        result = neosintez.put_attributes(url, token, req_body, neosintez_id)
        if result.status_code == 200:
            counter_success += 1
        else:
            counter_exception += 1
        # print(f'запрос по обновлению потребности {item_number} выполнен со статусом {result}')  # для дебага
    return counter_success, counter_exception

def get_neosintez_id(*, attribute_value=None, attribute_id=None, folder_id,
                     item_name, class_id, sub_folder_id=None):
    """ функция ищет существующую потребность по номеру и создает новую если не находит. Возвращает id из Неосинтеза
    """
    response = neosintez.find_item(url=url, token=token, attribute_value=attribute_value, item_name=item_name, attribute_id=attribute_id, folder_id=folder_id, class_id=class_id)
    response = json.loads(response.text)
    # количество найденных результатов по условию поиска
    total = response['Total']

    if total == 1:
        neosintez_id = response['Result'][0]['Object']['Id']  # извлечение id из ответа на поисковый запрос
        # print('объект найден')
    elif total == 0:
        fold = sub_folder_id if sub_folder_id else folder_id
        neosintez_id, response = neosintez.create_item(url=url, token=token, attribute_value=attribute_value, attribute_id=attribute_id, folder_id=fold, item_name=item_name, class_id=class_id)
        if not neosintez_id:
            print(f'ошибка создания объекта {item_name}. Ответ: {response.text}')
    else:
        neosintez_id = ''
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


def get_amount(folder_id):
    req_url = url + 'api/objects/search?take=0'
    payload = json.dumps({
        "Filters": [
            {
                "Type": 4,
                "Value": folder_id  # id класса в Неосинтез
            },
            {
                "Type": 5,
                "Value": class_id  # id класса в Неосинтез
            }
        ]
    })
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json-patch+json',
        'X-HTTP-Method-Override': 'GET'
    }
    # поисковый запрос
    response = requests.post(req_url, headers=headers, data=payload)
    # извлечение количестова
    amount = json.loads(response.text)['Total'] if response.status_code == 200 else 0
    return amount

def get_number_set(folder_id, new_number_set):
    req_url = url + 'api/objects/search?take=20000'
    payload = json.dumps({
        "Filters": [
            {
                "Type": 4,
                "Value": folder_id  # id папки в Неосинтез
            },
            {
                "Type": 5,
                "Value": class_id  # id класса в Неосинтез
            }
        ]
    })
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json-patch+json',
        'X-HTTP-Method-Override': 'GET'
    }
    # поисковый запрос
    response = requests.post(req_url, headers=headers, data=payload)
    response = json.loads(response.text)
    # извлечение количестова
    number_dict = dict(map(lambda x: (x['Object']['Id'], x['Object']['Attributes']['4903a891-f402-eb11-9110-005056b6948b']['Value']), response['Result']))
    # кортеж идентификаторов дублей по номеру потребности
    double_id_tuple = tuple(filter(lambda k: k[1] > 1, map(lambda x: (x[0], list(number_dict.values()).count(x[1])), number_dict.items())))
    double_id_tuple = tuple(item[0] for item in double_id_tuple)

    # множество номеров потребностей
    number_set = set(number_dict.values())

    del_number_set = number_set - new_number_set
    del_id_tuple = tuple(filter(lambda x: x[1] in del_number_set, number_dict.items()))
    del_id_tuple = tuple(item[0] for item in del_id_tuple)

    return number_set, double_id_tuple, del_id_tuple


def get_xl_data(mvz):
    f_list = [f for f in os.listdir(path=xl_directory) if mvz in f and 'ЗО' in f]
    f_date = [ctime(os.path.getctime(xl_directory+f)) for f in f_list]
    f_path = xl_directory + f_list[f_date.index(max(f_date))]
    xl_new = pd.read_excel(f_path, sheet_name='TDSheet', converters={'Код (НСИ)': str, 'Потребность.Номер': str})
    xl_new.sort_values('Подобъект', inplace=True)
    # множество номеров потребностей в новом файле для импорта
    new_number_set = set(xl_new['Потребность.Номер'].tolist())


    f_prev_path = xl_directory + f'prev/{mvz}_prev.xlsx'
    if os.path.isfile(f_prev_path):
        xl_prev = pd.read_excel(f_prev_path, sheet_name='TDSheet', converters={'Код (НСИ)': str, 'Потребность.Номер': str})
        # формирование дата фрейма только вновь добавленных или изменных потребностей
        xl_data = pd.concat([xl_new, xl_prev]).drop_duplicates(keep=False)
        print(len(xl_data))
        xl_data.drop_duplicates('Потребность.Номер', inplace=True)

    else:
        xl_data = xl_new


    # количество записей в таблицах
    amount_new = len(xl_new.index)
    amount_unique = len(xl_data.index)

    # копия новой выгрузки в файл prev, чтобы в будущем сравнивать уже с ним
    shutil.copy2(f_path, f_prev_path)
    os.remove(f_path)
    os.remove(f_path.replace('ЗО', 'СП'))
    return xl_data, amount_new, amount_unique, new_number_set


def get_time():
    return f'{datetime.now().strftime("%Y-%m-%d_%H.%M.%S")}'


def integration():
    """главный процесс интеграции"""

    folders_dict = get_MTO_folders_dict()  # словарь с id папок в неосинтезе и со списком мвз по каждой папке
    print(folders_dict)
    counter = 0
    for folder in folders_dict:
        print(f'{get_time()}: Количество МВЗ по текущей папке {len(folders_dict[folder])}', file=log) # количество МВЗ по текущей папке
        for mvz in folders_dict[folder]:

            print(f'{get_time()}: Начат импорт МВЗ {mvz} в папку {folder}. ', end='', file=log)
            log.flush()

            try:

                xl_data, amount_new, amount_unique, new_number_set = get_xl_data(mvz)  # получить дата фрейм из файла эксель по нужным мвз
                print(f'Файл {mvz} найден. Строк в эксель всего {amount_new}, обновить {amount_unique}', file=log)

            except:
                print(f'Файл {mvz} не найден', file=log)
                continue

            counter += 1

            #  поиск или создание папки с нужным МВЗ
            folder_id = get_neosintez_id(folder_id=folder, item_name=mvz, class_id=mvz_folder_class_id)

            # опредедление сущностей для удаления
            number_set, del_double_id_tuple, del_id_tuple = get_number_set(folder_id, new_number_set)
            print(f'{get_time()}: Количество уникальных в Неосинтез {len(number_set)}, количество дублируемых потребностей {len(del_double_id_tuple)}, количество отсутствующих в новой выгрузке {len(del_id_tuple)}', file=log)
            log.flush()

            # удаление (перенос)
            del_set = set(del_id_tuple + del_double_id_tuple)
            print(del_set)
            for id in del_set:
                response = neosintez.delete_item(url, token, id, bin)
                if response.status_code != 200:
                    print(f'{get_time()}: Не удалось удалить сущность {id}', file=log)
            log.flush()

            #  интеграция записей (создание/ обновление) в Неосинтезе
            counter_success, counter_exception = import_excel_to_folder(folder_id, xl_data)

            #  количество сущностей по МВЗ в Неосинтезе в итоге
            amount_final = get_amount(folder_id)


            print(f'{get_time()}: {counter}. Успешно обновлено {counter_success} строк, ошибок {counter_exception}. В итоге потребностей {amount_final}', file=log)
            log.flush()

    print(f'{get_time()}: Обработано файлов {counter}', file=log)

# директория расположения файлов для имопрта в неосинтез
with open('xl_directory.txt', encoding='utf-8') as f:
    xl_directory = f.read()

atr_data = pd.read_excel('default_attributes.xlsx')  # дата фрейм для мэпинга атрибутов и колонок эксель файла

with open('auth_data.txt') as f:
    aut_string = f.read()


file_name = f'log/{get_time()}.txt'
log = open(file_name, 'w')

#  получение токена для авторизации в API неосинтеза
token = neosintez.authentification(url=url, aut_string=aut_string)
if not token:
    print('Ошибка аутентификации')

#  основной процесс
integration()

print(f'{get_time()}: длительность работы {datetime.now() - start_time}', file=log)

log.close()