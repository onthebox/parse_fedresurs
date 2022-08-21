import time
from datetime import datetime, date, timedelta
from collections import defaultdict
import logging
import sys

import requests
import pandas as pd


def get_company_guid(code: str) -> str:
    """
    Get company guid code from server.

    Arguments:
        code (string) -- Uniaue company identifier (ИНН)
    Returns:
        guid (string)
    """

    url = 'https://fedresurs.ru/backend/companies'

    querystring = {"limit":"15","offset":"0","code":code,"isActive":"true"}

    headers = {
        "Referer": f'https://fedresurs.ru/search/entity?code={code}'
    }

    try:
        #res = requests.request("GET", url, headers=headers, params=querystring).json()
        response = requests.request("GET", url, headers=headers, params=querystring)
        res = response.json()
    except requests.exceptions.ConnectionError:
        logging.warning('Не удается подключиться. Проверьте подключение к сети!')
        sys.exit()
    except requests.exceptions.Timeout:
        logging.warning('Превышено время ожидания от сервера!')
        sys.exit()

    try:
        return res['pageData'][0]['guid']
    except (KeyError, IndexError) as no_company:
        return None


def get_messages_guid(company_guid: date, from_date: date, to_date: list) -> list:
    """
    Get guid of all the messages within the given publication date range.

    Arguments:
        company_guid (string) -- company guid
        from_date (date) -- date object, the first day of time interval
        to_date (date) -- date object, the last date of time interval
    Returns:
        all_msgs (list) -- list of strings
    """

    def date_range(start_date: date, end_date: date) -> str:
        """
        Yield every date from given start to end dates.

        Arguments:
            start_date (date) -- date obj
            end_date (date) -- date obj
        Yield:
            date as a string
        """

        for day in range(int((end_date - start_date).days)):
            yield (start_date + timedelta(day)).strftime('%Y-%m-%dT%H:%M:%S.000Z')

    url = f'https://fedresurs.ru/backend/companies/{company_guid}/publications'
    all_msgs = []

    for day in date_range(from_date, to_date):
        
        for load in range(0, 525, 15):
            time.sleep(1)
            querystring = {"limit": "15",
                "offset": str(load),
                "startDate": day,
                "endDate": day,
                "searchCompanyEfrsb": "false",
                "searchAmReport": "false",
                "searchFirmBankruptMessage": "false",
                "searchFirmBankruptMessageWithoutLegalCase": "false",
                "searchSfactsMessage": "true",
                "searchSroAmMessage": "false",
                "searchTradeOrgMessage": "false"
            }

            headers = {
                "Referer": f'https://fedresurs.ru/company/{company_guid}'
            }
            
            try:
                #res = requests.request("GET", url, headers=headers, params=querystring).json()
                response = requests.request("GET", url, headers=headers, params=querystring)
                res = response.json()
            except requests.exceptions.ConnectionError:
                logging.warning('Не удается подключиться. Проверьте подключение к сети!')
                sys.exit()
            except requests.exceptions.Timeout:
                logging.warning('Превышено время ожидания от сервера!')
                sys.exit()
            
            for msg in res['pageData']:
                if msg['title'] == 'Заключение договора финансовой аренды (лизинга)':
                    all_msgs.append(msg['guid'])
                else:
                    continue

            if len(res['pageData']) < 15:
                break

    return all_msgs


def get_message_data(message_guid: str) -> dict:
    """
    Get mesage data from json. 

    Arguments:
        message_guid (string) -- message guid
    Returns:
        message_data (dict) -- dict with message data
    """

    message_data = dict.fromkeys([
        "Дата",
        "ИНН",
        "ОГРН",
        "Договор",
        "Срок финансовой аренды",
        "Лизингодатель",
        "Лизингополучатель",
        "ИНН лизингополучателя",
        "ОГРН лизингополучателя",
        "Идентификатор",
        "Классификация",
        "Описание"
    ])

    url = f'https://fedresurs.ru/backend/sfactmessages/{message_guid}'

    headers = {
        "Referer": f'https://fedresurs.ru/sfactmessage/{message_guid}'
    }

    try:
        #res = requests.request("GET", url, headers=headers).json()
        response = requests.request("GET", url, headers=headers)
        res = response.json()
    except requests.exceptions.ConnectionError:
        logging.warning('Не удается подключиться. Проверьте подключение к сети!')
        sys.exit()
    except requests.exceptions.Timeout:
        logging.warning('Превышено время ожидания от сервера!')
        sys.exit()

    if 'lockReason' in list(res.keys()):
        message_data['Дата'] = res['annulmentMessageInfo']['datePublish'].split('T')[0]
        message_data['ИНН'] = res['publisher']['inn']
        message_data['ОГРН'] = res['publisher']['ogrn']
        message_data['Договор'] = f'Сообщение заблокировано! Ссылка: {url}'

        return message_data

    content = res['content']
    
    try:
        lessee = [key for key in content.keys() if key.startswith('lessees') and len(content[key]) > 0][0]

        message_data['Дата'] = res['datePublish'].split('T')[0]
        message_data['ИНН'] = content['lessorsCompanies'][0]['inn']
        message_data['ОГРН'] = content['lessorsCompanies'][0]['ogrn']
        message_data['Договор'] = content['contractNumber'] + ' от ' + content['contractDate'].split('T')[0]
        message_data['Срок финансовой аренды'] = content['startDate'].split('T')[0] + ' - ' + content['endDate'].split('T')[0]
        message_data['Лизингодатель'] = content['lessorsCompanies'][0]['fullName']
        if lessee == 'lesseesCompanies':
            message_data['Лизингополучатель'] = content[lessee][0]['fullName']
            message_data['ИНН лизингополучателя'] = content[lessee][0]['inn']
            message_data['ОГРН лизингополучателя'] = content[lessee][0]['ogrn']
        else:
            message_data['Лизингополучатель'] = content[lessee][0]['fio']
            message_data['ИНН лизингополучателя'] = content[lessee][0]['inn']
            message_data['ОГРН лизингополучателя'] = content[lessee][0].get('ogrnip', 'Не указано')
        message_data['Идентификатор'] = content['subjects'][0].get('subjectId', 'Не указано')
        message_data['Классификация'] = content['subjects'][0].get('classifierCode', 'Не указано') + ', ' + content['subjects'][0].get('classifierName', 'Не указано')
        message_data['Описание'] = content['subjects'][0]['description']
    except:
        logging.warning(f'Неизвестная структура сообщения №{res["number"]}')

    return message_data


def main() -> None:
    """
    Start the main process of scraping data from fedresurs.ru.
    """
    
    try:
        with open('INN to parse.txt', 'r') as f:
            codes = f.read().split('\n')
    except FileNotFoundError:
        logging.warning('Не удается открыть файл с информацией об ИНН.')
        sys.exit()

    while True:
        try:
            from_date = list(map(int, input('Введите дату начала в формате 2022,1,1: \n').replace(' ', '').split(',')))
            to_date = list(map(int, input('Введите дату конца в формате 2022,2,1: \n').replace(' ', '').split(',')))
            from_date = date(*from_date)
            to_date = date(*to_date) + timedelta(1)
            if from_date <= to_date:
                break
            else:
                raise ValueError('Дата начала больше даты конца!')
        except (ValueError, NameError) as e:
            logging.warning('Неправильно введена дата, либо дата начала больше даты конца! Поробуйте еще раз.')

    all_data = defaultdict(list)

    for code in codes:
        logging.info(f'Обрабатываю сообщения компании с ИНН {code} ' + 
            f'за период с {from_date.strftime("%Y-%m-%d")} по {to_date.strftime("%Y-%m-%d")}. Это займет некоторое время...')
        company_guid = get_company_guid(code)
        if company_guid == None:
            logging.info(f'Не найдено компаний с таким ИНН: {code}')
            continue
        else:
            messages_guid = get_messages_guid(company_guid, from_date, to_date)
            logging.info(f'Найдено сообщений: {len(messages_guid)}')
        for guid in messages_guid:
            time.sleep(1)
            message_data = get_message_data(guid)
            for k, v in message_data.items():
                all_data[k].append(v)

    result = pd.DataFrame.from_dict(all_data)
    result.to_excel(f'{datetime.now().strftime("%d-%m-%Y_%H-%M-%S")}.xlsx')
    logging.info('Готово!')



if __name__ == '__main__':
    
    logging.basicConfig(format='[%(asctime)s][%(levelname)7s] %(message)s', level=logging.INFO, stream=sys.stdout)
    main()