import requests
import os
import pandas as pd
import json
from datetime import date, datetime, timedelta
import time
import random
import os.path
from os import path

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from urllib.parse import urlparse, parse_qs, urlencode, urlunparse


def find_requests(driver, company_selector=None, luzi_selector=None, data_selector=None):
    company_url = None
    luzi_url = None
    data_url = None

    for log_entry in driver.get_log('performance'):
        try:
            log_data = json.loads(log_entry['message'])
            request_url = log_data['message']['params']['request']['url']

            if company_selector and company_selector in request_url:
                company_url = request_url
            elif luzi_selector and luzi_selector in request_url:
                luzi_url = request_url
            elif data_selector and data_selector in request_url:
                data_url = request_url

            if company_url and luzi_url and data_url:
                break

        except (json.JSONDecodeError, KeyError, requests.exceptions.RequestException):
            pass

    return company_url, luzi_url, data_url


def setup_webdriver():
    caps = DesiredCapabilities.CHROME
    caps['goog:loggingPrefs'] = {'performance': 'ALL'}

    chrome_options = Options()
    chrome_options.add_argument('--enable-logging')
    chrome_options.add_argument('--v=1')

    wd = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options,
                          desired_capabilities=caps)

    return wd


def load_website(driver, url):
    driver.get(url)  # 打开要爬的网址
    driver.implicitly_wait(10)  # 每0.5秒进行一次操作 如果一直失败超过10秒则报错


def close_homepage_banner(driver):
    driver.find_element(By.XPATH, '//*[@id="gkClose"]').click()  # 关掉首页黄色通知


def open_dropdown_menu(driver):
    driver.find_element(By.XPATH, '//*[@id="psListShowBtn"]').click()  # 点开下拉菜单


def select_company(driver, company_name):
    driver.find_element(By.XPATH, f"//li[contains(., '{company_name}')]").click()  # 点击公司名


def create_replacement_dict(first_string, provided_dict):
    first_url = urlparse(first_string)
    first_query_params = parse_qs(first_url.query)
    replacement_dict = {}

    for key, value in provided_dict.items():
        if key in first_query_params:
            replacement_dict[key] = first_query_params[key][0]
        else:
            replacement_dict[key] = value

    return replacement_dict


def replace_query_params_with_dict(url_string, replacement_dict):
    # Parse URL and extract query parameters
    url = urlparse(url_string)
    query_params = parse_qs(url.query)

    # Replace query parameters in the URL with values from the dictionary
    for key in replacement_dict:
        query_params[key] = replacement_dict[key]

    # Construct the modified URL
    modified_query = urlencode(query_params, doseq=True)
    modified_url = urlunparse((url.scheme, url.netloc, url.path, url.params, modified_query, url.fragment))

    return modified_url


def main():
    file_path = './data/'
    start_date = date(2020, 1, 1)
    end_date = datetime.now().date()
    delta = timedelta(days=1)

    df_code = pd.read_csv(os.path.join(file_path, 'luzi_code.csv'))
    ps_code_list = df_code['ps_code'].tolist()

    url = 'https://ljgk.envsc.cn/'

    company_name = '温州龙湾伟明环保能源有限公司'
    select_company_url = 'GetPSList.ashx'
    select_luzi_url = 'GetBurnList.ashx'
    select_data_url = 'GetMonitorDataList.ashx'

    old_data_url = 'https://ljgk.envsc.cn/OutInterface/GetMonitorDataList.ashx?pscode' \
                   '=13C2D0DCE6FB5F5F1BFDA298A54CA80D&outputcode=13C2D0DCE6FB5F5FB2BE3E32478A0CC5&day=20230424' \
                   '&SystemType=C16A882D480E678F&sgn=5690ecf19458c834a84f47df1cb586a838a4a931&ts=1682445782405&tc' \
                   '=65124856'

    provided_dict = {
        'pscode': 'pscode',
        'outputcode': 'outputcode',
        'day': 'day',
        'SystemType': 'NewSystemType',
        'sgn': 'NewSgnValue',
        'ts': 'NewTsValue',
        'tc': 'NewTcValue'
    }

    wd = setup_webdriver()
    load_website(wd, url)
    close_homepage_banner(wd)
    open_dropdown_menu(wd)
    select_company(wd, company_name)

    company_url, _, _ = find_requests(wd, select_company_url, select_luzi_url, select_data_url)

    # 全公司信息
    company_html = requests.get(company_url)
    all_company = pd.json_normalize(company_html.json())

    current_date = start_date
    while current_date < end_date:
        current_date_str = current_date.strftime('%Y%m%d')

        company_folder = os.path.join(file_path, company_name)
        os.makedirs(company_folder, exist_ok=True)
        csv_file = os.path.join(company_folder, f"{current_date_str}.csv")

        for ps in ps_code_list:
            if not path.exists(csv_file):
                mp_code_list = df_code[df_code['ps_code'] == ps]['mp_code'].unique()
                df_data = pd.DataFrame()
                for mp in mp_code_list:
                    provided_dict['pscode'] = ps
                    provided_dict['outputcode'] = mp
                    provided_dict['day'] = current_date_str
                    # 获取公司名称
                    company_name = all_company[all_company['ps_code'] == ps]['ps_name'].tolist()[0]

                    replacement_dict = create_replacement_dict(company_url, provided_dict)
                    real_data_url = replace_query_params_with_dict(old_data_url, replacement_dict)
                    try:
                        # 开始爬取数据
                        temp_data = requests.get(real_data_url).json()

                        for i in range(len(temp_data)):
                            test = pd.json_normalize(temp_data[i])
                            df_data = pd.concat([df_data, test]).reset_index(drop=True)

                        # Save df_data to a CSV file in a folder named with company_name
                        if not df_data.empty:
                            df_data.to_csv(csv_file, index=False, encoding='utf_8_sig')
                            time.sleep(random.uniform(2, 5))
                        break
                    except Exception as e:
                        print(e)
                        return

        # Sleep for a random duration between 30-50 seconds at the beginning of each year
        if current_date.month == 1 and current_date.day == 1 and current_date != start_date:
            time.sleep(random.uniform(30, 50))

        current_date += delta


if __name__ == '__main__':
    main()
