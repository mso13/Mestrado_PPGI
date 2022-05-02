import os
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import os.path as op
import urllib3
urllib3.disable_warnings()


def find_between(s, first, last):
    try:
        start = s.index(first) + len(first)
        end = s.index(last, start)
        return s[start:end]
    except ValueError:
        return ""


def get_reports(cvm_code, year):
    url = f'https://bvmf.bmfbovespa.com.br/pt-br/mercados/acoes/empresas/ExecutaAcaoConsultaInfoRelevantes.asp?codCVM={cvm_code}&ano={year}'

    with requests.Session() as s:
        req = s.get(url,
                    allow_redirects=False,
                    verify=False)

        soup = BeautifulSoup(req.content.decode('utf-8'), 'html.parser')

        # Get all <div> tags with class 'large-12 columns'
        divs = [d for d in soup.find_all('div', {'class': 'large-12 columns'})]

        # Skip first 3 divs (Metadata)
        # Return last 10 divs
        return divs[2:13]


def get_data(ticker, cvm_code, year):
    results_list = list()
    divs = get_reports(cvm_code, year)

    for div in divs:
        try:
            results_dict = dict()
            all_info = list()

            results_dict['Ticker'] = ticker
            results_dict['CVM'] = cvm_code
            links = [link.text.strip() for link in div.find_all('p', {'class': 'primary-text'})]
            results_dict['Categoria'] = str(links[0])

            for table in div.find_all('table', {'class': 'ficha'}):
                for data in table.find_all('td'):
                    all_info.append(data.text.strip())

                for index, data in enumerate(all_info):
                    if index % 2 == 0:
                        if all_info[index] == '':
                            results_dict['Assunto Extra'] = all_info[index+1]
                        else:
                            results_dict[f'{all_info[index]}'] = all_info[index+1]

            results_list.append(results_dict)
        except:
            continue

    return results_list


def save_data(results_list, ticker):
    # Save the list of dicts
    with open(op.join(path_crawlers, f'{ticker}.json'), 'w', encoding='utf8') as f:
        json.dump(results_list, f, ensure_ascii=False)


def upload_qflib(ticker):
    # Load JSON for ticker
    json_file = open(op.join(path_crawlers, f'{ticker}.json'), encoding='utf8')
    json_obj = json.load(json_file)

    with new_conn:
        # Open new cursor
        with new_conn.cursor() as c:
            for item in json_obj:
                reference_date = str(item['Data Referência'][:10]) if 'Data Referência' in item.keys() else '01/01/1900'
                release_date = str(item['Data Entrega'][:10]) if 'Data Entrega' in item.keys() else '01/01/1900'
                ticker = str(item['Ticker'])
                headline = ((str(item['Assunto']) if 'Assunto' in item.keys() else 'Sem informação.') + ' ' + (str(item['Assunto Extra']) if 'Assunto Extra' in item.keys() else '')).strip()
                topic = ((str(item['Tipo']) if 'Tipo' in item.keys() else '') + ' - ' + (str(item['Categoria']) if 'Categoria' in item.keys() else '')).strip()

                if topic.startswith('- '):
                    topic = topic.replace('- ', '')

                c.execute(
                    """
                    INSERT INTO XXX (reference_date, release_date, headline, ticker, topic)
                    VALUES (%(reference_date)s, %(release_date)s, %(headline)s, %(ticker)s, %(topic)s)
                    ON CONFLICT (reference_date, ticker, headline, topic) DO NOTHING;
                     """,
                    {
                        "reference_date": datetime.strptime(reference_date, '%d/%m/%Y'),
                        "release_date": datetime.strptime(release_date, '%d/%m/%Y'),
                        "headline": headline,
                        "ticker": ticker,
                        "topic": topic,
                    },
                )
    new_conn.close()


def main(year):
    # 1. Get Ticker <--> CVM code info
    with open(op.join(path_crawlers, 'results-b3.json'), encoding='utf8') as json_file:
        data = json.load(json_file)

    df_b3 = pd.DataFrame(data)

    # 2. Save info into Dict
    ticker_code = dict()
    for index, row in df_b3.iterrows():
        if row['tickers']:
            for ticker in row['tickers']:
                ticker_code[ticker] = find_between(row['url_dados'], '?CodCVM=', '&AnoDoc=')

    # 3. Get news for all tickers
    for ticker, cvm_code in ticker_code.items():
        results_list = get_data(ticker, cvm_code, year)

        # Save the list of dicts
        save_data(results_list, ticker)

        # Upload to Qflib
        upload_qflib(ticker)

        # Delete JSON file with crawler results
        os.remove(op.join(path_crawlers, f'{ticker}.json'))


if __name__ == '__main__':
    currentYear = datetime.now().year
    main(year=currentYear)
