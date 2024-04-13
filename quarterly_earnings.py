import requests
import json
import pandas as pd


def pull_data():
    url = "https://ws.cso.ie/public/api.jsonrpc?data=%7B%22jsonrpc%22:%222.0%22," \
          "%22method%22:%22PxStat.Data.Cube_API.ReadDataset%22,%22params%22:%7B%22class%22:%22query%22," \
          "%22id%22:%5B%5D,%22dimension%22:%7B%7D,%22extension%22:%7B%22pivot%22:null,%22codes%22:false," \
          "%22language%22:%7B%22code%22:%22en%22%7D,%22format%22:%7B%22type%22:%22JSON-stat%22," \
          "%22version%22:%222.0%22%7D,%22matrix%22:%22EHQ15%22%7D,%22version%22:%222.0%22%7D%7D "

    response = requests.get(url)

    if response.status_code == 200:
        # File content is stored in response.text
        file_content = response.text
        json_data = json.loads(file_content)
        return json_data
    else:
        print("Failed to fetch the file:", response.status_code)
        return 0


def structured_data(data: str):
    dictionary_list = []

    # pulling earnings list
    temp_data = data['result']['dimension']['STATISTIC']['category']['label']
    earnings_category_list = []

    for keys, values in temp_data.items():
        earnings_category_list.append(values)

    # pulling quarter list
    temp_data = data['result']['dimension']['TLIST(Q1)']['category']['label']
    querters_list = []

    for keys, values in temp_data.items():
        querters_list.append(values)

    # pulling economic sector list
    temp_data = data['result']['dimension']['C02665V03225']['category']['label']
    sector_list = []

    for keys, values in temp_data.items():
        sector_list.append(values)

    # pulling value list
    value_list = data['result']['value']

    count = 0
    for each_earning in earnings_category_list:
        for each_quarter in querters_list:
            for each_sector in sector_list:
                temp_dict = {
                    "earning_category": each_earning,
                    "quarter": each_quarter,
                    "economic_sector": each_sector,
                    "value": value_list[count],
                }
                dictionary_list.append(temp_dict)
                count += 1

    return dictionary_list


if __name__ == '__main__':
    data = pull_data()
    result = structured_data(data)

    print(pd.DataFrame(result))
