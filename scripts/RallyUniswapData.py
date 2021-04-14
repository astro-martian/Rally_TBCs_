import os
import requests, json, csv
import pandas as pd
from datetime import datetime

# ssh_path = f"{os.getenv('HOME')}/scripts/AaveScripts/userDataExtraction@4HrGMT/"
uniswapV2SubgraphURL = 'https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2'
# ethereumBlocksSubgraphURL = 'https://api.thegraph.com/subgraphs/name/blocklytics/ethereum-blocks'

blockNumbers = []
timestamps = []
pairRows = []


def _main():
    return populatePairData()


def populatePairData():
    return deployScript()


def deployScript():
    # loadBlockNumbersList()
    getData('0xf1f955016ecbcd7321c7266bccfb96c68ea5e49b', 18551, 18695)
    writePairData()


def getData(address, start, end):
    query = """ query($ID: String) {
      tokenDayData(id: $ID) {
        id
        date
        token {
          symbol
        }
        dailyVolumeToken
        dailyVolumeETH
        dailyVolumeUSD
        dailyTxns
        totalLiquidityToken
        totalLiquidityETH
        totalLiquidityUSD
        priceUSD
        maxStored
        }
    }"""

    for i in range(start, end):
        variables = {'ID': address + '-' + str(i)}
        r = requests.post(uniswapV2SubgraphURL, json={'query': query, 'variables': variables})
        j_xt = json.loads(r.text)
        day_data = json.loads(r.text)['data']['tokenDayData']
        # print(pair_data)
        pairRows.append(day_data)
        print('Data fetched for day ' + str(i))


# GETTING BLOCK NUMBERS BASED ON TIMESTAMPS
# GETTING BLOCK NUMBERS BASED ON TIMESTAMPS
# GETTING BLOCK NUMBERS BASED ON TIMESTAMPS
# def loadBlockNumbersList():
#     df = pd.read_csv('../reflexor_rai.csv')
#     rows = df.apply(lambda x: x.tolist(), axis=1)
#     # print(rows)
#     for row in rows:
#         blockNumbers.append(int(row[0]))
#         timestamps.append(int(row[1]))
#     return


# WRITES DATA FOR A DAY TO THE FILE
def writePairData():
    print('\n \n')
    path = './rally_uniswap_daily.csv'
    with open(path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['date', 'date (GMT)', 'dailyVolumeToken', 'dailyVolumeETH', 'dailyVolumeUSD', 'dailyTxns',
                         'totalLiquidityToken',
                         'totalLiquidityETH', 'totalLiquidityUSD', 'priceUSD', 'maxStored'])
    for row in pairRows:
        with open(path, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([row['date'], datetime.utcfromtimestamp(int(row['date'])).strftime('%Y-%m-%d %H:%M:%S'),
                             row['dailyVolumeToken'], row['dailyVolumeETH'], row['dailyVolumeUSD'], row['dailyTxns'],
                             row['totalLiquidityToken'], row['totalLiquidityETH'], row['totalLiquidityUSD'],
                             row['priceUSD'], row['maxStored']])


if __name__ == "__main__":
    # execute only if run as a script
    _main()
