import pandas as pd
import requests
from time import sleep
import numpy as np
from numpy import nan

import json
import pandas as pd

def get_transactions_to(api_key, eoa_address):
    print("Fetching transactions to " + eoa_address + "...")
    page_key = None
    process_active = True
    res_df = pd.DataFrame(columns=[
        "blockNum", "hash", "from", "to", "value", "erc721TokenId", "tokenId", "asset", "category"
    ])

    while process_active:
        headers = {
            "Accept": "application/json",
        }

        params = {
            'jsonrpc': '2.0',
            'id': 0,
            'method': 'alchemy_getAssetTransfers',
            'params':[{
                'fromBlock': '0x0',
                'toAddress': eoa_address,
                'excludeZeroValue': False,
                'category': [
                    "external",
                    "erc1155",
                    "erc721",
                    "erc20"
                ]
            }]
        }
        if not page_key:
            alchemy_url = 'https://eth-mainnet.alchemyapi.io/v2/{api_key}'.format(
                api_key=api_key
            )
        else:
            alchemy_url = 'https://eth-mainnet.alchemyapi.io/v2/{api_key}'.format(
                api_key=api_key
            )
            params['params'][0]['pageKey'] = page_key

        retries = 3
        for i in range(retries):
            try:
                r = requests.post(alchemy_url, headers=headers, data=json.dumps(params))
                j = r.json()
                if 'error' in j:
                    process_active = False

                raw_data = j['result']['transfers']
                txn_df = pd.DataFrame(raw_data)
                res_df = pd.concat([res_df, txn_df], ignore_index=True)

                try:
                    page_key = j["result"]["pageKey"]
                except:
                    process_active = False

            except KeyError as e:
                if i < retries - 1:
                    print("Alchemy request failed. Retrying request...")
                    sleep(5)
                    continue
                else:
                    raise
            break
    return res_df

def get_transactions_from(api_key, eoa_address):
    print("Fetching transactions from " + eoa_address + "...")
    page_key = None
    process_active = True
    res_df = pd.DataFrame(columns=[
        "blockNum", "hash", "from", "to", "value", "erc721TokenId", "tokenId", "asset", "category"
    ])

    while process_active:
        headers = {
            "Accept": "application/json",
        }

        params = {
            'jsonrpc': '2.0',
            'id': 0,
            'method': 'alchemy_getAssetTransfers',
            'params':[{
                'fromBlock': '0x0',
                'fromAddress': eoa_address,
                'excludeZeroValue': False,
                'category': [
                    "external",
                    "erc1155",
                    "erc721",
                    "erc20"
                ]
            }]
        }
        if not page_key:
            alchemy_url = 'https://eth-mainnet.alchemyapi.io/v2/{api_key}'.format(
                api_key=api_key
            )
        else:
            alchemy_url = 'https://eth-mainnet.alchemyapi.io/v2/{api_key}'.format(
                api_key=api_key
            )
            params['params'][0]['pageKey'] = page_key

        retries = 3
        for i in range(retries):
            try:
                r = requests.post(alchemy_url, headers=headers, data=json.dumps(params))
                j = r.json()
                if 'error' in j:
                    process_active = False

                raw_data = j['result']['transfers']
                txn_df = pd.DataFrame(raw_data)
                res_df = pd.concat([res_df, txn_df], ignore_index=True)

                try:
                    page_key = j["result"]["pageKey"]
                except:
                    process_active = False

            except KeyError as e:
                if i < retries - 1:
                    print("Alchemy request failed. Retrying request...")
                    sleep(5)
                    continue
                else:
                    raise
            break
    return res_df

def get_transactions(api_key, eoa_address, output):
  df1 = get_transactions_to(api_key, eoa_address)
  df2 = get_transactions_from(api_key, eoa_address)
  final_df = pd.concat([df1, df2], ignore_index=True)

  # Clean the data frame
  # Rename to a more conventional name in this project
  final_df.rename(columns={
    'hash': 'transaction_hash',
    'blockNum': 'block_number',
    'value': 'value_eth'
    },
    inplace=True
  )

  final_df['block_number'] = final_df['block_number'].apply(lambda x: int(x, 16))
  final_df['token_id'] = final_df['tokenId'].apply(lambda x: int(x, 16) if x else None)

  final_df.sort_values(by=['block_number'], inplace=True)
  final_df.to_csv(output, index=False)
  print('Merged final file')

def generate_transactions_output(date_block_mapping_file,eth_prices_file, transactions_file, output):
  date_blocks_df = pd.read_csv(date_block_mapping_file)
  eth_prices_df = pd.read_csv(eth_prices_file)
  txn_df = pd.read_csv(transactions_file)

  # Transpose data from date block mapping file
  date_blocks_df.index = pd.IntervalIndex.from_arrays(
      date_blocks_df["starting_block"], date_blocks_df["ending_block"], closed="both"
  )

  # get the last block that is supported by this program
  last_block = date_blocks_df.iloc[-1]["ending_block"]

  # Drop any transactions that have happened after the last block date
  txn_df = txn_df.loc[txn_df["block_number"] <= last_block]

  txn_df["date"] = txn_df["block_number"].apply(
    lambda x: date_blocks_df.iloc[date_blocks_df.index.get_loc(x)]["date"]
  )

  txn_df = txn_df.merge(eth_prices_df, on="date", how="left")

  txn_df["value_usd"] = txn_df["value_eth"] * txn_df["price_of_eth"]

  df1 = txn_df['rawContract'].apply(eval).apply(pd.Series).rename(columns={
    'value': 'rawContract_value',
    'address': 'contract',
    'decimal': 'rawContract_decimal'
  })

  df1['rawContract_value'] = df1['rawContract_value'].apply(lambda x: int(x, 16) if x else 0) \
    / df1['rawContract_decimal'].apply(lambda x: int(x, 16) if x else 0).apply(lambda x: pow(10, x))

  txn_df = txn_df.join(df1)

  df2 =txn_df['erc1155Metadata']
  df2 = df2.apply(lambda x: eval(x)[0] if isinstance(x, str) else np.nan)
  df2 = df2.apply(pd.Series)

  df2['tokenId'] = df2['tokenId'].apply(lambda x: int(x, 16) if isinstance(x, str) else None)
  df2['value'] = df2['value'].apply(lambda x: int(x, 16) if isinstance(x, str) else 0)

  df2 = df2.rename(columns={
    'tokenId': '1155token_id',
    'value': 'value_1155',
  })
  txn_df = pd.concat([txn_df, df2], axis=1)

  df3 = txn_df[['1155token_id', 'token_id']]\
    .apply(lambda x: x[0] if not np.isnan(x[0]) else x[1], axis=1)

  txn_df['token_id'] = df3

  txn_df = txn_df.drop(
    ['rawContract', 'rawContract_decimal', 'erc1155Metadata', 0, '1155token_id']
    , axis=1)

  txn_df.to_csv(output, index=False)
  print('Created final output file')
