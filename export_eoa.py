import sys

# Check for Python version <= 3.10
if sys.version_info[0] == 3 and sys.version_info[1] >= 10:
    raise Exception("Python >=3.10 is not supported at this time.")

import click
from pathlib import Path
import json
import os
from web3 import Web3

from ethereumetl.service.eth_service import EthService
from jobs.update_eth_prices import update_eth_prices
from core.generate_eoa_txn_output import generate_transactions_output, get_transactions
from jobs.update_block_to_date_mapping import update_block_to_date_mapping
from utils.aws_upload import aws_upload

# Set click CLI parameters
@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option(
    "-a",
    "--alchemy-api-key",
    required=True,
    type=str,
    help="The Alchemy API key to use for data extraction.",
)
@click.option(
    "-e",
    "--eoa-address",
    required=True,
    type=str,
    help="The address of the EOA.",
)
@click.option(
  "-x",
  "--aws-access-key-id",
  required=True,
  type=str,
  help="The AWS access key to use for S3 uploads.",
)
@click.option(
  "-y",
  "--aws-secret-key",
  required=True,
  type=str,
  help="The AWS secret access key to use for S3 uploads.",
)
@click.option(
  "-b",
  "--aws-s3-bucket",
  required=True,
  type=str,
  help="The S3 bucket to use for S3 uploads.",
)
@click.option(
  "-r",
  "--aws-region",
  required=True,
  type=str,
  help="The AWS region to use for S3 uploads.",
)
def export_eoa(eoa_address,
  alchemy_api_key,
  aws_access_key_id,
  aws_secret_key,
  aws_s3_bucket,
  aws_region):

    if (alchemy_api_key is None) or (alchemy_api_key == ""):
        raise Exception("Alchemy API key is required.")

    if (aws_access_key_id is None) or (aws_access_key_id == ""):
        raise Exception("AWS access key is required.")

    if (aws_secret_key is None) or (aws_secret_key == ""):
        raise Exception("AWS secret key is required.")

    if (aws_s3_bucket is None) or (aws_s3_bucket == ""):
        raise Exception("AWS S3 bucket is required.")

    if (aws_region is None) or (aws_region == ""):
        raise Exception("AWS region is required.")

    contract_address = Web3.toChecksumAddress(eoa_address)

    txn_csv = "./outputs/"+ contract_address +"/eoa_txn_" + contract_address + ".csv"
    txn_op_csv = "./outputs/"+ contract_address +"/eoa_txn_op_" + contract_address + ".csv"

    # create an appropriate dir to store the files at a high level
    curr_dir = Path(os.path.dirname(os.path.realpath(__file__)))
    op_dir = curr_dir.joinpath(curr_dir, "outputs")
    op_dir.mkdir(parents=True, exist_ok=True)
    dir = op_dir.joinpath(contract_address)
    dir.mkdir(parents=True, exist_ok=True)

    provider_uri = "https://eth-mainnet.alchemyapi.io/v2/" + alchemy_api_key
    web3 = Web3(Web3.HTTPProvider(provider_uri))
    eth_service = EthService(web3)

    date_block_mapping_csv = "./raw-data/date_block_mapping.csv"
    update_block_to_date_mapping(
        filename=date_block_mapping_csv, eth_service=eth_service
    )
    # Update ETH Prices
    eth_prices_csv = "./raw-data/eth_prices.csv"
    update_eth_prices(filename=eth_prices_csv)

    get_transactions(api_key=alchemy_api_key,
      eoa_address=contract_address,
      output=txn_csv)

    generate_transactions_output(
      date_block_mapping_file=date_block_mapping_csv,
      eth_prices_file=eth_prices_csv,
      transactions_file=txn_csv,
      output=txn_op_csv
    )

    response_aws = aws_upload([txn_op_csv],
          aws_access_key_id=aws_access_key_id,
          aws_secret_access_key=aws_secret_key,
          bucket=aws_s3_bucket,
          region_name=aws_region
        )
    print(json.dumps({
          "success": True,
          "aws": response_aws
        }))


if __name__ == "__main__":
    export_eoa()