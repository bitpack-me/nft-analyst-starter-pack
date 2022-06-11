import sys

# Check for Python version <= 3.10
if sys.version_info[0] == 3 and sys.version_info[1] >= 10:
    raise Exception("Python >=3.10 is not supported at this time.")

import asyncio
import os
import sys
import tempfile
import warnings
import json
from datetime import datetime, timedelta
from pathlib import Path

import click
import ethereumetl
import numpy as np
import pandas as pd
from ethereumetl.service.eth_service import EthService
from web3 import Web3

from core.generate_metadata_output import generate_metadata_output
from core.generate_sales_output import generate_sales_output
from core.generate_transfers_output import generate_transfers_output
from jobs.export_logs import export_logs
from jobs.export_update_logs import export_update_logs
from jobs.get_recent_block import get_recent_block
from jobs.export_token_transfers import export_token_transfers
from jobs.export_1155_transfers import export_1155_transfers
from jobs.get_nft_metadata import get_metadata_for_collection
from jobs.update_block_to_date_mapping import update_block_to_date_mapping
from jobs.update_eth_prices import update_eth_prices
from jobs.cleanup_outputs import clean_up_outputs
from utils.check_contract_support import check_contract_support
from utils.extract_unique_column_value import extract_unique_column_value
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
    "-c",
    "--contract-address",
    required=True,
    type=str,
    help="The contract address of the desired NFT collection.",
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
def export_data(contract_address,
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

    # Convert address to checksummed address (a specific pattern of uppercase and lowercase letters)
    contract_address = Web3.toChecksumAddress(contract_address)

    # Check if contract address is supported by Alchemy
    check_contract_support(
        alchemy_api_key=alchemy_api_key, contract_address=contract_address
    )

    warnings.simplefilter(action="ignore", category=FutureWarning)
    print("Process started for contract address: " + str(contract_address))

    # Get current timestamp
    right_now = str(datetime.now().timestamp())

    # Assign file paths (persisting files only)
    date_block_mapping_csv = "./raw-data/date_block_mapping.csv"
    eth_prices_csv = "./raw-data/eth_prices.csv"
    sales_csv = "./outputs/" + contract_address + "/sales_" + contract_address + "_" + right_now + ".csv"
    metadata_csv = "./outputs/"+ contract_address +"/metadata_" + contract_address + ".csv"
    transfers_csv = "./outputs/"+ contract_address +"/transfers_" + contract_address + "_" + right_now + ".csv"
    updates_csv = "./update-logs/" + contract_address + ".csv"

    # Set provider
    provider_uri = "https://eth-mainnet.alchemyapi.io/v2/" + alchemy_api_key
    web3 = Web3(Web3.HTTPProvider(provider_uri))
    eth_service = EthService(web3)
    ethereum_etl_batch_size = 1000
    ethereum_etl_max_workers = 8

    # Get block range
    # If update logs exist, read from the saved file and set the start block
    start_block = get_recent_block(updates_csv, contract_address, web3)

    yesterday = datetime.today() - timedelta(days=1)
    _, end_block = eth_service.get_block_range_for_date(yesterday)

    # If start_block == end_block, then data is already up to date
    if start_block == end_block:
        print("Data is up to date. No updates required.")
        sys.exit(0)

    # create an appropriate dir to store the files at a high level
    curr_dir = Path(os.path.dirname(os.path.realpath(__file__)))
    op_dir = curr_dir.joinpath(curr_dir, "outputs")
    op_dir.mkdir(parents=True, exist_ok=True)
    dir = op_dir.joinpath(contract_address)
    dir.mkdir(parents=True, exist_ok=True)

    # Create tempfiles
    with tempfile.NamedTemporaryFile(
        delete=False
    ) as logs_csv, tempfile.NamedTemporaryFile(
        delete=False
    ) as transaction_hashes_txt, tempfile.NamedTemporaryFile(
        delete=False
    ) as token_ids_txt, tempfile.NamedTemporaryFile(
        delete=False
    ) as raw_attributes_csv:

        # Export token transfers
        export_token_transfers(
            start_block=start_block,
            end_block=end_block,
            batch_size=ethereum_etl_batch_size,
            provider_uri=provider_uri,
            max_workers=ethereum_etl_max_workers,
            tokens=contract_address,
            output=transfers_csv,
        )

        # If there are no 721 transfers, export 1155 transfers
        if os.stat(transfers_csv).st_size == 0:
            print(
                "No ERC-721 transfers were identified.",
                "Therefore, searching for and extracting any ERC-1155 transfers.",
            )
            export_1155_transfers(
                start_block=start_block,
                end_block=end_block,
                batch_size=ethereum_etl_batch_size,
                provider_uri=provider_uri,
                max_workers=ethereum_etl_max_workers,
                tokens=contract_address,
                output=transfers_csv,
            )

        # Create staging files
        extract_unique_column_value(
            input_filename=transfers_csv,
            output_filename=transaction_hashes_txt.name,
            column="transaction_hash",
        )

        extract_unique_column_value(
            input_filename=transfers_csv,
            output_filename=token_ids_txt.name,
            column="value",
        )

        # Export logs
        export_logs(
            start_block=start_block,
            end_block=end_block,
            batch_size=ethereum_etl_batch_size,
            provider_uri=provider_uri,
            max_workers=ethereum_etl_max_workers,
            tx_hashes_filename=transaction_hashes_txt.name,
            output=logs_csv.name,
        )

        # Update date block mapping
        update_block_to_date_mapping(
            filename=date_block_mapping_csv, eth_service=eth_service
        )

        # Update ETH prices
        update_eth_prices(filename=eth_prices_csv)

        # Generate sales output
        generate_sales_output(
            transfers_file=transfers_csv,
            logs_file=logs_csv.name,
            date_block_mapping_file=date_block_mapping_csv,
            eth_prices_file=eth_prices_csv,
            output=sales_csv,
        )

        # Generate transfers output
        generate_transfers_output(
            transfers_file=transfers_csv,
            date_block_mapping_file=date_block_mapping_csv,
            output=transfers_csv,
        )

        # Consolidate sales and transfers data into final outputs
        # Perform only for this contract
        op_transfer_csv, op_sales_csv = clean_up_outputs(dir)[0]

        # Fetch metadata
        get_metadata_for_collection(
            api_key=alchemy_api_key,
            contract_address=contract_address,
            output=raw_attributes_csv.name,
        )

        # Generate metadata output
        generate_metadata_output(
            raw_attributes_file=raw_attributes_csv.name,
            token_ids_file=token_ids_txt.name,
            output=metadata_csv,
        )

        # Export to update log file
        export_update_logs(
            update_log_file=updates_csv,
            current_block_number=end_block,
        )

        # Move files to appropriate locations
        aws_upload([
            op_transfer_csv,
            op_sales_csv,
            metadata_csv
          ],
          aws_access_key_id=aws_access_key_id,
          aws_secret_access_key=aws_secret_key,
          bucket=aws_s3_bucket,
          region_name=aws_region
        )

        print(json.dumps({
          "success": True
        }))


if __name__ == "__main__":
    export_data()
