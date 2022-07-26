import requests
import concurrent.futures
import time
from typing import Dict
from typing import List
from dataclasses import dataclass
from dataclasses import fields
from requests.adapters import HTTPAdapter
from requests.adapters import Retry
import json
import traceback
import pandas as pd

PROCESS_COUNT = 32
TOP_K_COLLECTIONS = 10000  # Max 10000
TOP_COLLECTIONS_PATH = "top_collections.csv"
STATS_OUTPUT_FILE_PATH = "stats.md"

NFTPORT_API_KEY = "API-KEY"
ALCHEMY_API_KEY = "API-KEY"
ALCHEMY_BASE_URL = f"https://eth-mainnet.alchemyapi.io/nft/v2/{ALCHEMY_API_KEY}"
MORALIS_API_KEY = "API-KEY"
MORALIS_BASE_URL = "https://deep-index.moralis.io/api/v2/nft"
QUICKNODE_URL = "YOUR-QUICKNODE-URL"

MAX_PAGES_PER_CONTRACT = 1000000  # Safe limit especially for transactions which can have lots of pages

PROVIDERS = ["nftport", "alchemy", "moralis", "quicknode"]


@dataclass(frozen=False)
class ContractStats:
    num_nfts: int = 0
    num_has_metadata: int = 0
    num_has_cached_image: int = 0
    num_sale_transactions: int = 0
    has_floor_price: bool = False


@dataclass(frozen=True)
class CompareContractStats:
    address: str
    slug: str = None
    token_supply: int = None
    nftport: ContractStats = None
    alchemy: ContractStats = None
    moralis: ContractStats = None
    quicknode: ContractStats = None


@dataclass(frozen=False)
class GlobalContractsStats:
    total_num_nfts: int = 0
    total_num_has_metadata: int = 0
    total_num_has_cached_image: int = 0
    total_num_sale_transactions: int = 0
    total_collections_floor_price_found: int = 0


@dataclass(frozen=False)
class GlobalCompareContractStats:
    total_num_collections: int = 0
    total_token_supply: int = 0
    nftport: GlobalContractsStats = GlobalContractsStats()
    alchemy: GlobalContractsStats = GlobalContractsStats()
    moralis: GlobalContractsStats = GlobalContractsStats()
    quicknode: GlobalContractsStats = GlobalContractsStats()


def build_http_client(
        backoff_factor: int = 200,
        total_retries: int = 5
):
    retry_strategy = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    return http


def _execute_nftport_request(contract_address: str, page_number: int):
    url = f"https://api.nftport.xyz/v0/nfts/{contract_address}"
    querystring = {
        "chain": "ethereum",
        "page_number": page_number,
        "include": "metadata"
    }
    headers = {
        'Content-Type': "application/json",
        'Authorization': NFTPORT_API_KEY
    }
    client = build_http_client()
    response = client.get(
        url, headers=headers, params=querystring)
    return response.json()


def _execute_nftport_transactions(contract_address: str, continuation: str):
    url = f"https://api.nftport.xyz/v0/transactions/nfts/{contract_address}"
    querystring = {
        "chain": "ethereum",
        "type": "sale"
    }
    if continuation:
        querystring["continuation"] = continuation
    headers = {
        'Content-Type': "application/json",
        'Authorization': NFTPORT_API_KEY
    }
    client = build_http_client()
    response = client.get(
        url, headers=headers, params=querystring)
    return response.json()


def _get_nftport_floor_price(contract_address: str):
    url = f"https://api.nftport.xyz/v0/transactions/stats/{contract_address}"
    querystring = {
        "chain": "ethereum"
    }
    headers = {
        'Content-Type': "application/json",
        'Authorization': NFTPORT_API_KEY
    }
    client = build_http_client()
    response = client.get(
        url, headers=headers, params=querystring)
    return response.json().get("statistics", {}).get("floor_price")


def _execute_alechmey_request(contract_address: str, start_token: str):
    url = f"{ALCHEMY_BASE_URL}/getNFTsForCollection"
    querystring = {
        "contractAddress": contract_address,
        "startToken": start_token,
        "withMetadata": True
    }
    headers = {
        'Content-Type': "application/json"
    }
    client = build_http_client()
    response = client.get(
        url, headers=headers, params=querystring)
    return response.json()


def _execute_alchemy_transactions(contract_address: str, page_key: str):
    url = f"https://eth-mainnet.alchemyapi.io/v2/{ALCHEMY_API_KEY}"
    data = {
        "jsonrpc": "2.0",
        "method": "alchemy_getAssetTransfers",
        "params": [
            {
                "contractAddresses": [contract_address],
                "category": ["erc721", "erc1155"]
            }
        ]
    }
    if page_key:
        data["params"][0]["pageKey"] = page_key
    headers = {
        'Content-Type': "application/json"
    }
    client = build_http_client()
    response = client.post(
        url, headers=headers, data=json.dumps(data))
    return response.json()


def _get_token_supply(contract_address: str):
    url = f"{ALCHEMY_BASE_URL}/getContractMetadata"
    querystring = {
        "contractAddress": contract_address
    }
    headers = {
        'Content-Type': "application/json"
    }
    client = build_http_client()
    response = client.get(
        url, headers=headers, params=querystring).json()
    supply = response.get("contractMetadata", {}).get("totalSupply")
    if supply:
        return int(supply)


def _get_alchemy_floor_price(contract_address: str):
    url = f"{ALCHEMY_BASE_URL}/getFloorPrice"
    querystring = {
        "contractAddress": contract_address
    }
    headers = {
        'Content-Type': "application/json"
    }
    client = build_http_client()
    response = client.get(
        url, headers=headers, params=querystring).json()
    return response.get("openSea", {}).get("floorPrice")


def _execute_moralis_request(contract_address: str, cursor: str):
    url = f"{MORALIS_BASE_URL}/{contract_address}"
    querystring = {
        "chain": "eth",
        "format": "decimal"
    }
    if cursor:
        querystring["cursor"] = cursor
    headers = {
        'Content-Type': "application/json",
        'X-API-Key': MORALIS_API_KEY
    }
    client = build_http_client()
    response = client.get(
        url, headers=headers, params=querystring)
    return response.json()


def _execute_moralis_transactions(contract_address: str, cursor: str):
    url = f"{MORALIS_BASE_URL}/{contract_address}/transfers"
    querystring = {
        "chain": "eth",
        "format": "decimal"
    }
    if cursor:
        querystring["cursor"] = cursor
    headers = {
        'Content-Type': "application/json",
        'X-API-Key': MORALIS_API_KEY
    }
    client = build_http_client()
    response = client.get(
        url, headers=headers, params=querystring)
    return response.json()


def _get_moralis_floor_price(contract_address: str):
    url = f"{MORALIS_BASE_URL}/{contract_address}/lowestprice"
    querystring = {
        "chain": "eth"
    }
    headers = {
        'Content-Type': "application/json",
        'Authorization': MORALIS_API_KEY
    }
    client = build_http_client()
    response = client.get(
        url, headers=headers, params=querystring)
    return response.json().get("price")


def _execute_quicknode_request(contract_address: str, page_number: int):
    data = json.dumps({
        "id": 0,
        "jsonrpc": "2.0",
        "method": "qn_fetchNFTsByCollection",
        "params": {
            "collection": contract_address,
            "page": page_number,
            "perPage": 100
        }
    })
    headers = {
        'Content-Type': 'application/json',
        'x-qn-api-version': '1'
    }
    client = build_http_client()
    response = client.post(
        QUICKNODE_URL, headers=headers, data=data)
    return response.json()


def _crunch_nftport_stats(response, stats: ContractStats):
    for nft in response.get("nfts"):
        if nft.get("metadata"):
            stats.num_has_metadata += 1
        if nft.get("cached_file_url"):
            stats.num_has_cached_image += 1
        stats.num_nfts += 1
    return stats


def _crunch_alechemy_stats(response, stats: ContractStats):
    for nft in response.get("nfts"):
        if nft.get("metadata"):
            stats.num_has_metadata += 1
            media = nft.get("media")
            if media and media[0].get("gateway"):
                # Only files uploaded to cloudinary are cached, others are IPFS gateways
                if "res.cloudinary.com" in media[0].get("gateway", ""):
                    stats.num_has_cached_image += 1
        stats.num_nfts += 1
    return stats


def _crunch_moralis_stats(response, stats: ContractStats):
    for nft in response.get("result"):
        if nft.get("metadata"):
            stats.num_has_metadata += 1
        # Moralis does not have cached files
        stats.num_nfts += 1
    return stats


def _crunch_quicknode_stats(response, stats: ContractStats):
    for nft in response.get("result", {}).get("tokens", []):
        if nft.get("imageUrl") or nft.get("traits"):
            stats.num_has_metadata += 1
        # Quicknode does not return raw metadata
        # Quicknode does not cache image
        stats.num_nfts += 1
    return stats


def _get_nftport_contract_stats(contract_address: str):
    page_number = 1
    stats = ContractStats()
    i = 0
    try:
        while True:
            i += 1
            if i > MAX_PAGES_PER_CONTRACT:
                break
            response = _execute_nftport_request(
                contract_address=contract_address, page_number=page_number)
            stats = _crunch_nftport_stats(response, stats)
            if not response.get("nfts"):
                break
        if _get_nftport_floor_price(contract_address):
            stats.has_floor_price = True
    except Exception:
        print(f"Following error occurred for contract {contract_address}")
        print(traceback.format_exc())
    return stats


def _get_alchemy_contract_stats(contract_address: str):
    start_token = ""
    stats = ContractStats()
    i = 0
    try:
        while True:
            i += 1
            if i > MAX_PAGES_PER_CONTRACT:
                break
            response = _execute_alechmey_request(
                contract_address=contract_address, start_token=start_token)
            start_token = response.get("nextToken")
            stats = _crunch_alechemy_stats(response, stats)
            if not start_token:
                break
        if _get_alchemy_floor_price(contract_address):
            stats.has_floor_price = True
    except Exception:
        print(f"Following error occurred for contract {contract_address}")
        print(traceback.format_exc())
    return stats


def _get_moralis_contract_stats(contract_address: str):
    cursor = None
    stats = ContractStats()
    i = 0
    try:
        while True:
            i += 1
            if i > MAX_PAGES_PER_CONTRACT:
                break
            response = _execute_moralis_request(
                contract_address=contract_address, cursor=cursor)
            cursor = response.get("cursor")
            stats = _crunch_moralis_stats(response, stats)
            if not cursor:
                break
        if _get_moralis_floor_price(contract_address):
            stats.has_floor_price = True
    except Exception:
        print(f"Following error occurred for contract {contract_address}")
        print(traceback.format_exc())
    return stats


def _get_quicknode_contract_stats(contract_address: str):
    page_number = 1
    stats = ContractStats()
    i = 0
    try:
        while True:
            i += 1
            if i > MAX_PAGES_PER_CONTRACT:
                break
            response = _execute_quicknode_request(
                contract_address=contract_address, page_number=page_number)
            stats = _crunch_quicknode_stats(response, stats)
            if not response.get("result", {}).get("tokens"):
                break
        # Quicknode does not have floor price
    except Exception:
        print(f"Following error occurred for contract {contract_address}")
        print(traceback.format_exc())
    return stats


def _get_nftport_transaction_stats(contract_address: str) -> int:
    continuation = ""
    sales = 0
    i = 0
    try:
        while True:
            i += 1
            if i > MAX_PAGES_PER_CONTRACT:
                break
            response = _execute_nftport_transactions(
                contract_address=contract_address, continuation=continuation)
            continuation = response.get("continuation")
            transactions = response.get("transactions")
            for t in transactions:
                if t.get("type") == "sale" and t.get("price_details"):
                    sales += 1
            if not continuation:
                break
    except Exception:
        print(f"Following error occurred for contract {contract_address}")
        print(traceback.format_exc())
    return sales


def _get_alchemy_transaction_stats(contract_address: str) -> int:
    page_key = None
    sales = 0
    i = 0
    try:
        while True:
            i += 1
            if i > MAX_PAGES_PER_CONTRACT:
                break
            response = _execute_alchemy_transactions(
                contract_address=contract_address, page_key=page_key)
            page_key = response.get("result", {}).get("pageKey")
            transactions = response.get("result", {}).get("transfers", [])
            for t in transactions:
                if t.get("value"):
                    sales += 1
                    print(t.get("value"))
            if not page_key:
                break
    except Exception:
        print(f"Following error occurred for contract {contract_address}")
        print(traceback.format_exc())
    return sales


def _get_moralis_transaction_stats(contract_address: str) -> int:
    cursor = None
    sales = 0
    i = 0
    try:
        while True:
            i += 1
            if i > MAX_PAGES_PER_CONTRACT:
                break
            response = _execute_moralis_transactions(
                contract_address=contract_address, cursor=cursor)
            cursor = response.get("cursor")
            transactions = response.get("result", [])
            for t in transactions:
                price = int(t.get("value", 0))
                if price > 0:
                    # Moralis bundles sales together with transfer, value > 0 means sale
                    sales += 1
            if not cursor:
                break
    except Exception:
        print(f"Following error occurred for contract {contract_address}")
        print(traceback.format_exc())
    return sales


def _clear():
    with open(STATS_OUTPUT_FILE_PATH, "w") as f:
        f.write("")


def _log(line: str):
    with open(STATS_OUTPUT_FILE_PATH, "a") as f:
        f.write(line)


def _write_provider_contract_stats(stats: ContractStats):
    _log(f"\nNumber of NFTs found:   {stats.num_nfts}")
    _log(f"\nNumber of NFTs with metadata:   {stats.num_has_metadata}")
    _log(
        f"\nNumber of NFTs with cached images:   {stats.num_has_cached_image}")
    _log(
        f"\nNumber of transactions with sale price:   {stats.num_sale_transactions}")
    _log(f"\nFloor price available:   {stats.has_floor_price}")


def _write_stats(contracts: List[CompareContractStats]):
    _log(f"\n\n---------------------------------------------------------\n")
    _log(f"Stats for each collection")
    _log(f"\n---------------------------------------------------------\n")
    for contract in contracts:
        _log(f"\nAddress:  {contract.address}")
        _log(f"\nSlug:  {contract.slug}")
        _log(f"\nToken supply:   {contract.token_supply}")
        for provider in PROVIDERS:
            provider_stats = getattr(contract, provider)
            _log(f"\n\n{provider}:")
            _write_provider_contract_stats(provider_stats)
        _log(
            f"\n\n---------------------------------------------------------\n")


def _write_global_stats(
        global_stats: GlobalCompareContractStats
):
    _clear()
    _log(f"\n## Report of collection stats\n\n")
    _log(
        f"\nTotal number of collections:   {global_stats.total_num_collections}")
    _log(
        f"\nTotal NFT supply of all collections:   {global_stats.total_token_supply}\n")
    for provider in PROVIDERS:
        stats = getattr(global_stats, provider)
        _log(f"\n---------------------------------------------------------\n")
        _log(f"\n{provider}\n")
        _log(f"\nTotal NFTs found:   {stats.total_num_nfts}")
        _log(
            f"\nTotal NFTs that have metadata:    {stats.total_num_has_metadata}")
        _log(
            f"\nTotal NFTs that have cached images:    {stats.total_num_has_cached_image}")
        _log(
            f"\nTotal transactions with sale price:    {stats.total_num_sale_transactions}")
        _log(
            f"\nTotal collections with floor price:    {stats.total_collections_floor_price_found}")


def _calculate_global_stats(
        contract_stats: List[CompareContractStats]
) -> GlobalCompareContractStats:
    global_stats = GlobalCompareContractStats()
    for contract_stat in contract_stats:
        global_stats.total_num_collections += 1
        global_stats.total_token_supply += (contract_stat.token_supply or 0)
        for provider in PROVIDERS:
            global_stats_provider = getattr(global_stats, provider)
            contract_stats_provider = getattr(contract_stat, provider)
            global_stats_provider.total_num_nfts += contract_stats_provider.num_nfts
            global_stats_provider.total_num_has_metadata += contract_stats_provider.num_has_metadata
            global_stats_provider.total_num_has_cached_image += contract_stats_provider.num_has_cached_image
            global_stats_provider.total_num_sale_transactions += contract_stats_provider.num_sale_transactions
            global_stats_provider.total_collections_floor_price_found += int(
                contract_stats_provider.has_floor_price)
    return global_stats


def _run_thread(df, is_get_transactions: bool):
    contract_stats = []
    df = df.reset_index()
    for index, row in df.iterrows():
        address = row["contract_address"]
        try:
            token_supply = _get_token_supply(contract_address=address)
            nftport_stats = _get_nftport_contract_stats(
                contract_address=address)
            alchemy_stats = _get_alchemy_contract_stats(
                contract_address=address)
            moralis_stats = _get_moralis_contract_stats(
                contract_address=address
            )
            quicknode_stats = _get_quicknode_contract_stats(
                contract_address=address
            )
            if is_get_transactions:
                nftport_stats.num_sale_transactions = _get_nftport_transaction_stats(
                    contract_address=address)
                alchemy_stats.num_sale_transactions = _get_alchemy_transaction_stats(
                    contract_address=address)
                moralis_stats.num_sale_transactions = _get_moralis_transaction_stats(
                    contract_address=address)
                # Quicknode does not have transactions by contract as of yet
            contract_stats.append(
                CompareContractStats(
                    address=address,
                    slug=row["slug"],
                    token_supply=token_supply,
                    nftport=nftport_stats,
                    alchemy=alchemy_stats,
                    moralis=moralis_stats,
                    quicknode=quicknode_stats
                )
            )
        except Exception:
            print(f"Following error occurred for contract {address}")
            print(traceback.format_exc())
    return contract_stats


def _get_patches(df):
    return [df.iloc[i::PROCESS_COUNT] for i in range(PROCESS_COUNT)]


def main():
    df = pd.read_csv(TOP_COLLECTIONS_PATH)[:TOP_K_COLLECTIONS]
    is_get_transactions = True
    patches = _get_patches(df)
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=PROCESS_COUNT) as executor:
        futures = []
        for i in range(len(patches)):
            future = executor.submit(
                _run_thread, patches[i], is_get_transactions)
            futures.append(future)
            time.sleep(0.1)
        contract_stats = []
        for f in futures:
            res = f.result()
            if res:
                contract_stats.extend(res)
    global_stats = _calculate_global_stats(contract_stats)
    _write_global_stats(global_stats)
    _write_stats(contract_stats)
    print("Completed writing report!")


if __name__ == '__main__':
    main()
