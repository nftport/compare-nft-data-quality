import requests
from tqdm import tqdm
from typing import List
from dataclasses import dataclass
from dataclasses import fields
from requests.adapters import HTTPAdapter
from requests.adapters import Retry
import json
import traceback

STATS_OUTPUT_FILE_PATH = "ownership_stats.md"

NFTPORT_API_KEY = "API-KEY"
ALCHEMY_API_KEY = "API-KEY"
ALCHEMY_BASE_URL = f"https://eth-mainnet.alchemyapi.io/nft/v2/{ALCHEMY_API_KEY}"
MORALIS_API_KEY = "API-KEY"
MORALIS_BASE_URL = "https://deep-index.moralis.io/api/v2"
QUICKNODE_URL = "QUICKNODE-URL"

# Most prominent NFT wallets as reported here- https://www.nftqt.com/the-nft-whale-directory-50-top-nft-buyers-whales/
wallets = [
    "0x3432B45B9ee95BD5C31a726B936CF2ec719A2153",
    "0xf476Cd75BE8Fdd197AE0b466A2ec2ae44Da41897",
    "0xfD22004806A6846EA67ad883356be810F0428793",
    "0x79788917F5963880F79e138A3F01ac049df40812",
    "0x2Debdf4427CcBcfDbC7f29D63964499a0ec184F6",
    "0x6c0Cf880cB20EefabFB09341Fba9e2Bd29ad3DFA",
    "0xe32ACFcEdfa8BDa1d0ed03CaFa854219Ce0C9cEA",
    "0xd4b5D09a7f72aC7EbC1853Fd2663Bdd23c059b02",
    "0xCA37d80aBD9799E9060E2Fa64B9A9476E5Bb097f",
    "0x378BCce7235D53BBc3774BFf8559191F06E6818E",
    "0x721931508DF2764fD4F70C53Da646Cb8aEd16acE",
    "0x1DA5331994e781AB0E2AF9f85bfce2037A514170",
    "0x0bD3D88aec644c098574C881c25e963d9f57CA57",
    "0x2337918436302b406C9D0B775366184799d1Df88",
    "0xd6a984153aCB6c9E2d788f08C2465a1358BB89A7",
    "0xf0D6999725115E3EAd3D927Eb3329D63AFAEC09b",
    "0xeEE5Eb24E7A0EA53B75a1b9aD72e7D20562f4283",
    "0x85560DBeF2533eEc139b3e206b119fD700f90262",
    "0xe1D29d0a39962a9a8d2A297ebe82e166F8b8EC18",
    "0xA351a4FffCeed60b6d4351e1B20C55E3A6fB5503"
]


@dataclass(frozen=False)
class WalletStats:
    address: str
    nftport_nfts_count: int = 0
    alchemy_nfts_count: int = 0
    quicknode_nfts_count: int = 0
    moralis_nfts_count: int = 0


@dataclass(frozen=False)
class GlobalProviderStats:
    total_nftport_nfts_count: int = 0
    total_alchemy_nfts_count: int = 0
    total_quicknode_nfts_count: int = 0
    total_moralis_nfts_count: int = 0


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


def _execute_nftport_request(address: str):
    url = f"https://api.nftport.xyz/v0/accounts/{address}"
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
    return response.json()


def _execute_alchemy_request(address: str):
    url = f"{ALCHEMY_BASE_URL}/getNFTs"
    querystring = {
        "owner": address
    }
    headers = {
        'Content-Type': "application/json"
    }
    client = build_http_client()
    response = client.get(
        url, headers=headers, params=querystring)
    return response.json()


def _execute_moralis_request(address: str):
    url = f"{MORALIS_BASE_URL}/{address}/nft"
    querystring = {
        "chain": "eth",
        "format": "decimal"
    }
    headers = {
        'Content-Type': "application/json",
        'X-API-Key': MORALIS_API_KEY
    }
    client = build_http_client()
    response = client.get(
        url, headers=headers, params=querystring)
    return response.json()


def _execute_quicknode_request(address: str):
    data = json.dumps({
        "id": 0,
        "jsonrpc": "2.0",
        "method": "qn_fetchNFTs",
        "params": {
            "wallet": address,
            "page": 1,
            "perPage": 1
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


def _get_nftport_stats(address: str):
    count = 0
    try:
        response = _execute_nftport_request(
            address=address)
        count = response.get("total", 0)
    except Exception:
        print(f"Following error occurred for wallet {address}")
        print(traceback.format_exc())
    return count


def _get_alchemy_stats(address: str):
    count = 0
    try:
        response = _execute_alchemy_request(
            address=address)
        count = response.get("totalCount", 0)
    except Exception:
        print(f"Following error occurred for wallet {address}")
        print(traceback.format_exc())
    return count


def _get_quicknode_stats(address: str):
    count = 0
    try:
        response = _execute_quicknode_request(
            address=address)
        count = response.get("result", {}).get("totalItems", 0)
    except Exception:
        print(f"Following error occurred for address {address}")
        print(traceback.format_exc())
    return count


def _get_moralis_stats(address: str):
    count = 0
    try:
        response = _execute_moralis_request(
            address=address)
        count = response.get("total", 0)
    except Exception:
        print(f"Following error occurred for address {address}")
        print(traceback.format_exc())
    return count


def _clear():
    with open(STATS_OUTPUT_FILE_PATH, "w") as f:
        f.write("")


def _log(line: str):
    with open(STATS_OUTPUT_FILE_PATH, "a") as f:
        f.write(line)


def main():
    gloabl_stats = GlobalProviderStats()
    wallets_stats = []
    for address in tqdm(wallets):
        wallet_stats = WalletStats(address=address)
        wallet_stats.nftport_nfts_count = _get_nftport_stats(address)
        wallet_stats.alchemy_nfts_count = _get_alchemy_stats(address)
        wallet_stats.quicknode_nfts_count = _get_quicknode_stats(address)
        wallet_stats.moralis_nfts_count = _get_moralis_stats(address)
        gloabl_stats.total_nftport_nfts_count += wallet_stats.nftport_nfts_count
        gloabl_stats.total_alchemy_nfts_count += wallet_stats.alchemy_nfts_count
        gloabl_stats.total_quicknode_nfts_count += wallet_stats.quicknode_nfts_count
        gloabl_stats.total_moralis_nfts_count += wallet_stats.moralis_nfts_count
        wallets_stats.append(wallet_stats)
    _clear()
    _log("## Global stats of top 20 most prominent wallets")
    _log(f"\nNFTPort total count: {gloabl_stats.total_nftport_nfts_count}")
    _log(f"\nAlchemy total count: {gloabl_stats.total_alchemy_nfts_count}")
    _log(f"\nQuickNode total count: {gloabl_stats.total_quicknode_nfts_count}")
    _log(f"\nMoralis total count: {gloabl_stats.total_moralis_nfts_count}")
    _log("\n\nStats by wallet")
    for stats in wallets_stats:
        _log(f"\n\nWallet address: {stats.address}")
        _log(f"\nNFTPort NFTs count: {stats.nftport_nfts_count}")
        _log(f"\nAlchemy NFTs count: {stats.alchemy_nfts_count}")
        _log(f"\nQuickNode NFTs count: {stats.quicknode_nfts_count}")
        _log(f"\nMoralis NFTs count: {stats.moralis_nfts_count}")


if __name__ == '__main__':
    main()
