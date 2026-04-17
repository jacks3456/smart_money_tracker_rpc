from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv


DEFAULT_POLL_INTERVAL_SECONDS = 3600
DEFAULT_BOOTSTRAP_LOOKBACK_MINUTES = 60
DEFAULT_TIMEOUT_SECONDS = 45
DEFAULT_EVM_LOG_BLOCK_CHUNK = 1500
DEFAULT_SOLANA_SIGNATURE_LIMIT = 100
MAX_SEEN_TRANSACTIONS = 5000
USER_AGENT = "smart-money-tracker-rpc/1.0"
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ERC20_SYMBOL_SELECTOR = "0x95d89b41"
ERC20_DECIMALS_SELECTOR = "0x313ce567"
NATIVE_TOKEN_PLACEHOLDER = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
EVM_MONITOR_CHAINS = ("ethereum", "base", "bnb")
SOL_MONITOR_CHAIN = "solana"
SOL_SWAP_KEYWORDS = ("swap", "route", "raydium", "jupiter", "orca", "meteora", "pump")
CHAIN_NATIVE_SYMBOL = {
    "ethereum": "ETH",
    "base": "ETH",
    "bnb": "BNB",
}
CHAIN_WRAPPED_NATIVE_TOKEN = {
    "ethereum": "0xc02aa39b223fe8d0a0e5c4f27ead9083c756cc2",
    "base": "0x4200000000000000000000000000000000000006",
    "bnb": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
}


@dataclass(slots=True)
class WatchAddress:
    address_type: str
    address: str
    label: str
    blockchains: tuple[str, ...]


@dataclass(slots=True)
class EvmTransfer:
    token_address: str
    from_address: str
    to_address: str
    value: int
    log_index: int
    tx_hash: str
    block_number: int
    watched_address: str
    direction: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_bool(value: str | None, default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def isoformat_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso_datetime(value: str) -> datetime:
    normalized = value.strip().replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).astimezone(timezone.utc)


def normalize_evm_address(value: str) -> str:
    normalized = value.strip().lower()
    if not normalized.startswith("0x") or len(normalized) != 42:
        raise ValueError(f"invalid EVM address: {value}")
    return normalized


def normalize_solana_address(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError("empty Solana address")
    return normalized


def infer_label(row: dict[str, str], address: str) -> str:
    for key in ("label", "name", "alias"):
        candidate = (row.get(key) or "").strip()
        if candidate:
            return candidate

    last_active = (row.get("last_active") or "").strip()
    if last_active:
        return f"{address[:8]}... ({last_active})"
    return address


def load_watchlist(csv_path: Path) -> tuple[list[WatchAddress], list[WatchAddress]]:
    if not csv_path.exists():
        raise FileNotFoundError(f"watchlist file not found: {csv_path}")

    evm_watches: list[WatchAddress] = []
    sol_watches: list[WatchAddress] = []

    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {"address"}
        missing = required.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(f"missing required CSV columns: {', '.join(sorted(missing))}")

        for row in reader:
            address_type = (row.get("address_type") or "").strip().lower() or "evm"
            if address_type not in {"evm", "sol"}:
                continue

            if not parse_bool(row.get("enabled"), default=True):
                continue

            raw_address = (row.get("address") or "").strip()
            if not raw_address:
                continue

            label = infer_label(row, raw_address)
            if address_type == "evm":
                evm_watches.append(
                    WatchAddress(
                        address_type="evm",
                        address=normalize_evm_address(raw_address),
                        label=label,
                        blockchains=EVM_MONITOR_CHAINS,
                    )
                )
            else:
                sol_watches.append(
                    WatchAddress(
                        address_type="sol",
                        address=normalize_solana_address(raw_address),
                        label=label,
                        blockchains=(SOL_MONITOR_CHAIN,),
                    )
                )

    if not evm_watches and not sol_watches:
        raise ValueError("no enabled addresses found in the watchlist CSV")

    return evm_watches, sol_watches


def load_state(state_file: Path, bootstrap_lookback_minutes: int) -> dict[str, Any]:
    if not state_file.exists():
        return {
            "last_checked_at": isoformat_z(utc_now() - timedelta(minutes=bootstrap_lookback_minutes)),
            "seen_transactions": {},
            "evm_last_scanned_blocks": {},
        }

    with state_file.open("r", encoding="utf-8") as handle:
        state = json.load(handle)

    state.setdefault("last_checked_at", isoformat_z(utc_now() - timedelta(minutes=bootstrap_lookback_minutes)))
    state.setdefault("seen_transactions", {})
    state.setdefault("evm_last_scanned_blocks", {})
    return state


def save_state(state_file: Path, state: dict[str, Any]) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    with state_file.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=True, indent=2, sort_keys=True)


def prune_seen_transactions(seen_transactions: dict[str, str]) -> dict[str, str]:
    if len(seen_transactions) <= MAX_SEEN_TRANSACTIONS:
        return seen_transactions

    sorted_items = sorted(seen_transactions.items(), key=lambda item: item[1], reverse=True)
    return dict(sorted_items[:MAX_SEEN_TRANSACTIONS])


def append_alert_log(log_file: Path, message: str) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    with log_file.open("a", encoding="utf-8") as handle:
        handle.write(message)
        handle.write("\n\n")


def send_telegram_alert(bot_token: str, chat_id: str, message: str) -> None:
    response = requests.post(
        f"https://api.telegram.org/bot{bot_token}/sendMessage",
        json={"chat_id": chat_id, "text": message},
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )
    response.raise_for_status()


def send_webhook(url: str, message: str) -> None:
    response = requests.post(url, json={"text": message, "content": message}, timeout=DEFAULT_TIMEOUT_SECONDS)
    response.raise_for_status()


def send_wecom_webhook(url: str, message: str) -> None:
    response = requests.post(
        url,
        json={"msgtype": "markdown", "markdown": {"content": message.replace("\n", "\n> ")}},
        timeout=DEFAULT_TIMEOUT_SECONDS,
    )
    response.raise_for_status()


def dispatch_alerts(message: str, log_file: Path) -> None:
    print(message, flush=True)
    append_alert_log(log_file, message)

    telegram_bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if telegram_bot_token and telegram_chat_id:
        send_telegram_alert(telegram_bot_token, telegram_chat_id, message)

    wecom_webhook_url = os.getenv("WECOM_WEBHOOK_URL", "").strip()
    if wecom_webhook_url:
        send_wecom_webhook(wecom_webhook_url, message)

    for env_name in ("SLACK_WEBHOOK_URL", "DISCORD_WEBHOOK_URL", "GENERIC_WEBHOOK_URL"):
        webhook_url = os.getenv(env_name, "").strip()
        if webhook_url:
            send_webhook(webhook_url, message)


class JsonRpcClient:
    def __init__(self, name: str, url: str, timeout: int = DEFAULT_TIMEOUT_SECONDS):
        self.name = name
        self.url = url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json", "User-Agent": USER_AGENT})
        self._request_id = 0

    def call(self, method: str, params: list[Any]) -> Any:
        self._request_id += 1
        payload = {"jsonrpc": "2.0", "id": self._request_id, "method": method, "params": params}
        response = self.session.post(self.url, json=payload, timeout=self.timeout)
        response.raise_for_status()
        data = response.json()
        if data.get("error"):
            raise RuntimeError(f"{self.name} RPC {method} failed: {data['error']}")
        return data.get("result")


def padded_topic_address(address: str) -> str:
    return "0x" + ("0" * 24) + address[2:].lower()


def int_to_hex(value: int) -> str:
    return hex(value)


def hex_to_int(value: str | None) -> int:
    if not value:
        return 0
    return int(value, 16)


def normalize_hex_address(value: str | None) -> str:
    if not value:
        return ""
    if value.startswith("0x"):
        return "0x" + value[-40:].lower()
    return "0x" + value[-40:].lower()


def safe_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def format_decimal(amount: Decimal) -> str:
    text = format(amount.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def evm_get_latest_block(client: JsonRpcClient) -> int:
    return hex_to_int(client.call("eth_blockNumber", []))


def evm_get_block(client: JsonRpcClient, block_number: int) -> dict[str, Any]:
    return client.call("eth_getBlockByNumber", [int_to_hex(block_number), False]) or {}


def evm_find_block_by_timestamp(client: JsonRpcClient, target_time: datetime, latest_block: int) -> int:
    low = 0
    high = latest_block
    target_ts = int(target_time.timestamp())
    answer = latest_block

    while low <= high:
        mid = (low + high) // 2
        block = evm_get_block(client, mid)
        block_ts = hex_to_int(block.get("timestamp"))
        if block_ts >= target_ts:
            answer = mid
            high = mid - 1
        else:
            low = mid + 1
    return answer


def evm_get_logs(
    client: JsonRpcClient,
    from_block: int,
    to_block: int,
    topics: list[Any],
) -> list[dict[str, Any]]:
    params = [
        {
            "fromBlock": int_to_hex(from_block),
            "toBlock": int_to_hex(to_block),
            "topics": topics,
        }
    ]
    return client.call("eth_getLogs", params) or []


def evm_eth_call(client: JsonRpcClient, to: str, data: str) -> str:
    result = client.call("eth_call", [{"to": to, "data": data}, "latest"])
    return result or "0x"


def evm_get_transaction_receipt(client: JsonRpcClient, tx_hash: str) -> dict[str, Any]:
    return client.call("eth_getTransactionReceipt", [tx_hash]) or {}


def decode_erc20_symbol(raw: str) -> str | None:
    if not raw or raw == "0x":
        return None
    payload = raw[2:]
    if len(payload) >= 128:
        try:
            length = int(payload[64:128], 16)
            symbol_hex = payload[128:128 + length * 2]
            return bytes.fromhex(symbol_hex).decode("utf-8", errors="ignore").strip("\x00") or None
        except ValueError:
            return None
    try:
        decoded = bytes.fromhex(payload).decode("utf-8", errors="ignore").strip("\x00").strip()
        return decoded or None
    except ValueError:
        return None


def decode_erc20_decimals(raw: str) -> int | None:
    if not raw or raw == "0x":
        return None
    try:
        return int(raw, 16)
    except ValueError:
        return None


def evm_token_metadata(client: JsonRpcClient, token_address: str, cache: dict[str, dict[str, Any]]) -> dict[str, Any]:
    cached = cache.get(token_address)
    if cached:
        return cached

    symbol = decode_erc20_symbol(evm_eth_call(client, token_address, ERC20_SYMBOL_SELECTOR)) or token_address[:10]
    decimals = decode_erc20_decimals(evm_eth_call(client, token_address, ERC20_DECIMALS_SELECTOR))
    metadata = {"symbol": symbol, "decimals": 18 if decimals is None else decimals}
    cache[token_address] = metadata
    return metadata


def format_token_amount(raw_value: int, decimals: int) -> str:
    scale = Decimal(10) ** decimals
    return format_decimal(Decimal(raw_value) / scale)


def native_token_metadata(chain: str) -> dict[str, Any]:
    return {
        "symbol": CHAIN_NATIVE_SYMBOL.get(chain, "NATIVE"),
        "address": NATIVE_TOKEN_PLACEHOLDER,
        "decimals": 18,
    }


def receipt_mentions_native_swap(chain: str, receipt: dict[str, Any], watched_wallets: set[str]) -> bool:
    if not watched_wallets:
        return False

    wrapped_native = CHAIN_WRAPPED_NATIVE_TOKEN.get(chain, "")
    native_markers = {
        padded_topic_address(NATIVE_TOKEN_PLACEHOLDER),
        padded_topic_address("0x0000000000000000000000000000000000000000"),
    }
    if wrapped_native:
        native_markers.add(padded_topic_address(wrapped_native))

    watched_markers = {padded_topic_address(address) for address in watched_wallets}

    for log in receipt.get("logs") or []:
        topics = [str(topic).lower() for topic in (log.get("topics") or [])]
        payload = "".join(topics) + str(log.get("data") or "").lower() + str(log.get("address") or "").lower()
        if any(marker in payload for marker in watched_markers) and any(marker in payload for marker in native_markers):
            return True
    return False


def group_evm_transfers(
    transfers: list[EvmTransfer],
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for transfer in sorted(transfers, key=lambda item: (item.tx_hash, item.log_index)):
        entry = grouped.setdefault(
            transfer.tx_hash,
            {
                "wallets": set(),
                "incoming": [],
                "outgoing": [],
                "block_number": transfer.block_number,
            },
        )
        entry["wallets"].add(transfer.watched_address)
        if transfer.direction == "incoming":
            entry["incoming"].append(transfer)
        else:
            entry["outgoing"].append(transfer)
    return grouped


def fetch_evm_swap_candidates(
    chain: str,
    client: JsonRpcClient,
    watches: list[WatchAddress],
    start_block: int,
    end_block: int,
    block_chunk_size: int,
) -> tuple[list[dict[str, Any]], int]:
    if start_block > end_block:
        return [], end_block

    transfers: list[EvmTransfer] = []
    padded_addresses = {watch.address: padded_topic_address(watch.address) for watch in watches}

    for watch in watches:
        topic_address = padded_addresses[watch.address]
        cursor = start_block
        while cursor <= end_block:
            chunk_end = min(cursor + block_chunk_size - 1, end_block)
            outgoing_logs = evm_get_logs(client, cursor, chunk_end, [ERC20_TRANSFER_TOPIC, topic_address])
            incoming_logs = evm_get_logs(client, cursor, chunk_end, [ERC20_TRANSFER_TOPIC, None, topic_address])

            for log in outgoing_logs:
                topics = log.get("topics") or []
                if len(topics) < 3:
                    continue
                transfers.append(
                    EvmTransfer(
                        token_address=normalize_hex_address(log.get("address")),
                        from_address=normalize_hex_address(topics[1]),
                        to_address=normalize_hex_address(topics[2]),
                        value=hex_to_int(log.get("data")),
                        log_index=hex_to_int(log.get("logIndex")),
                        tx_hash=str(log.get("transactionHash", "")).lower(),
                        block_number=hex_to_int(log.get("blockNumber")),
                        watched_address=watch.address,
                        direction="outgoing",
                    )
                )

            for log in incoming_logs:
                topics = log.get("topics") or []
                if len(topics) < 3:
                    continue
                transfers.append(
                    EvmTransfer(
                        token_address=normalize_hex_address(log.get("address")),
                        from_address=normalize_hex_address(topics[1]),
                        to_address=normalize_hex_address(topics[2]),
                        value=hex_to_int(log.get("data")),
                        log_index=hex_to_int(log.get("logIndex")),
                        tx_hash=str(log.get("transactionHash", "")).lower(),
                        block_number=hex_to_int(log.get("blockNumber")),
                        watched_address=watch.address,
                        direction="incoming",
                    )
                )

            cursor = chunk_end + 1

    block_cache: dict[int, dict[str, Any]] = {}
    tx_cache: dict[str, dict[str, Any]] = {}
    receipt_cache: dict[str, dict[str, Any]] = {}
    token_cache: dict[str, dict[str, Any]] = {}
    rows: list[dict[str, Any]] = []

    for tx_hash, grouped in group_evm_transfers(transfers).items():
        tx_data = tx_cache.get(tx_hash)
        if tx_data is None:
            tx_data = client.call("eth_getTransactionByHash", [tx_hash]) or {}
            tx_cache[tx_hash] = tx_data

        block_number = grouped["block_number"]
        block = block_cache.get(block_number)
        if block is None:
            block = evm_get_block(client, block_number)
            block_cache[block_number] = block

        block_time = datetime.fromtimestamp(hex_to_int(block.get("timestamp")), tz=timezone.utc)
        tx_from = normalize_hex_address(tx_data.get("from"))
        tx_to = normalize_hex_address(tx_data.get("to"))
        tx_value = hex_to_int(tx_data.get("value"))

        if grouped["incoming"] and grouped["outgoing"]:
            outgoing = grouped["outgoing"][0]
            incoming = grouped["incoming"][0]
            outgoing_meta = evm_token_metadata(client, outgoing.token_address, token_cache)
            incoming_meta = evm_token_metadata(client, incoming.token_address, token_cache)

            rows.append(
                {
                    "kind": "evm",
                    "blockchain": chain,
                    "block_time": isoformat_z(block_time),
                    "tx_hash": tx_hash,
                    "tx_from": tx_from,
                    "tx_to": tx_to,
                    "project": tx_to or "unknown-router",
                    "trade_source": "erc20-transfer-heuristic",
                    "watched_wallets": sorted(grouped["wallets"]),
                    "token_sold_symbol": outgoing_meta["symbol"],
                    "token_sold_address": outgoing.token_address,
                    "token_sold_amount": format_token_amount(outgoing.value, outgoing_meta["decimals"]),
                    "token_bought_symbol": incoming_meta["symbol"],
                    "token_bought_address": incoming.token_address,
                    "token_bought_amount": format_token_amount(incoming.value, incoming_meta["decimals"]),
                    "token_pair": f"{outgoing_meta['symbol']}/{incoming_meta['symbol']}",
                }
            )
            continue

        if grouped["incoming"] and tx_value > 0 and tx_from in grouped["wallets"]:
            incoming = grouped["incoming"][0]
            incoming_meta = evm_token_metadata(client, incoming.token_address, token_cache)
            sold_meta = native_token_metadata(chain)
            rows.append(
                {
                    "kind": "evm",
                    "blockchain": chain,
                    "block_time": isoformat_z(block_time),
                    "tx_hash": tx_hash,
                    "tx_from": tx_from,
                    "tx_to": tx_to,
                    "project": tx_to or "unknown-router",
                    "trade_source": "native-value+erc20-in-heuristic",
                    "watched_wallets": sorted(grouped["wallets"]),
                    "token_sold_symbol": sold_meta["symbol"],
                    "token_sold_address": sold_meta["address"],
                    "token_sold_amount": format_token_amount(tx_value, sold_meta["decimals"]),
                    "token_bought_symbol": incoming_meta["symbol"],
                    "token_bought_address": incoming.token_address,
                    "token_bought_amount": format_token_amount(incoming.value, incoming_meta["decimals"]),
                    "token_pair": f"{sold_meta['symbol']}/{incoming_meta['symbol']}",
                }
            )
            continue

        if grouped["outgoing"]:
            receipt = receipt_cache.get(tx_hash)
            if receipt is None:
                receipt = evm_get_transaction_receipt(client, tx_hash)
                receipt_cache[tx_hash] = receipt

            if receipt_mentions_native_swap(chain, receipt, grouped["wallets"]):
                outgoing = grouped["outgoing"][0]
                outgoing_meta = evm_token_metadata(client, outgoing.token_address, token_cache)
                bought_meta = native_token_metadata(chain)
                rows.append(
                    {
                        "kind": "evm",
                        "blockchain": chain,
                        "block_time": isoformat_z(block_time),
                        "tx_hash": tx_hash,
                        "tx_from": tx_from,
                        "tx_to": tx_to,
                        "project": tx_to or "unknown-router",
                        "trade_source": "erc20-out+native-receipt-heuristic",
                        "watched_wallets": sorted(grouped["wallets"]),
                        "token_sold_symbol": outgoing_meta["symbol"],
                        "token_sold_address": outgoing.token_address,
                        "token_sold_amount": format_token_amount(outgoing.value, outgoing_meta["decimals"]),
                        "token_bought_symbol": bought_meta["symbol"],
                        "token_bought_address": bought_meta["address"],
                        "token_bought_amount": "native-amount-unavailable",
                        "token_pair": f"{outgoing_meta['symbol']}/{bought_meta['symbol']}",
                    }
                )

    return rows, end_block


def solana_get_signatures(client: JsonRpcClient, address: str, limit: int) -> list[dict[str, Any]]:
    return client.call("getSignaturesForAddress", [address, {"limit": limit}]) or []


def solana_get_transaction(client: JsonRpcClient, signature: str) -> dict[str, Any]:
    return client.call(
        "getTransaction",
        [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}],
    ) or {}


def solana_balance_deltas(meta: dict[str, Any], watched_address: str) -> list[dict[str, Any]]:
    pre_balances = meta.get("preTokenBalances") or []
    post_balances = meta.get("postTokenBalances") or []
    deltas: dict[str, Decimal] = {}

    for item in pre_balances:
        if item.get("owner") != watched_address:
            continue
        mint = item.get("mint")
        if not mint:
            continue
        amount = safe_decimal((item.get("uiTokenAmount") or {}).get("uiAmountString"))
        deltas[mint] = deltas.get(mint, Decimal("0")) - amount

    for item in post_balances:
        if item.get("owner") != watched_address:
            continue
        mint = item.get("mint")
        if not mint:
            continue
        amount = safe_decimal((item.get("uiTokenAmount") or {}).get("uiAmountString"))
        deltas[mint] = deltas.get(mint, Decimal("0")) + amount

    results = []
    for mint, delta in deltas.items():
        if abs(delta) < Decimal("0.00000001"):
            continue
        results.append({"mint": mint, "delta": delta})
    return results


def fetch_solana_swap_candidates(
    client: JsonRpcClient,
    watches: list[WatchAddress],
    start_time: datetime,
    signature_limit: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for watch in watches:
        signatures = solana_get_signatures(client, watch.address, signature_limit)
        for item in signatures:
            block_time_raw = item.get("blockTime")
            if not block_time_raw:
                continue

            block_time = datetime.fromtimestamp(int(block_time_raw), tz=timezone.utc)
            if block_time < start_time:
                break

            tx = solana_get_transaction(client, item["signature"])
            meta = tx.get("meta") or {}
            if meta.get("err") is not None:
                continue

            deltas = solana_balance_deltas(meta, watch.address)
            incoming = [item for item in deltas if item["delta"] > 0]
            outgoing = [item for item in deltas if item["delta"] < 0]
            if not incoming or not outgoing:
                continue

            log_messages = [str(log).lower() for log in (meta.get("logMessages") or [])]
            keyword_hit = any(keyword in log for log in log_messages for keyword in SOL_SWAP_KEYWORDS)
            message = tx.get("transaction") or {}
            account_keys = message.get("message", {}).get("accountKeys") or []
            signers = []
            for entry in account_keys:
                if isinstance(entry, dict):
                    pubkey = entry.get("pubkey")
                    if pubkey:
                        signers.append(pubkey)
                elif isinstance(entry, str):
                    signers.append(entry)

            outgoing_token = outgoing[0]
            incoming_token = incoming[0]
            rows.append(
                {
                    "kind": "sol",
                    "blockchain": "solana",
                    "block_time": isoformat_z(block_time),
                    "tx_id": item["signature"],
                    "project": "solana-programs",
                    "trade_source": "token-balance-heuristic" + ("+logs" if keyword_hit else ""),
                    "watched_wallets": [watch.address],
                    "tx_from": signers[0] if signers else watch.address,
                    "tx_to": "",
                    "token_sold_symbol": outgoing_token["mint"][:8],
                    "token_sold_address": outgoing_token["mint"],
                    "token_sold_amount": format_decimal(abs(outgoing_token["delta"])),
                    "token_bought_symbol": incoming_token["mint"][:8],
                    "token_bought_address": incoming_token["mint"],
                    "token_bought_amount": format_decimal(incoming_token["delta"]),
                    "token_pair": f"{outgoing_token['mint'][:8]}/{incoming_token['mint'][:8]}",
                    "event_name": "swap-like" if keyword_hit else "token rebalance",
                }
            )

    return rows


def tx_identifier(row: dict[str, Any]) -> str:
    if row.get("tx_hash"):
        return f"{row.get('blockchain')}:{str(row['tx_hash']).lower()}"
    if row.get("tx_id"):
        return f"{row.get('blockchain')}:{row['tx_id']}"
    return ""


def format_alert(row: dict[str, Any], watch_map: dict[str, WatchAddress]) -> str:
    watched_wallets = ", ".join(
        watch_map.get(address, WatchAddress("", address, address, tuple())).label
        for address in row.get("watched_wallets", [])
    )
    tx_id = row.get("tx_hash") or row.get("tx_id") or "unknown"
    return (
        f"time: {row.get('block_time', 'unknown')}\n"
        f"chain: {row.get('blockchain', 'unknown')}\n"
        f"watched wallet: {watched_wallets or 'unknown'}\n"
        f"initiator: {row.get('tx_from', 'unknown')}\n"
        f"pair: {row.get('token_pair', 'unknown')}\n"
        f"sell: {row.get('token_sold_amount', 'n/a')} {row.get('token_sold_symbol', 'unknown')}\n"
        f"sell token: {row.get('token_sold_address', 'n/a')}\n"
        f"buy: {row.get('token_bought_amount', 'n/a')} {row.get('token_bought_symbol', 'unknown')}\n"
        f"buy token: {row.get('token_bought_address', 'n/a')}\n"
        f"tx: {tx_id}"
    )


def build_rpc_clients() -> tuple[dict[str, JsonRpcClient], JsonRpcClient | None]:
    evm_clients: dict[str, JsonRpcClient] = {}
    for chain, env_name in (
        ("ethereum", "ETHEREUM_RPC_URL"),
        ("base", "BASE_RPC_URL"),
        ("bnb", "BNB_RPC_URL"),
    ):
        rpc_url = os.getenv(env_name, "").strip()
        if rpc_url:
            evm_clients[chain] = JsonRpcClient(chain, rpc_url)

    solana_url = os.getenv("SOLANA_RPC_URL", "").strip()
    solana_client = JsonRpcClient("solana", solana_url) if solana_url else None
    return evm_clients, solana_client


def run_once(
    evm_clients: dict[str, JsonRpcClient],
    solana_client: JsonRpcClient | None,
    csv_path: Path,
    state_file: Path,
    alert_log_file: Path,
    bootstrap_lookback_minutes: int,
    evm_log_block_chunk: int,
    solana_signature_limit: int,
) -> None:
    state = load_state(state_file, bootstrap_lookback_minutes)
    start_time = parse_iso_datetime(state["last_checked_at"])
    end_time = utc_now()

    evm_watches, sol_watches = load_watchlist(csv_path)
    watch_map = {watch.address: watch for watch in evm_watches + sol_watches}
    rows: list[dict[str, Any]] = []

    if evm_watches:
        evm_last_scanned = state.get("evm_last_scanned_blocks", {})
        for chain, client in evm_clients.items():
            try:
                latest_block = evm_get_latest_block(client)
                saved_block = evm_last_scanned.get(chain)
                if saved_block is None:
                    start_block = evm_find_block_by_timestamp(client, start_time, latest_block)
                else:
                    start_block = int(saved_block) + 1

                chain_rows, last_scanned_block = fetch_evm_swap_candidates(
                    chain=chain,
                    client=client,
                    watches=evm_watches,
                    start_block=start_block,
                    end_block=latest_block,
                    block_chunk_size=evm_log_block_chunk,
                )
                rows.extend(chain_rows)
                evm_last_scanned[chain] = last_scanned_block
            except Exception as exc:
                append_alert_log(
                    alert_log_file,
                    f"[{isoformat_z(utc_now())}] evm chain scan failed for {chain}: {exc}",
                )
        state["evm_last_scanned_blocks"] = evm_last_scanned

    if sol_watches and solana_client is not None:
        try:
            rows.extend(fetch_solana_swap_candidates(solana_client, sol_watches, start_time, solana_signature_limit))
        except Exception as exc:
            append_alert_log(
                alert_log_file,
                f"[{isoformat_z(utc_now())}] solana scan failed: {exc}",
            )

    seen_transactions = state.get("seen_transactions", {})
    fresh_rows: list[dict[str, Any]] = []
    for row in sorted(rows, key=lambda item: item.get("block_time", "")):
        identifier = tx_identifier(row)
        if not identifier or identifier in seen_transactions:
            continue
        fresh_rows.append(row)
        seen_transactions[identifier] = isoformat_z(end_time)

    for row in fresh_rows:
        dispatch_alerts(format_alert(row, watch_map), alert_log_file)

    state["last_checked_at"] = isoformat_z(end_time)
    state["seen_transactions"] = prune_seen_transactions(seen_transactions)
    save_state(state_file, state)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Monitor smart money swap activity directly from RPC.")
    parser.add_argument("--once", action="store_true", help="Run a single polling cycle and exit.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv()

    csv_path = Path(os.getenv("SMART_MONEY_CSV", "smart_money_active.csv")).expanduser().resolve()
    state_file = Path(os.getenv("STATE_FILE", "monitor_state.json")).expanduser().resolve()
    alert_log_file = Path(os.getenv("ALERT_LOG_FILE", "alerts.log")).expanduser().resolve()
    poll_interval = int(os.getenv("POLL_INTERVAL_SECONDS", str(DEFAULT_POLL_INTERVAL_SECONDS)))
    bootstrap_lookback_minutes = int(
        os.getenv("BOOTSTRAP_LOOKBACK_MINUTES", str(max(DEFAULT_BOOTSTRAP_LOOKBACK_MINUTES, poll_interval // 60)))
    )
    evm_log_block_chunk = int(os.getenv("EVM_LOG_BLOCK_CHUNK", str(DEFAULT_EVM_LOG_BLOCK_CHUNK)))
    solana_signature_limit = int(os.getenv("SOLANA_SIGNATURE_LIMIT", str(DEFAULT_SOLANA_SIGNATURE_LIMIT)))

    evm_clients, solana_client = build_rpc_clients()
    if not evm_clients and solana_client is None:
        print("at least one RPC URL is required.", file=sys.stderr)
        return 1

    print(
        f"watching {csv_path}; evm_chains={sorted(evm_clients)}; solana={'enabled' if solana_client else 'disabled'}; "
        f"polling every {poll_interval}s",
        flush=True,
    )

    while True:
        try:
            run_once(
                evm_clients=evm_clients,
                solana_client=solana_client,
                csv_path=csv_path,
                state_file=state_file,
                alert_log_file=alert_log_file,
                bootstrap_lookback_minutes=bootstrap_lookback_minutes,
                evm_log_block_chunk=evm_log_block_chunk,
                solana_signature_limit=solana_signature_limit,
            )
            if args.once:
                return 0
        except KeyboardInterrupt:
            print("monitor stopped by user", flush=True)
            return 0
        except Exception as exc:
            error_message = f"[{isoformat_z(utc_now())}] monitor error: {exc}"
            print(error_message, file=sys.stderr, flush=True)
            append_alert_log(alert_log_file, error_message)
            if args.once:
                return 1

        time.sleep(poll_interval)


if __name__ == "__main__":
    raise SystemExit(main())
