"""
Order_server.py
-----------------------
Kafka Consumer-based order server.
Listens to 'trading-signals' topic for order placement requests.

Start:
    python Order_server.py

Kafka Message Format (JSON):
    {
        "Exc": "NSE",
        "SymbolId": "12345",
        "Symbol": "RELIANCE",
        "Side": "BUY",
        "OrderType": "MARKET",
        "ProductType": "MIS",
        "qty": 1,
        "Price": 0,
        "CallBy": "PythonAlgo",
        "PlaceOrder": true,
        "StrategyName": "MyStrategy",
        "PClose": 0,
        "Signature": "JarvisAlgo@123",
        "InstrumentType": "",
        "OrderTag": ""
    }
"""

import csv
import json
import time
import threading
import traceback
from datetime import datetime
from pathlib import Path
from kafka import KafkaConsumer, KafkaProducer
from OrderExecutor_kafka import OrderDispatcher
import redis,pandas as pd
import requests
from dateutil.parser import parse
from zoneinfo import ZoneInfo

# =====================================================
# STARTUP — load mappings & init OrderDispatcher
# =====================================================

def get_public_ip():
    try:
        return requests.get("https://api.ipify.org").text
    except Exception as e:
        return str(e)
    
    
SERVER_IP = get_public_ip()
print(f"Public IP: {SERVER_IP}")

# =====================================================
# CONFIG
# =====================================================
ORDERS_DIR = Path(__file__).parent / "orders"

VALID_SIGNATURE = "JarvisAlgo@123"

KAFKA_BROKER = "98.70.53.180:9092"
TOPIC = "trading-signals"
GROUP_ID = f"server_{SERVER_IP}"
HEARTBEAT_TOPIC = "server-heartbeat"
ALERT_TOPIC = "alert-message"
ALERT_PREFIX = "server-alert: "
SERVER_START_TIME = datetime.now(ZoneInfo("Asia/Kolkata"))
PRODUCER = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks='all',
        retries=3,
        request_timeout_ms=10000
    )
# =====================================================
# REDIS
# =====================================================
redis_conn_str = redis.Redis(
    host='redis-11429.crce179.ap-south-1-1.ec2.cloud.redislabs.com',
    port=11429,
    decode_responses=True,
    username="default",
    password="IKomAGomiMlxqLJbZxsL1SAsv59ZBB3H",
)
# redis_conn_str = redis.Redis(
#     host='redis-19731.crce182.ap-south-1-1.ec2.cloud.redislabs.com',
#     port=19731,
#     decode_responses=True,
#     username="default",
#     password="xJOOwytWRYTeFZUCBXAg1CAdQDzdLWKG",
# )


def read_redis(key):
    try:
        return json.loads(redis_conn_str.get(key))
    except Exception as ex:
        print("Got exceptions = ", ex)
        return {}


# =====================================================
# UTILITIES Functions
# =====================================================

def reverse_jarvis_ttype(ttype):
    mapping = {
        "BUY": ("BUY", "OPEN"),
        "SELL": ("SELL", "CLOSE"),
        "SHORT": ("SELL", "OPEN"),
        "COVER": ("BUY", "CLOSE"),
    }
    try:
        return mapping[ttype.upper()]
    except KeyError:
        raise ValueError(f"Invalid ttype: {ttype}")


def get_users_by_ip(data, ip):
    return [user for user, value in data.items() if value == ip]


def get_ip_from_server_mapping(account_id, broker):
    """
    Fetch IP from server_mapping for a given account_id and broker.
    Returns IP string or None if not found.
    """
    server_mapping_data = read_redis(server_mapping_key)
    if not server_mapping_data:
        return None
    
    entries = server_mapping_data if isinstance(server_mapping_data, list) else [server_mapping_data]
    for entry in entries:
        if entry.get("account_id") == account_id and entry.get("broker") == broker:
            return entry.get("ip")
    return None


def get_server_account_broker():
    """
    Get the account_id and broker for this server from server_mapping.
    Returns (account_id, broker) or (None, None) if not found.
    """
    server_mapping_data = read_redis(server_mapping_key)
    print("server_mapping_data == ",server_mapping_data)
    if not server_mapping_data:
        return None, None
    
    entries = server_mapping_data if isinstance(server_mapping_data, list) else [server_mapping_data]
    for entry in entries:
        print(entry.get("ip"))
        if str(entry.get("ip")) == str(SERVER_IP):
            return entry.get("account_id"), entry.get("broker")
    
    return None, None


def send_alert(message, account_id=None, broker=None):
    """
    Send an alert to the ALERT_TOPIC with account_id and broker included.
    """
    try:
        alert_data = {
            "message": message,
            "user_id": account_id,
            "broker": broker,
            "timestamp": datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S")
        }
        PRODUCER.send(ALERT_TOPIC, value=alert_data)
        PRODUCER.flush(timeout=5)
    except Exception as e:
        print(f"[ALERT_ERROR] Failed to send alert: {e}")
        traceback.print_exc()


# =====================================================
# CSV HELPERS
# =====================================================
ORDER_PLACEMENT_FIELDS = [
    "date", "Exc", "SymbolId", "Symbol", "Side", "OrderType",
    "ProductType", "qty", "Price", "CallBy", "PlaceOrder",
    "StrategyName", "PClose", "InstrumentType", "OrderTag",
    "UserTriggered","MapedIp","SourceIp", "status", "message","timestamp","issquareoff"
]


def ensure_directory():
    """Create orders directory if it doesn't exist."""
    ORDERS_DIR.mkdir(parents=True, exist_ok=True)


def get_order_csv_path(strategy_name: str = "all") -> Path:
    """Generate orders CSV filename as orders_{strategy}_{YYYY-MM-DD}.csv"""
    # today = datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d")
    return ORDERS_DIR / f"{strategy_name}.csv"


def write_order_to_csv(row_data: dict, strategy_name: str = "all"):
    """
    Append an order placement row to the appropriate CSV file.
    Creates the file with headers if it doesn't exist.
    """
    ensure_directory()
    csv_path = get_order_csv_path("OrderServerSignalHistory.csv")
    # csv_path = get_order_csv_path(strategy_name)
    file_exists = csv_path.exists()

    with open(csv_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ORDER_PLACEMENT_FIELDS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(row_data)

strategy_mapping_key = "users:strategy:map"
server_mapping_key = "users:server:map"
strategy_mapping = pd.DataFrame(read_redis(strategy_mapping_key))
server_mapping = read_redis(server_mapping_key)

# Get the account and broker configured for this server
SERVER_ACCOUNT_ID, SERVER_BROKER = get_server_account_broker()
print(f"[STARTUP] Server Account: {SERVER_BROKER}-{SERVER_ACCOUNT_ID}")

if SERVER_ACCOUNT_ID is None or SERVER_BROKER is None:
    print("[ERROR] This server IP is not configured in server_mapping. Exiting...")
    exit(1)

OrderManager = OrderDispatcher("logged_users", "users_details", "OrderServer", "redis")

# =====================================================
# HEARTBEAT — extract account info & detect IP conflicts
# =====================================================
def extract_heartbeat_accounts():
    """
    Read users:strategy:map from Redis.
    - Extract unique (account_id, broker) pairs.
    - For each pair, fetch IP from server_mapping.
    - Filter accounts matching this server's IP (SERVER_IP).
    - Alert if any account has conflicting IPs in server_mapping.
    """
    raw = read_redis(strategy_mapping_key)
    if not raw:
        print("[HEARTBEAT] No data found in users:strategy:map")
        return []

    strategy_entries = raw if isinstance(raw, list) else [raw]
    server_map_data = read_redis(server_mapping_key)
    if not server_map_data:
        print("[HEARTBEAT] No data found in users:server:map")
        return []
    
    server_entries = server_map_data if isinstance(server_map_data, list) else [server_map_data]

    # Build server mapping: (account_id, broker) -> IP
    from collections import defaultdict
    server_ip_map = {}
    account_ip_map = defaultdict(set)  # Track all IPs for each account_id
    
    for entry in server_entries:
        acc_id = entry.get("account_id")
        broker = entry.get("broker")
        ip = entry.get("ip")
        server_ip_map[(acc_id, broker)] = ip
        account_ip_map[acc_id].add(ip)

    # Extract unique (account_id, broker) pairs from strategy_mapping
    seen = set()
    local_accounts = []
    for entry in strategy_entries:
        acc_id = entry.get("account_id")
        broker = entry.get("broker")
        key = (acc_id, broker)
        
        if key not in seen:
            seen.add(key)
            # Get IP from server_mapping
            ip = server_ip_map.get(key)
            
            if ip == SERVER_IP:
                local_accounts.append({
                    "account_id": acc_id,
                    "broker": broker,
                })

    # Alert if any local account_id has multiple IPs in server_mapping
    for acc in local_accounts:
        ips = account_ip_map.get(acc["account_id"], set())
        if len(ips) > 1:
            other_ips = ips - {SERVER_IP}
            send_alert(
                ALERT_PREFIX + f"account_id={acc['account_id']} is mapped to other IP(s): {other_ips}",
                acc["account_id"],
                acc["broker"]
            )
            print(
                f"⚠️  [ALERT] account_id={acc['account_id']} is mapped "
                f"to other IP(s): {other_ips}"
            )

    # Alert if multiple accounts share the same IP
    ip_to_accounts = defaultdict(list)
    for entry in server_entries:
        _ip = entry.get("ip")
        if _ip:
            ip_to_accounts[_ip].append(entry.get("account_id"))
            
    for ip, accs in ip_to_accounts.items():
        unique_accs = list(set(accs))
        if len(unique_accs) > 1:
            send_alert(
                ALERT_PREFIX + f"IP={ip} is shared by multiple accounts: {unique_accs}",
                SERVER_ACCOUNT_ID,
                SERVER_BROKER
            )
            print(
                f"⚠️  [ALERT] IP={ip} is shared by multiple accounts: {unique_accs}"
            )

    return local_accounts


HEARTBEAT_ACCOUNTS = extract_heartbeat_accounts()
print(f"[HEARTBEAT] Tracking {len(HEARTBEAT_ACCOUNTS)} account(s) for IP {SERVER_IP}")
# print("HEARTBEAT_ACCOUNTS == ",HEARTBEAT_ACCOUNTS)
# exit(0)

def heartbeat_sender(interval_minutes=1):
    """
    Periodically publish heartbeat messages to the server-heartbeat Kafka topic.
    Payload: {ip, timestamp, account_id} for each unique account.
    Args:
        interval_minutes: How often to send heartbeats (in minutes).
    """
    interval_seconds = interval_minutes * 60

    print(f"[HEARTBEAT] Sender started → topic: {HEARTBEAT_TOPIC} | interval: {interval_minutes} min")

    while True:
        for acc in HEARTBEAT_ACCOUNTS:
            payload = {
                "ip": SERVER_IP,
                "timestamp": datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S"),
                "account_id": acc["account_id"],
                "broker": acc["broker"],
            }
            try:
                PRODUCER.send(HEARTBEAT_TOPIC, value=payload)
                
            except Exception as e:
                send_alert(
                    ALERT_PREFIX + f"[HEARTBEAT ERROR] {e}",
                    SERVER_ACCOUNT_ID,
                    SERVER_BROKER
                )
                print(f"[HEARTBEAT ERROR] {e}")
        PRODUCER.flush()
        time.sleep(interval_seconds)


# =====================================================
# SIGNAL PROCESSING (replaces FastAPI /place-order)
# =====================================================
def process_signal(signal: dict):
    """
    Process an incoming Kafka signal.
    """
    # --- Required field check ---
    required = ["Exc", "SymbolId", "Symbol", "Side", "OrderType",
                "ProductType", "qty", "StrategyName", "Signature","issquareoff"]
    missing = [f for f in required if f not in signal]
    if missing:
        send_alert(
            ALERT_PREFIX + f"[SKIP] Missing fields: {missing} - {signal.get('StrategyName')} - {signal.get('SymbolId')} - {signal.get('Side')}",
            SERVER_ACCOUNT_ID,
            SERVER_BROKER
        )
        print(f"[SKIP] Missing fields: {missing} - {signal.get("StrategyName")} - {signal.get("SymbolId")} - {signal.get("Side")}")
        return

    # --- Signature validation ---
    if signal.get("Signature") != VALID_SIGNATURE:
        print("[SKIP] Invalid Signature")
        return

    # --- PlaceOrder flag ---
    if not signal.get("PlaceOrder", True):
        print("[SKIP] PlaceOrder=False")
        return

    # --- Timestamp ---
    received_timestamp = signal.get("timestamp")
    
    if received_timestamp:
        received_timestamp = parse(received_timestamp)
        received_timestamp = received_timestamp.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
        if received_timestamp < SERVER_START_TIME:
            send_alert(
                ALERT_PREFIX + f"Received timestamp is older than server start time: {received_timestamp} - {signal.get('StrategyName')} - {signal.get('SymbolId')} - {signal.get('Side')}",
                SERVER_ACCOUNT_ID,
                SERVER_BROKER
            )
            print(f"[SKIP] Received timestamp is older than server start time: {received_timestamp} - {signal.get("StrategyName")} - {signal.get("SymbolId")} - {signal.get("Side")}")
            return
    else:
       return
    
    # --- Strategy lookup ---
    strategy_name = signal.get("StrategyName", "")
    trigger_strategy = strategy_mapping[strategy_mapping["strategy"] == strategy_name]
    if trigger_strategy.empty:
        send_alert(
            ALERT_PREFIX + f"Strategy not found: {strategy_name} - {signal.get('SymbolId')} - {signal.get('Side')}",
            SERVER_ACCOUNT_ID,
            SERVER_BROKER
        )
        print(f"[SKIP] Strategy not found: {strategy_name} - {signal.get("SymbolId")} - {signal.get("Side")}")
        return
    
    # --- Get Server IP ---
    try:
        myip = get_public_ip()
    except Exception as e:
        send_alert(
            ALERT_PREFIX + f"IP fetch failed: {e} - {strategy_name} - {signal.get('SymbolId')} - {signal.get('Side')}",
            SERVER_ACCOUNT_ID,
            SERVER_BROKER
        )
        print(f"[ERROR] IP fetch failed: {e} - {strategy_name} - {signal.get("SymbolId")} - {signal.get("Side")}")
        return

    # --- Find matching strategy entry with matching IP from server_mapping ---
    final_user = None
    for _, row in trigger_strategy.iterrows():
        account_id = row.get("account_id")
        broker = row.get("broker")
        
        # Get IP from server_mapping
        ip_from_server = get_ip_from_server_mapping(account_id, broker)
        
        if ip_from_server == myip:
            final_user = row
            break
    
    if final_user is None:
        send_alert(
            ALERT_PREFIX + f"No mapping for IP: {myip} | Strategy: {strategy_name} - {signal.get('SymbolId')} - {signal.get('Side')}",
            SERVER_ACCOUNT_ID,
            SERVER_BROKER
        )
        print(f"[SKIP] No mapping for IP: {myip} | Strategy: {strategy_name} - {signal.get('SymbolId')} - {signal.get('Side')}")
        return

    # --- Get User ---
    qty = final_user.get("qty")
    userid = final_user.get("account_id")
    broker = final_user.get("broker")

    # --- Validate this order is for the configured server account ---
    if userid != SERVER_ACCOUNT_ID or broker != SERVER_BROKER:
        send_alert(
            ALERT_PREFIX + f"Order for different account. Expected {SERVER_BROKER}-{SERVER_ACCOUNT_ID}, got {broker}-{userid} - Strategy: {strategy_name} - {signal.get('SymbolId')} - {signal.get('Side')}",
            SERVER_ACCOUNT_ID,
            SERVER_BROKER
        )
        print(f"[SKIP] Order for different account: {broker}-{userid} != {SERVER_BROKER}-{SERVER_ACCOUNT_ID}")
        return

    if qty is None:
        send_alert(
            ALERT_PREFIX + f"Qty missing for {broker}-{userid} - {strategy_name} - {signal.get('SymbolId')} - {signal.get('Side')}",
            userid,
            broker
        )
        print(f"[SKIP] Qty missing for {broker}-{userid} - {strategy_name} - {signal.get("SymbolId")} - {signal.get("Side")}")
        return

    # --- Get mapped IP from server_mapping ---
    mapped_ip = get_ip_from_server_mapping(userid, broker)
    
    row_data = {
        "date": received_timestamp.strftime("%Y-%m-%d"),
        "Exc": signal.get("Exc"),
        "SymbolId": signal.get("SymbolId"),
        "Symbol": signal.get("Symbol"),
        "Side": signal.get("Side"),
        "OrderType": signal.get("OrderType"),
        "ProductType": signal.get("ProductType"),
        "qty": signal.get("qty"),
        "Price": signal.get("Price", 0),
        "CallBy": signal.get("CallBy", "PythonAlgo"),
        "PlaceOrder": signal.get("PlaceOrder", True),
        "StrategyName": signal.get("StrategyName", ""),
        "PClose": signal.get("PClose", 0),
        "InstrumentType": signal.get("InstrumentType", ""),
        "OrderTag": signal.get("OrderTag", ""),
        "broker": broker,
        "UserTriggered": userid,
        "MapedIp": mapped_ip,
        "SourceIp": signal.get("SourceIp", ""),
        "status": "RECEIVED",
        "timestamp": signal.get("timestamp"),
        "issquareoff": signal.get("issquareoff"),
        "message": "",
    }
    
    
    
    try:
        qty = int(qty)
    except Exception as e:
        send_alert(
            ALERT_PREFIX + f"Invalid qty for {broker}-{userid}: {qty} - {strategy_name} - {signal.get('SymbolId')} - {signal.get('Side')}",
            userid,
            broker
        )
        print(f"[ERROR] Invalid qty for {broker}-{userid}: {qty} - {strategy_name} - {signal.get("SymbolId")} - {signal.get("Side")}")
        return

    if qty <= 0:
        send_alert(
            ALERT_PREFIX + f"Qty=0 for {broker}-{userid} - {strategy_name} - {signal.get('SymbolId')} - {signal.get('Side')}",
            userid,
            broker
        )
        print(f"[SKIP] Qty=0 for {broker}-{userid} - {strategy_name} - {signal.get("SymbolId")} - {signal.get("Side")}")
        return

    # --- Transaction mapping ---
    try:
        ttype, position_type = reverse_jarvis_ttype(signal.get("Side"))
    except Exception as e:
        send_alert(
            ALERT_PREFIX + f"Invalid Side: {signal.get('Side')} - {strategy_name} - {signal.get('SymbolId')} - {signal.get('Side')}",
            userid,
            broker
        )
        print(f"[ERROR] Invalid Side: {signal.get('Side')} - {strategy_name} - {signal.get("SymbolId")} - {signal.get("Side")}")
        return

    # ----- posotions type validate -----
    if signal.get("issquareoff",False):
        position_type == "SQOFF"
    # --- Place order ---
    
    response = OrderManager._place_order(
        userid,
        signal.get("SymbolId"),
        signal.get("Exc"),
        qty,
        signal.get("ProductType"),
        signal.get("OrderType"),
        ttype,
        position_type,
        price=signal.get("Price", 0),
        tag=signal.get("OrderTag", ""),
        strategy_name=strategy_name,
    )

    print(
        f"[ORDER] {received_timestamp} | {broker}-{userid} | ",
        response
    )

    # --- Write CSV ---
    strategy_key = strategy_name if strategy_name else "all"
    write_order_to_csv(row_data, strategy_key)
    
    
# =====================================================
# KAFKA CONSUMER
# =====================================================
def start_consumer():
    """Consume messages from Kafka and process each as an order signal."""
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='latest',
        enable_auto_commit=True,
    )

    print("=" * 60)
    print(f"🚀 Order Server (Kafka Consumer) started")
    print(f"📡 Broker : {KAFKA_BROKER}")
    print(f"📌 Topic  : {TOPIC}")
    print(f"👥 Group  : {GROUP_ID}")
    print("=" * 60)

    for message in consumer:
        signal = message.value
        try:
            process_signal(signal)
        except Exception as e:
            print(f"[ERROR] Exception while processing signal: {e}")
            traceback.print_exc()
            time.sleep(1)


# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    # Start heartbeat in a background daemon thread (interval in minutes)
    hb_thread = threading.Thread(target=heartbeat_sender, args=(1,), daemon=True)
    hb_thread.start()

    start_consumer()
