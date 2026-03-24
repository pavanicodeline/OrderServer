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
from datetime import datetime
from pathlib import Path
from kafka import KafkaConsumer
from OrderExecutor_kafka import OrderDispatcher
import redis
import requests

# =====================================================
# CONFIG
# =====================================================
ORDERS_DIR = Path(__file__).parent / "orders"

VALID_SIGNATURE = "JarvisAlgo@123"

KAFKA_BROKER = "51.20.76.226:9092"
TOPIC = "trading-signals"
GROUP_ID = "order_server_group"

# =====================================================
# REDIS
# =====================================================
redis_conn_str = redis.Redis(
    host='redis-19731.crce182.ap-south-1-1.ec2.cloud.redislabs.com',
    port=19731,
    decode_responses=True,
    username="default",
    password="xJOOwytWRYTeFZUCBXAg1CAdQDzdLWKG",
)


def read_redis(key):
    try:
        return json.loads(redis_conn_str.get(key))
    except Exception as ex:
        print("Got exceptions = ", ex)
        return {}


# =====================================================
# UTILITIES
# =====================================================
def get_public_ip():
    try:
        return requests.get("https://api.ipify.org").text
    except Exception as e:
        return str(e)


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


# =====================================================
# CSV HELPERS
# =====================================================
ORDER_PLACEMENT_FIELDS = [
    "date", "Exc", "SymbolId", "Symbol", "Side", "OrderType",
    "ProductType", "qty", "Price", "CallBy", "PlaceOrder",
    "StrategyName", "PClose", "InstrumentType", "OrderTag", "status", "message"
]


def ensure_directory():
    """Create orders directory if it doesn't exist."""
    ORDERS_DIR.mkdir(parents=True, exist_ok=True)


def get_order_csv_path(strategy_name: str = "all") -> Path:
    """Generate orders CSV filename as orders_{strategy}_{YYYY-MM-DD}.csv"""
    today = datetime.now().strftime("%Y-%m-%d")
    return ORDERS_DIR / f"orders_{strategy_name}_{today}.csv"


def write_order_to_csv(row_data: dict, strategy_name: str = "all"):
    """
    Append an order placement row to the appropriate CSV file.
    Creates the file with headers if it doesn't exist.
    """
    ensure_directory()
    csv_path = get_order_csv_path(strategy_name)
    file_exists = csv_path.exists()

    with open(csv_path, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ORDER_PLACEMENT_FIELDS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerow(row_data)


# =====================================================
# STARTUP — load mappings & init OrderDispatcher
# =====================================================
print(f"Public IP: {get_public_ip()}")

strategy_mapping_key = "users:strategy:map"
server_mapping_key = "users:server:map"
strategy_mapping = read_redis(strategy_mapping_key)
server_mapping = read_redis(server_mapping_key)

OrderManager = OrderDispatcher("logged_users", "users_details", "OrderServer", "redis")


# =====================================================
# SIGNAL PROCESSING (replaces FastAPI /place-order)
# =====================================================
def process_signal(signal: dict):
    """
    Process an incoming Kafka signal the same way the old
    POST /place-order endpoint did.
    """
    print(f"\n[SIGNAL] Received: {signal}")

    # --- Required field check ---
    required = ["Exc", "SymbolId", "Symbol", "Side", "OrderType",
                 "ProductType", "qty", "StrategyName", "Signature"]
    missing = [f for f in required if f not in signal]
    if missing:
        print(f"[SKIP] Missing fields: {missing}")
        return

    # --- Signature validation ---
    if signal.get("Signature") != VALID_SIGNATURE:
        print("[SKIP] Invalid Signature")
        return

    # --- PlaceOrder flag ---
    if not signal.get("PlaceOrder", True):
        print("[SKIP] PlaceOrder is False — order not placed")
        return

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Build row for CSV
    row_data = {
        "date": timestamp,
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
        "status": "RECEIVED",
        "message": "",
    }

    # --- Strategy lookup ---
    strategy_name = signal.get("StrategyName", "")
    trigger_strategy = strategy_mapping.get(strategy_name)
    if not trigger_strategy:
        print(f"[WARN] Strategy not found: {strategy_name}")
        return

    print("trigger_strategy =", trigger_strategy)

    # --- Symbol lookup ---
    symbol = signal.get("Symbol")
    trigger_symbol_strategy = trigger_strategy.get(symbol)
    if not trigger_symbol_strategy:
        print(f"[WARN] Symbol not mapped in strategy: {symbol}")
        return

    print("trigger_symbol_strategy =", trigger_symbol_strategy)

    # --- Get IP ---
    try:
        myip = get_public_ip()
    except Exception as e:
        print(f"[ERROR] Failed to get IP: {e}")
        return

    print("myip =", myip)

    # --- Get User ---
    users = get_users_by_ip(server_mapping, myip)
    if not users:
        print(f"[WARN] No user mapped for IP: {myip}")
        return

    userid = users[0]
    print("userid =", userid)

    # --- Get Quantity ---
    qty = trigger_symbol_strategy.get(userid)
    if qty is None:
        print(f"[WARN] No qty configured for user {userid}")
        return

    try:
        qty = int(qty)
    except Exception:
        print(f"[ERROR] Invalid qty for user {userid}: {qty}")
        return

    if qty <= 0:
        print(f"[WARN] Qty is 0 for user {userid}")
        return

    print("qty =", qty)

    # --- Transaction mapping ---
    try:
        ttype, position_type = reverse_jarvis_ttype(signal.get("Side"))
    except Exception as e:
        print(f"[ERROR] Invalid Side: {signal.get('Side')}")
        return

    print("ttype, position_type =", ttype, position_type)

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
    print(f"[ORDER] {timestamp} | {signal.get('Side')} {qty} x {symbol} @ {signal.get('Price', 0)} | Strategy: {strategy_name}")
    print("order Placed =", response)

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
            time.sleep(1)


# =====================================================
# MAIN
# =====================================================
if __name__ == "__main__":
    start_consumer()
