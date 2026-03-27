import pandas as pd,json,dill,requests
from io import StringIO
from collections import namedtuple
from datetime import datetime
from typing import Literal
import math,os
from kafka import KafkaProducer
from api_helper import ShoonyaApiPy
import time
from filelock import FileLock
from pya3 import Instrument as pya3instrument
import getpass,urllib
import warnings,logging,sys
import uuid
import inspect
import httpx
from fyers_apiv3 import fyersModel
from pya3 import Aliceblue,TransactionType,ProductType,OrderType
import upstox_client
from upstox_client.rest import ApiException
from kiteconnect import KiteConnect
from fivepaisa import FivepaisaBroker 
from dhanhq import dhanhq 
from functools import lru_cache
import numpy as np
from TradeMaster.TradeSync import TransactionType as trade_ttype,ProductType as trade_prod_type,OrderComplexity as trade_oComplexity,OrderSource as trade_Osource,OrderType as trade_Otype,TradeHub
# from Connect import XTSConnect
from SmartApi.smartExceptions import SmartAPIException
from SmartApi import SmartConnect
import redis
# from Connect import XTSConnect
warnings.filterwarnings('ignore')
from findoc import FindocAPI
# import logging
# logging.basicConfig(level=logging.DEBUG)
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from threading import Lock
import threading
import queue
from MOFSLOPENAPI import MOFSLOPENAPI
import base64
os.makedirs("ORDERS/LOCK",exist_ok=True)
os.makedirs("ORDERS/logs",exist_ok=True)
os.makedirs("ORDERS/OLD_RECORD",exist_ok=True)
os.makedirs("masters",exist_ok=True)

#  payload_send = {
#         "topic": notification_topic,
#         "account_id": account_id,
#         "broker": broker,
#         "symbol": symbol,
#         "qty": qty,
#         "exchange": exchange,
#         "product_type": product_type,
#         "transaction_type": transaction_type,
#         "order_status": order_status,
#         "reason_message": reason_message,
#         "strategy_name": strategy_name,
#     }

            

class OrderDispatcher:
    Instrument = namedtuple('Instrument', ['exchange', 'token', 'symbol', 'name', 'expiry', 'lot_size'])
    ALICE_CONTRACT_URLS = {
    "NSE": 'https://v2api.aliceblueonline.com/restpy/static/contract_master/NSE.csv',
    "BSE": 'https://v2api.aliceblueonline.com/restpy/static/contract_master/BSE.csv',
    "NFO": 'https://v2api.aliceblueonline.com/restpy/static/contract_master/NFO.csv',
    "BFO": 'https://v2api.aliceblueonline.com/restpy/static/contract_master/BFO.csv',
    "MCX": 'https://v2api.aliceblueonline.com/restpy/static/contract_master/MCX.csv',
    }
    UPSTOX_CONTRACT_URLS = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
    ZERODHA_CONTRACT_URLS = "https://api.kite.trade/instruments"
    ANGEL_CONTRACT_URLS = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"

    XTS_CONTRACT_URLS = {
    "NSE": 'https://techfanetechnologies.github.io/XTS_MasterInstruments/csv/NSECM.csv',
    "BSE": 'https://techfanetechnologies.github.io/XTS_MasterInstruments/csv/BSECM.csv',
    "NFO": 'https://techfanetechnologies.github.io/XTS_MasterInstruments/csv/NSEFO.csv',
    "BFO": 'https://techfanetechnologies.github.io/XTS_MasterInstruments/csv/BSEFO.csv',
    "MCX": 'https://techfanetechnologies.github.io/XTS_MasterInstruments/csv/MCXFO.csv',
    }
        

    FYERS_CONTRACT_URLS = {
        "NSE": "https://public.fyers.in/sym_details/NSE_CM.csv",
        "BSE": "https://public.fyers.in/sym_details/BSE_CM.csv",
        "NFO": "https://public.fyers.in/sym_details/NSE_FO.csv",
        "BFO": "https://public.fyers.in/sym_details/BSE_FO.csv",
        "MCX": "https://public.fyers.in/sym_details/MCX_COM.csv",
    }
    upstock_exchange_map ={
            "NFO":"NSE_FO",
            "BFO":"BSE_FO",
            "MCX":"MCX_FO",
            "BSE":"BSE_EQ",
            "NSE":"NSE_EQ",
        }

  
    def decode(self,val):
        if isinstance(val, str) and val.startswith("B64:"):
            try:
                return val[4:]
            except Exception:
                return val
        return val
        
    def get_redis_files(self,logged_users_path,users_details_path):
        try:
            logged_users = self.redis_conn_str.get(self.redis_folder_key+logged_users_path)
            logged_users = pd.DataFrame(json.loads(logged_users))
            # logged_users.to_csv("logged_users.csv",index=False)
            # exit(0)
            logged_users["object"] = logged_users["object"].apply(self.decode)
        except Exception as ex:
            print("Got Exceptoion while fetching logged users .. ",ex)
            logged_users  = pd.DataFrame(columns=["broker","id","object","session_token","type","status"])
            
        try:
            users_details = self.redis_conn_str.get(self.redis_folder_key+users_details_path)
            users_details = pd.DataFrame(json.loads(users_details))
        except Exception as ex:
            print("Got Exceptoion while fetching users details .. ",ex)
            users_details  = pd.DataFrame(columns=["account_id","holdername","registered_no","api_key","api_secret","app_id","totp_key","password","redirect_url","account_type","broker","appsource","mpin"])
        
        return logged_users,users_details
    
    
    def __init__(self,logged_users_path,users_details_path,strategy_name = None,file_type:Literal["redis","normal"] = "normal"):
        """_summary_

        Args:
            logged_users_path (str): path to the file.
            users_details_path (str): path to the file.
            strategy_name (str): Name of the strategy [eg: nifty_straddle and should be unique].
        File_Formats:
        
            -=*logged_users_file*=-
            broker,id,object,session_token,type
            fyers,YJ06745,,eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9....,normal
            alice,1805656,b'\x80\....x03ub.',eyJhbGciO.....,normal
            
            -=*users_details_file*=-
            account_id,holdername,registered_no,api_key,api_secret,app_id,totp_key,password,redirect_url,account_type,broker
            YJ06745,Sai Krishna,700..JS7..,,3OIF567C-100,46JA..,2065,https://www.go..,normal,fyers
            1805656,Krishna,70..,LoeTr..,,1805656,,S@a..,https://www.go..,normal,alice
        """
        # ------*=csvs & df=*--------
        self.ltp_mode = "RD" # DF or RD
        # Threading locks for shared state protection
        self.merged_df_lock = Lock()  # Lock for merged_df and master dicts
        self.zerodha_master_lock = Lock()  # Lock for zerodha_master initialization
        self.alice_master_lock = Lock()  # Lock for alice_master initialization
        self.fyers_master_lock = Lock()  # Lock for fyers_master initialization
        self.upstock_master_lock = Lock()  # Lock for upstock_master initialization
        self.angel_master_lock = Lock()  # Lock for angel_master initialization
        # --------*=notification*=--------
        self.notification_webhook_url = "https://kellan-unmilitary-broodingly.ngrok-free.dev/webhook"
        # self.notification_webhook_url = "https://formatively-orthotropic-tonita.ngrok-free.dev/webhook"
        # self.notification_webhook_url = "http://192.168.29.111:8001/webhook"
        # self.notification_webhook_url = "http://localhost:8001/webhook"
        self.notification_topic = "ordernotification"

        self.producer = KafkaProducer(
            bootstrap_servers="195.250.30.177:9092",
            key_serializer=lambda k: k.encode('utf-8'),
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        # --------*=filter_recordscols*=--------
        self.filter_recordscols = [
            "strategy","broker","user_id",
            "exchange_token","product_type","order_status"
        ]
        self.redis_folder_key = "users:"
        self.redis_conn_str =   redis.Redis(
                host='redis-19731.crce182.ap-south-1-1.ec2.cloud.redislabs.com',
                port=19731,
                decode_responses=True,
                username="default",
                password="xJOOwytWRYTeFZUCBXAg1CAdQDzdLWKG",
            ) 
        
        
        # logged users , userdetails & merged users = [loggedusers + userdetails]
        if file_type == "normal":
            self.logged_users = pd.read_csv(logged_users_path,index_col=False) 
            self.users_details = pd.read_csv(users_details_path,index_col=False)
        else:
            self.logged_users,self.users_details = self.get_redis_files(logged_users_path,users_details_path)
            
        # self.logged_users.to_csv("self.logged_users.csv",index=False)
        # exit(0)    
        self.users_details.rename(columns={"broker":"broker_user_det"},inplace=True)
        self.users_details = self.users_details[~self.users_details["account_type"].isin(["market"])]
        self.logged_users = self.logged_users[~self.logged_users["type"].isin(["market"])]
        self.logged_users["id"]=self.logged_users["id"].astype('str').astype('str').replace(".0","")
        self.users_details["account_id"]=self.users_details["account_id"].astype('str').replace(".0","")
        # self.merged_df = pd.merge(self.logged_users,self.users_details,how="left",left_on="id",right_on="account_id")
        self.merged_df = pd.merge(
            self.logged_users,
            self.users_details,
            how="left",
            left_on=["id", "broker"],
            right_on=["account_id", "broker_user_det"]
        )        
        # ----------- calllers details -------------
        frame = inspect.stack()[1]
        self.calling_script = os.path.basename(frame.filename)
        self.calling_module = os.path.splitext(self.calling_script)[0]
        # ------*=class vars=*--------
        
        self.working_dir = os.getcwd()
        self.contract_master_dir = os.path.join(self.working_dir,"masters")
        self._split_size = 900 # split at 900 for options nfo,bfo only
        self.maintain_records = True # will store order records for future refference
        self.max_workers= 20 # total order proccessed at a time
        
        # Initialize instance identifier: PID (unique per process instance)
        self.process_id = os.getpid()
        self.instance_id = None
        
        
        # -------=* Datafeed paths *=-------
        self.datafeed_path = {
            "MIDCPNIFTY":f"/Users/harindersahu/Desktop/Sai_krishna/mytasks/Datafeeds/feed_files/NBSE/midniftydata{datetime.now().date()}.txt",
            "NIFTY":f"/Users/harindersahu/Desktop/Sai_krishna/mytasks/Datafeeds/feed_files/NBSE/niftydata{datetime.now().date()}.txt",
            "BANKNIFTY":f"/Users/harindersahu/Desktop/Sai_krishna/mytasks/Datafeeds/feed_files/NBSE/midniftydata{datetime.now().date()}.txt",
            "MCX":f"/Users/harindersahu/Desktop/Sai_krishna/mytasks/Datafeeds/feed_files/MCX/futures_data{datetime.now().date()}.txt",
            "STOCKS":f"/Users/harindersahu/Desktop/Sai_krishna/mytasks/Datafeeds/feed_files/MCX/futures_data{datetime.now().date()}.txt",
            "STOCKS_OPTIONS":f"/Users/harindersahu/Desktop/Sai_krishna/mytasks/Datafeeds/feed_files/MCX/futures_data{datetime.now().date()}.txt",
            "SENSEX":f"/Users/harindersahu/Desktop/Sai_krishna/mytasks/Datafeeds/scripts/Angel/DF/angel_sensexdata{datetime.now().date()}.txt",
            }
        
        # -------=* Datafeed cols *=-------
        self.datafeed_cols = ['symbol','ltp','timestamp']
        
        # -------=* Order vars *=-------
        self.limit_retry = 10 # retries for no. of time untill limit order complete else cancels after maximum tries
        self.exit_limit_retry = 50 # retries for no. of time untill limit order complete else cancels after maximum tries
        self.modify_after_sleep = 0.1
        self.logs_dir = os.path.join(self.working_dir,"ORDERS","logs")
        
        # -------=* order status check  *=-------
        self.order_status_check_interval = 0.2
        # -------=* Logger vars *=-------
        self.executor_log_file = None  # Will be set in setup_logger
        self.enable_logging = True   
        # -------=* retry Order vars *=-------
        self.order_retry = 3 # retry if order id is not recived
        
        # -------=* Master vars *=-------
        self._date = str(datetime.now().date())
        
        # -------=* Limit Order vars *=-------
        self.limit_price_diff_percent = 15
        self.make_limit_price_diff = 0.01 
        self.error_buffer = 1
        self._record_queue = queue.Queue()
        self._record_worker = threading.Thread(target=self._record_processor, daemon=True)
        self._record_worker.start()
        
        # Message queue for async WhatsApp/Telegram notifications
        self._message_queue = queue.Queue()
        self._message_worker = threading.Thread(target=self._message_processor, daemon=True)
        self._message_worker.start()
        if strategy_name is not None:
            self.maintain_records = True
            self.strategy_name = strategy_name
            self.instance_id = f"{self.strategy_name}"  # Unique identifier per process
            self.Old_file_path = os.path.join(self.working_dir,"ORDERS","OLD_RECORD",f"{self.strategy_name}{datetime.now().date()}.csv")
            self.file_path = os.path.join(self.working_dir,"ORDERS",f"{self.strategy_name}.csv")
            self.lock_path = os.path.join(self.working_dir,"ORDERS","LOCK",f"{self.strategy_name}.lock")
            # self.telegram_chatid = "-5157337118"
            self.telegram_chatid = "-5645485118"
            self.whatsapp_number = "7000863437,9666206830"
            self.username = getpass.getuser()
            if not os.path.exists(self.file_path):
                self.safe_append("timestamp,strategy,broker,user_id,exchange,exchange_token,symbol,trading_symbol,position_type,product_type,transaction_type,order_status,order_id,total_quantity,success_quantity,order_message,tags")
        # Persistent executor for threaded placement and split orders
        self.executor = ThreadPoolExecutor(max_workers=5)
        
        # Cache for deserialized broker instances
        self._broker_instances = {}
        self._broker_instances_lock = Lock()
        
        # Optimized master data (dicts for O(1) lookup)
        self._optimized_masters = {
            "alice": {},
            "fyers": {},
            "zerodha": pd.DataFrame(),
            "upstock": pd.DataFrame(),
            "xts": pd.DataFrame(),
            "angel": pd.DataFrame(),
            "finvasia": pd.DataFrame(),
            "dhan": {},
            "5paisa": {}
        }
        self._load_and_prepare_masters()
        # Queue for asynchronous record keeping

        if self.enable_logging:
            self.setup_logger()    
    
    modify_price_size = {"NIFTY":10,"BANKNIFTY":10,"FINNIFTY":10,"MIDCPNIFTY":10,"MCX":10,"STOCKS":10,"STOCKS_OPTIONS":10,"SENSEX":10}
        # merged_df.columns
    # Set up logging once globally
    def setup_logger(self):
        """
        Setup logging with per-instance log files using Process ID.
        - Each process instance gets its own log file (PID-based)
        - Same process restarted will append to same log file
        - Format: Order_executor_PID<process_id>.log
        - Or if strategy_name: Order_executor_<strategy>_PID<process_id>.log
        """
        # Create logs directory if it doesn't exist
        
        # Generate unique log file name using PID
        if self.instance_id:
            self.executor_log_file = os.path.join(self.logs_dir, f"Order_executor_{self.instance_id}_{self._date}.log")
        else:
            self.executor_log_file = os.path.join(self.logs_dir, f"Order_executor_by_{self.calling_module}_{self._date}.log")
        if self.executor_log_file not in ["", None]:
            handler = [
                logging.FileHandler(self.executor_log_file, mode='a'),
                # logging.StreamHandler(sys.stdout)  # console output
            ]
            # Note: Be careful with redirecting stdout/stderr as it can break other output
            # Only do this if you absolutely need it
            # sys.stdout = open(self.executor_log_file, "a")
            # sys.stderr = sys.stdout
        else:
            handler = [
                logging.StreamHandler(sys.stdout)  # console output 
            ]
        
        # Remove any existing handlers to avoid duplicates
        for h in logging.root.handlers[:]:
            logging.root.removeHandler(h)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - [%(process)d] - %(message)s',
            handlers=handler,
            force=True  # Force reconfiguration
        )
        
        logging.info(f"Logger initialized for instance: {self.instance_id or 'Default'} (PID: {self.process_id})")

    

    def write_logs(self,log_type: Literal["info", "warning", "error"], message: str):
        if self.enable_logging:
            if log_type == "info":
                logging.info(message)
            elif log_type == "warning":
                logging.warning(message)
            elif log_type == "error":
                logging.error(message)
            else:
                logging.debug(f"Unknown log type '{log_type}': {message}")

     

    def flush_records(self):
        if os.path.exists(self.file_path):
            if os.path.exists(self.Old_file_path):
                os.remove(self.Old_file_path)
            os.rename(self.file_path,self.Old_file_path)
            self.safe_append("timestamp,strategy,broker,user_id,exchange,exchange_token,symbol,trading_symbol,position_type,product_type,transaction_type,order_status,order_id,total_quantity,success_quantity,order_message,tags")

    def safe_append(self, text):
        # Asynchronously put record into queue
        self._record_queue.put(text)

    def get_notification_payload(self,
        notification_topic,
        account_id,
        broker,
        symbol,
        qty,
        exchange,
        product_type,
        transaction_type,
        order_status,
        reason_message,
        strategy_name):
        payload =  {
                "topic": notification_topic,
                "account_id": account_id,
                "broker": broker,
                "symbol": symbol,
                "strategy_name": strategy_name,
                "qty": qty,
                "exchange": exchange,
                "product_type": product_type,
                "transaction_type": transaction_type,
                "order_status": order_status,
                "reason_message": reason_message,
            }
        self._send_message("SERVER_NOTIFICATION",payload)
        
        
    def _record_processor(self):
        """Background thread to process record writing to disk"""
        while True:
            text = self._record_queue.get()
            if text is None: break
            
            try:
                # Still use lock for cross-process safety if needed, 
                # but now it's in a background thread
                lock = FileLock(self.lock_path)
                with lock:
                    with open(self.file_path, "a", encoding="utf-8") as f:
                        f.write(text + "\n")
            except Exception as e:
                # Fallback to direct logging if record writing fails
                if hasattr(self, 'write_logs'):
                    self.write_logs("error", f"Asynch record writing failed: {str(e)}")
            finally:
                self._record_queue.task_done()
        
    def alice_instrument(self,contract:dict):

        return pya3instrument(contract.get('Exch',''), np.int64(contract.get('Token','0')),
                                        contract.get('Symbol',''), contract.get('Trading Symbol',''),
                                        contract.get('Expiry Date',''), np.int64(contract.get('Lot Size','')))
        
        
    def internal_ord_res (self,g_response,user_id,or_id,status,placed_qty,message,eerror):
        g_response[user_id]['result'].append(
                    {
                        "order_id":or_id,
                        "placed_qty":placed_qty,
                        "status":status,
                        "message":message,
                        "error":eerror
                    }
                )
        return g_response    
    
    def _send_message(
        self,
        messageType: Literal["ERROR", "UPDATE", "INFO", "ALERT", "SERVER_NOTIFICATION"],
        messageDetails
    ):
        """Non-blocking enqueue with overflow protection."""

        try:
            # Do NOT block order execution
            self._message_queue.put_nowait((messageType.upper(), messageDetails))
        except queue.Full:
            # Drop low priority messages if overloaded
            if messageType.upper() in ("INFO", "UPDATE"):
                return
            else:
                # Critical messages can block briefly
                try:
                    self._message_queue.put((messageType.upper(), messageDetails), timeout=0.5)
                except queue.Full:
                    self.write_logs("error", "Message queue full — critical message dropped")

    def _message_processor(self):
        """Worker thread: consumes messages safely."""

        while True:
            try:
                item = self._message_queue.get(timeout=1)
            except queue.Empty:
                continue  # no message, keep loop alive

            if item is None:
                self._message_queue.task_done()
                break

            try:
                messageType, messageDetails = item

                if messageType == "SERVER_NOTIFICATION":
                    self.send_order_notification(messageDetails)
                else:
                    self._send_message_worker(messageType, messageDetails)

            except Exception as ex:
                self.write_logs(
                    "error",
                    f"[_message_processor] Error processing message: {str(ex)}"
                )

            finally:
                self._message_queue.task_done()

    def _send_message_worker(self, messageType: Literal["ERROR", "UPDATE", "INFO", "ALERT"], messageDetails):
        """Actual HTTP sender — called by _message_processor worker thread."""
        # whatsapp 
        try:
            url = "https://assemble.jarvisalgo.in/api/WhatsApp/SendSystemNotification"
            payload = {
            "mobileNumbers": self.whatsapp_number,
            "recipientName": "OrderExecutor",
            "systemName": self.username,
            "messageType": messageType,
            "referenceId": self.strategy_name,
            "timestamp": f"{datetime.now()}",
            "messageDetails": messageDetails,
            "currentStatus": "NA"
            }
            res = requests.post(url,json=(payload))
            # self.write_logs("info", f"[_send_message] WhatsApp notification sent. Response: {res}")
        except Exception as ex:
            self.write_logs("error", f"[_send_message] WhatsApp send failed: {str(ex)}")

        
        # telegram
        try:
            payload = {
                "chat_id": self.telegram_chatid,
                "text": (
                    f"<b>This is an automated system notification from {self.username}</b>\n\n"
                    f"<b>OrderExecutor</b>\n"
                    f"<b>{messageType}</b>\n\n"
                    f"<b>Reference ID:</b> {self.strategy_name}\n"
                    f"<b>Timestamp:</b> {datetime.now()}\n"
                    f"<b>Message Details:</b> {messageDetails}\n"
                    f"<b>Current Status:</b> NA"
                ),
                "parse_mode": "HTML"
            }
            url = f"https://api.telegram.org/bot8046020954:AAFi7hbrCIDIHUJpsx7IlAng4OPqmPeNBnU/sendMessage"
            
            
            # self.write_logs("info",f"[DEBUG][_send_message] telegram payload - {payload}")
            
            response = requests.post(url, data=payload)
            # self.write_logs("info", f"[_send_message] Telegram notification sent. Response: {response.text}")
        except Exception as ex:
            self.write_logs("error",f"[DEBUG][_send_message] telegram execption - {str(ex)}")
            
            
    def send_order_notification(self,payload):
        try:
            response = requests.post(
                self.notification_webhook_url,
                json=payload,
                timeout=5
            )
            self.write_logs("info",f"Fallback HTTP webhook sent {response.json()}")
        except Exception as e:
            self.write_logs("error",f"Webhook fallback failed: {str(e)}")

    def get_instrument_type(self,exchange,option_type):
        if exchange in ["MCX","NFO","BFO"]:
            return "OPT" if option_type in ["CE","PE"] else "FUT"
        else:
            return option_type

    def fetch_ltp_redis(self,instrument):
        instype=self.get_instrument_type(instrument['Exch'],instrument["Option Type"])
        key = f"LTP:{instrument['Exch']}:{instype}"
        try:
            ltp = json.loads(self.redis_conn_str.hget(key,instrument['Token']))
        except Exception as e:
            self.write_logs("error",f"[DEBUG][_fetch_ltp_redis] redis exception - {str(e)}")
            return 0,False
        fltp = ltp.get('ltp',None)
        if fltp is None:
            self.write_logs("error",f"[DEBUG][_fetch_ltp_redis] ltp not found in redis for {instrument['Token']}")
            return 0,False
        return float(fltp),True
        
    def fetch_ltp_datafeed(self,exchange,symbol,trading_symbol,instype):
        if exchange == "MCX":
            datafeed_key = "MCX" 
        elif exchange == "NSE":
            datafeed_key = "STOCKS" 
        elif instype == "OPTSTK":
            datafeed_key = "STOCKS_OPTIONS" 
        else:
            datafeed_key = symbol

        if datafeed_key != "OPTIONS_STOCK" :
            df = pd.read_csv(self.datafeed_path[datafeed_key],names=self.datafeed_cols)
        else:
            df = pd.read_csv(self.datafeed_path[datafeed_key],names=['symbol','ltp','volume','timestamp']).tail(100000)
            
        df = df[df["symbol"]==trading_symbol]
        if df.empty:
            return 0
        ltp = float(df["ltp"].iloc[-1])
        return ltp

    def fetch_ltp(self, instrument_data, exchange, symbol, trading_symbol, instype):
        """Unified LTP fetcher that routes based on self.ltp_mode.
        
        Args:
            instrument_data: dict with instrument details (used for Redis mode)
            exchange: exchange string (used for datafeed mode)
            symbol: symbol string (used for datafeed mode)
            trading_symbol: trading symbol string (used for datafeed mode)
            instype: instrument type string (used for datafeed mode)
        
        Returns:
            float: LTP value, 0 if not available
        """
        if self.ltp_mode == "RD":
            ltp, success = self.fetch_ltp_redis(instrument_data)
            if success:
                return ltp
            else:
                self.write_logs("warning", f"[fetch_ltp] Redis LTP failed for {trading_symbol}, returning 0")
                return 0
        else:  # DF mode (datafeed)
            return self.fetch_ltp_datafeed(exchange, symbol, trading_symbol, instype)
    
    
    def round_to_tick(self, price, tick_size=0.05):
        return round(round(price / tick_size) * tick_size, 2)
    def _call_suborder(self,func, base_args, qty):
        args = base_args.copy()
        args["quantity"] = qty
        return func(**args)
    
    def close_order_execptions(self,position_type,broker,account_id,message,orderdetialmessage):
        formatted_message = f"Closing Order failed for [{broker} : id - {account_id}] \n# Exception {message}\n order_detials : {orderdetialmessage}"  
        if position_type.upper() == "CLOSE":
            self._send_message("ALERT",formatted_message)
    def _get_broker_instance(self, user_data):
        # print("user_data == ",user_data)
        user_id = user_data["id"]
        broker = user_data["broker"].lower()
        
        with self._broker_instances_lock:
            if user_id in self._broker_instances:
                return self._broker_instances[broker][user_id]
                
        instance = None
        try:
            if broker == "alice":
                instance = dill.loads(eval(user_data["object"]))
            elif broker == "trade_master":
                instance = dill.loads(eval(user_data["object"]))
            elif broker == "fyers":
                instance = fyersModel.FyersModel(client_id=user_data["app_id"], token=user_data["session_token"])
            elif broker == "zerodha":
                instance = dill.loads(eval(user_data["object"]))
            elif broker == "upstock":
                instance = user_data["session_token"] # Just the token for upstock
            elif broker == "angel":
                instance = dill.loads(eval(user_data["object"]))
            elif broker == "finvasia":
                instance = ShoonyaApiPy()
                instance.set_session(userid=user_data["account_id"], password=user_data["password"], usertoken=user_data["session_token"])
            elif broker == "xts":
                instance = dill.loads(eval(user_data["object"]))
            elif broker == "5paisa":
                instance = dill.loads(eval(user_data["object"]))
                instance.session = httpx.Client(verify=False)
            elif broker == "dhan":
                instance = dill.loads(eval(user_data["object"]))
            elif broker == "findoc":
                instance = dill.loads(eval(user_data["object"]))
            elif broker == "mo":
                instance = dill.loads(eval(user_data["object"]))
        except Exception as ex:
            print(f"Failed to initialize broker instance {user_id} ",{str(ex)})
            self.write_logs("error", f"Failed to initialize broker instance for {user_id}: {str(ex)}")
            return None
            
        if instance:
            with self._broker_instances_lock:
                if broker not in self._broker_instances:
                    self._broker_instances[broker] = {}
                self._broker_instances[broker][user_id] = instance
        return instance
        
    def _load_and_prepare_masters(self, brokers=["alice","angel","zerodha","upstock","fyers"],force_download=False):
        self.status_file_contract_master = os.path.join(self.contract_master_dir,"status_file_contract_master.json")
        """
        Download CSV (if parquet not exists), save as parquet,
        load into DataFrame and build optimized lookup dict.
        """
        contracts_exist = False
        if os.path.exists(self.status_file_contract_master):
            with open(self.status_file_contract_master,"r") as f:
                data:list = json.load(f)
            contracts_exist = self._date not in data
            if contracts_exist:
                data.append(self._date)
            
        else:
            data = [self._date]
            contracts_exist = True
        if contracts_exist:
                with open(self.status_file_contract_master,"w") as f:
                    json.dump(data,f)

        for broker in brokers:
            if broker == "alice":
                with self.alice_master_lock:
                    for exchange, url in self.ALICE_CONTRACT_URLS.items():
                        pq_path = os.path.join(self.contract_master_dir,f"alice_{exchange}.parquet")
                        if contracts_exist or force_download or (not os.path.exists(pq_path)):
                            self.write_logs("info","downloading alice contracts")
                            df = pd.read_csv(StringIO(requests.get(url).text))
                            df.to_parquet(pq_path, index=False)
                        else:
                            self.write_logs("info","loading alice contracts")
                            df = pd.read_parquet(pq_path)
                        if exchange == "MCX":
                            master_df = df[df["Instrument Type"].isin(["FUTIDX","OPTFUT","FUTCOM"])].drop_duplicates(subset=["Token"], keep="first")
                        else:
                            master_df = df
                        master_df['index_Token'] = master_df['Token']
                        self._optimized_masters["alice"][exchange] = (
                                master_df.set_index("index_Token").to_dict("index")
                            )
                        
            elif broker == "fyers":
                with self.fyers_master_lock:
                    for exchange, url in self.FYERS_CONTRACT_URLS.items():
                        pq_path = os.path.join(self.contract_master_dir,f"fyers_{exchange}.parquet")
                        if contracts_exist or force_download or (not os.path.exists(pq_path)):
                            self.write_logs("info","downloading fyers contracts")
                            names=['Fytoken','SymbolDetails','ExchangeInstrumentType','MinimumLotSize','TickSize','ISIN','TradingSession','LastUpdateDate','Expirydate','SymbolTicker','Exchange','Segment','ScripCode','UnderlyingSymbol','UnderlyingScripCode','StrikePrice','OptionType','UnderlyingFyToken','ReservedColumn1','ReservedColumn2','ReservedColumn3']

                            df = pd.read_csv(StringIO(requests.get(url).text),names=names,index_col=False)
                            df.to_parquet(pq_path, index=False)
                        else:
                            self.write_logs("info","loading fyers contracts")
                            df = pd.read_parquet(pq_path)
                        if exchange == "MCX":
                            master_df = df[df["ExchangeInstrumentType"].isin([31,30,"30","31"])].drop_duplicates(subset=["ScripCode"], keep="first")
                        else:
                            master_df = df
                        master_df['index_Token'] = master_df['ScripCode'].astype("int64")
                        # print("master_df -- ",master_df)
                        self._optimized_masters["fyers"][exchange] = (
                                master_df.set_index("index_Token").to_dict("index")
                            )
                        # master_df.to_csv("master_df.csv",index=False)
                        # exit(0)
            elif broker == "angel":
                with self.angel_master_lock:
                    pq_path = os.path.join(self.contract_master_dir,"angel.parquet")

                    # print("df == ", pq_path)
                    if contracts_exist or force_download  or (not os.path.exists(pq_path)):
                        self.write_logs("info","downloading angel contracts")
                        df = pd.read_json(self.ANGEL_CONTRACT_URLS,orient="records")
                        df = df[df["exch_seg"].isin(["NSE","BSE","BFO","NFO","MCX"])]
                        df["token"] = df["token"].astype(int)
                        # print("df == ", pq_path)
                        df.to_parquet(pq_path, index=False)
                    else:
                        self.write_logs("info","loading angel contracts")
                        df = pd.read_parquet(pq_path)
                    # print("df == ", df)
                    df['index_Token'] = df['token']
                    self._optimized_masters["angel"] = (
                        df.groupby("exch_seg")
                        .apply(lambda x: x.set_index("index_Token").to_dict("index"))
                    )

            elif broker == "zerodha":
                with self.zerodha_master_lock:
                    pq_path = os.path.join(self.contract_master_dir,"zerodha.parquet")

                    if contracts_exist or force_download or (not os.path.exists(pq_path)):
                        self.write_logs("info","downloading zerodha contracts")
                        df = pd.read_csv(self.ZERODHA_CONTRACT_URLS)
                        df = df[df["segment"].isin(["BFO-FUT","BFO-OPT","NFO-FUT","NFO-OPT","MCX-FUT","MCX-OPT","NSE","BSE"])]
                        df.to_parquet(pq_path, index=False)
                    else:
                        self.write_logs("info","loading zerodha contracts")
                        df = pd.read_parquet(pq_path)
                    df['index_Token'] = df['exchange_token']
                    self._optimized_masters["zerodha"] = (
                        df.groupby("exchange")
                        .apply(lambda x: x.set_index("index_Token").to_dict("index"))
                    )

            elif broker == "upstock":
                with self.upstock_master_lock:
                    pq_path = os.path.join(self.contract_master_dir,"upstock.parquet")
                    if contracts_exist or force_download or (not os.path.exists(pq_path)):
                        df=pd.read_csv(self.UPSTOX_CONTRACT_URLS,index_col=False)
                        df.to_parquet(pq_path, index=False)
                    else:
                        df = pd.read_parquet(pq_path)
                    df['index_Token'] = df['exchange_token']
                    self._optimized_masters["upstock"] = (
                        df.groupby("exchange")
                        .apply(lambda x: x.set_index("index_Token").to_dict("index"))
                    )

    
    @lru_cache
    def _get_instrument_data(self, broker, exchange, token):
        # Specific implementation for Alice masters since it's used as the base
        token = int(token)
        if exchange not in self._optimized_masters[broker]:
            if broker == "alice":
                with self.alice_master_lock:
                    # if exchange not in self.alice_master:
                    #     url = self.ALICE_CONTRACT_URLS.get(exchange)
                    #     if not url: return None
                    #     df = pd.read_csv(StringIO(requests.get(url).text), index_col=False)
                
                    # Create optimized dict if not exists
                    if exchange not in self._optimized_masters["alice"]:
                        self._load_and_prepare_masters(["alice"],True)
                        
            elif broker == "fyers":
                # print("broker = ",broker)
                with self.fyers_master_lock:
                    # if exchange not in self.fyers_master:
                    #     url = self.FYERS_CONTRACT_URLS.get(exchange)
                    #     names=['Fytoken','SymbolDetails','ExchangeInstrumentType','MinimumLotSize','TickSize','ISIN','TradingSession','LastUpdateDate','Expirydate','SymbolTicker','Exchange','Segment','ScripCode','UnderlyingSymbol','UnderlyingScripCode','StrikePrice','OptionType','UnderlyingFyToken','ReservedColumn1','ReservedColumn2','ReservedColumn3']

                    #     if not url: return None
                    #     df = pd.read_csv(StringIO(requests.get(url).text), index_col=False,names=names)
                        # df.to_csv(f"{exchange}_df.csv",index=False)
                    # # Create optimized dict if not exists
                    if exchange not in self._optimized_masters["fyers"] and self._optimized_masters["fyers"].empty:
                        self._load_and_prepare_masters(["fyers"],True)
                        
            elif broker == "angel":
                with self.angel_master_lock:
                    if "angel" in self._optimized_masters and self._optimized_masters["angel"].empty:
                        self._load_and_prepare_masters(["angel"],True)
                        
            elif broker == "zerodha":
                with self.zerodha_master_lock:
                    # Create optimized dict if not exists
                    if "zerodha" in self._optimized_masters and self._optimized_masters["zerodha"].empty:
                        self._load_and_prepare_masters(["zerodha"],True)
                        
            elif broker == "upstock":
                with self.upstock_master_lock:
                    # Create optimized dict if not exists
                    if "upstock" in self._optimized_masters and self._optimized_masters["upstock"].empty:
                        self._load_and_prepare_masters(["upstock"],True)
                        
                # print("exchange = ",exchange)
                exchange = self.upstock_exchange_map.get(exchange, exchange)
        return self._optimized_masters[broker].get(exchange, {}).get(token)

    def _place_order(self, user_id: str, exchange_token: int, exchange: str, quantity: int, product: str, order_type: str, transaction_type: str, position_type: Literal["CLOSE", "OPEN"], validity: str = None, price: float = 0, trigger_price: float = 0, disclosed_quantity: int = 0, is_amo: bool = False, api_version: str = 'v2', variety: str = "regular", tag: str = "", strategy_name: str = None,instrument_data=None):
        start_time = time.time()
        # self.write_logs("info", f"[_place_order] Initiating order for {user_id} ({broker}), Token: {exchange_token}, Qty: {quantity}, Type: {order_type}, Side: {transaction_type}")
        if not strategy_name:
            strategy_name = self.strategy_name
        print("strategy_name = ",strategy_name)    
        print("self.strategy_name = ",self.strategy_name)    
        _position_type = position_type.upper()
        exchange = exchange.upper()
        product = product.upper()
        order_type = order_type.upper()
        transaction_type = transaction_type.upper()
        tag = strategy_name[0]+ tag[:18] if tag else str(datetime.now())[:18]
        
        response = {user_id: {"result": [], "summary": {"total_qty": quantity, "traded_qty": 0, "failed_qty": 0}}}
        formated_qty = -quantity if transaction_type == "SELL" else quantity

        if not instrument_data:
            instrument_item = self._get_instrument_data("alice", exchange, exchange_token)
        else:
            instrument_item = instrument_data

        if not instrument_item:
            response_message = f"invalid_exchange_token {exchange_token}"
            self.write_logs("error", f"[_place_order] {response_message}")
            record = f"{datetime.now()},{strategy_name},{None},{user_id},{exchange},{exchange_token},,,{position_type.upper()},{product},{transaction_type},{False},{None},{formated_qty},{0},{response_message},{tag}"
            self.safe_append(record)
            return self.internal_ord_res(response, user_id, None, "FAILED", 0, response_message, None)

        Symbol = instrument_item["Symbol"]
        TradingSymbol = instrument_item["Trading Symbol"]
        tick_size = instrument_item["Tick Size"]
        lot_size = int(instrument_item["Lot Size"])

        with self.merged_df_lock:
            user_data = self.merged_df[(self.merged_df["id"] == str(user_id)) & ~(self.merged_df["broker"].str.lower().isin(["alice"]))].copy(deep=True)

        if user_data.empty:
            response_message = f"invalid_user_id {user_id}"
            self.write_logs("error", f"[_place_order] {response_message}")
            record = f"{datetime.now()},{strategy_name},{None},{user_id},{exchange},{exchange_token},{Symbol},{TradingSymbol},{position_type.upper()},{product},{transaction_type},{False},{None},{formated_qty},{0},{response_message},{tag}"
            self.safe_append(record)
            return self.internal_ord_res(response, user_id, None, "FAILED", 0, response_message, None)

        user_data = user_data.iloc[0]
        if position_type.upper() in ["OPEN", "CLOSE", "SQOFF"]:
            _position_type = "CLOSE" if position_type.upper() == "SQOFF" else position_type.upper()
        else:
            response_message = f"invalid_position_type {position_type}"
            self.write_logs("error", f"[_place_order] {response_message}")
            record = f"{datetime.now()},{strategy_name},{user_data['broker']},{user_id},{exchange},{exchange_token},,,{position_type.upper()},{product},{transaction_type},{False},{None},{formated_qty},{0},{response_message},{tag}"
            self.safe_append(record)
            return self.internal_ord_res(response, user_id, None, "FAILED", 0, response_message, None)

        # self.write_logs("info", f"[_place_order] Processing {user_id} @ {TradingSymbol}")
        split_size = self.get_split_size(exchange, Symbol, lot_size)
        
        # Broker-specific adjustments
        if (order_type in ["LIMIT", "ADJUST"]) and (Symbol not in ["MIDCPNIFTY"]) and (user_data["broker"].upper() in ["XTS"]):
            self.write_logs("info", f"[_place_order] Broker {user_data['broker']} does not support LIMIT/ADJUST directly in this flow, switching to MARKET")
            order_type = "MARKET"
            price = 0
            
        is_limit_order = (order_type == "LIMIT")
        is_adjust_order = (order_type == "ADJUST")

        if (is_limit_order or is_adjust_order) and ((price is None) or (price == 0)):
            
            price = self.fetch_ltp(instrument_item, exchange, Symbol, TradingSymbol, instrument_item.get("Instrument Type",""))
            # self.write_logs("info","price == ",price)
            if price > 0:
                price = self.calculate_price(price, transaction_type)
                price = self.round_to_tick(price, tick_size)
                # self.write_logs("info", f"[_place_order] Fetched price @ {price} for {TradingSymbol}")
            else:
                self.write_logs("error", f"[_place_order] LTP is 0 or not available for {TradingSymbol}")
                if is_adjust_order:
                    order_type = "MARKET"
                    # self.write_logs("info", f"[_place_order] Adjusted orderType to MARKET due to missing LTP")
                else:
                    response_message = f"LTP not available for LIMIT order {TradingSymbol}"
                    self._send_message("ALERT", f"Failed to place order as ltp not found\nAccount [{user_data['broker']} : id - {user_id}]\n(Symbol - {TradingSymbol} ** Side - {transaction_type} ** Ordertype - {order_type})")
                    self.get_notification_payload(self.notification_topic,user_id,user_data['broker'],TradingSymbol,quantity,exchange,product,transaction_type,"FAILED","Failed to place order as ltp not found",self.strategy_name)
                    return self.internal_ord_res(response, user_id, None, "FAILED", 0, response_message, None)

        if pd.isna(user_data["session_token"]):
            response_message = f"session_token_not_generated {user_id}"
            self.write_logs("error", f"[_place_order] {response_message}")
            record = f"{datetime.now()},{strategy_name},{user_data['broker']},{user_id},{exchange},{exchange_token},{Symbol},{TradingSymbol},{position_type.upper()},{product},{transaction_type},{False},{None},{formated_qty},{0},{response_message},{tag}"
            self.safe_append(record)
            self.get_notification_payload(self.notification_topic,user_id,user_data['broker'],TradingSymbol,quantity,exchange,product,transaction_type,"FAILED","Failed to place order as session token not generated",self.strategy_name)

            return self.internal_ord_res(response, user_id, None, "FAILED", 0, response_message, None)

        # Position check for CLOSE orders
        if position_type.upper() == "CLOSE" and self.maintain_records:
            if os.path.exists(self.file_path):
                try:
                    order_records = pd.read_csv(self.file_path, index_col=False)
                    order_records = order_records.replace("", pd.NA)
                    order_records = order_records.dropna(subset=self.filter_recordscols)
                    
                    order_records["exchange_token"] = pd.to_numeric(
                        order_records["exchange_token"], errors="coerce"
                    )
                    filtered_records = order_records[
                        (order_records["order_status"] == True) &
                        (order_records["strategy"] == self.strategy_name) &
                        (order_records["exchange_token"].astype("int64") == int(exchange_token)) &
                        (order_records["user_id"].astype(str).str.replace(".0", "") == str(user_id).replace(".0", "")) &
                        (order_records["broker"] == user_data["broker"]) &
                        (order_records["product_type"] == product)
                    ].reset_index(drop=True)

                    if filtered_records.empty:
                        response_message = f"No active position records for {TradingSymbol}"
                        self.write_logs("warning", f"[_place_order] {response_message}")
                        record = f"{datetime.now()},{strategy_name},{user_data['broker']},{user_id},{exchange},{exchange_token},{Symbol},{TradingSymbol},{position_type.upper()},{product},{transaction_type},{False},{None},{formated_qty},{0},{response_message},{tag}"
                        self.safe_append(record)
                        self.get_notification_payload(self.notification_topic,user_id,user_data['broker'],TradingSymbol,quantity,exchange,product,transaction_type,"FAILED",response_message,self.strategy_name)
                        return self.internal_ord_res(response, user_id, None, "FAILED", 0, response_message, None)
                    else:
                        final_qty = filtered_records['success_quantity'].sum()
                        if (final_qty < 0 and transaction_type == "SELL") or (final_qty > 0 and transaction_type == "BUY"):
                            response_message = f"Cannot close position: existing qty {final_qty} same side as {transaction_type}"
                            self.write_logs("error", f"[_place_order] {response_message}")
                            record = f"{datetime.now()},{strategy_name},{user_data['broker']},{user_id},{exchange},{exchange_token},{Symbol},{TradingSymbol},{position_type.upper()},{product},{transaction_type},{False},{None},{formated_qty},{0},{response_message},{tag}"
                            self.safe_append(record)
                            self.get_notification_payload(self.notification_topic,user_id,user_data['broker'],TradingSymbol,quantity,exchange,product,transaction_type,"FAILED",response_message,self.strategy_name)
                            return self.internal_ord_res(response, user_id, None, "FAILED", 0, response_message, None)
                        elif final_qty == 0:
                            response_message = "0 Quantity available to close"
                            self.write_logs("warning", f"[_place_order] {response_message}")
                            record = f"{datetime.now()},{strategy_name},{user_data['broker']},{user_id},{exchange},{exchange_token},{Symbol},{TradingSymbol},{position_type.upper()},{product},{transaction_type},{False},{None},{formated_qty},{0},{response_message},{tag}"
                            self.safe_append(record)
                            self.get_notification_payload(self.notification_topic,user_id,user_data['broker'],TradingSymbol,quantity,exchange,product,transaction_type,"FAILED",response_message,self.strategy_name)
                            return self.internal_ord_res(response, user_id, None, "FAILED", 0, response_message, None)

                        if abs(final_qty) < quantity:
                            self.write_logs("info", f"[_place_order] Capping close quantity to available qty {abs(final_qty)}")
                            quantity = abs(final_qty)
                except Exception as ex:
                    self.write_logs("error", f"[_place_order] Error reading records: {str(ex)}")

        # print("befire = initial broker = ",broker,"user id = ",user_id,"broker = ",user_data["broker"])
        api_instance = self._get_broker_instance(user_data)
        # print("api_instance = ",api_instance,"user id = ",user_id,"broker = ",user_data["broker"])
        if not api_instance:
            response_message = "Failed to initialize broker instance"
            self.write_logs("error", f"[_place_order] {response_message}")
            record = f"{datetime.now()},{strategy_name},{user_data['broker']},{user_id},{exchange},{exchange_token},{Symbol},{TradingSymbol},{position_type.upper()},{product},{transaction_type},{False},{None},{formated_qty},{0},{response_message},{tag}"
            self.safe_append(record)
            self.get_notification_payload(self.notification_topic,user_id,user_data['broker'],TradingSymbol,quantity,exchange,product,transaction_type,"FAILED",response_message,self.strategy_name)
            self.close_order_execptions(position_type, user_data["broker"], user_id, response_message, f"(Symbol - {TradingSymbol} ** Side - {transaction_type} ** Product - {product})")
            return self.internal_ord_res(response, user_id, None, "FAILED", 0, response_message, None)

        func = None
        broker_name = user_data["broker"].lower()
        if broker_name == "alice":
            func = self._place_order_alice
            func_agrs = {"account_id": user_data["account_id"], "position_type": _position_type, "broker_obj": api_instance, "exchange_token": exchange_token, "exchange": exchange, "quantity": quantity, "product": product, "order_type": order_type, "transaction_type": transaction_type, "validity": validity, "price": price, "trigger_price": trigger_price, "disclosed_quantity": disclosed_quantity, "tag": tag}
        elif broker_name == "trade_master":
            func = self._place_order_trade_master
            func_agrs = {"account_id": user_data["account_id"], "position_type": _position_type, "broker_obj": api_instance, "exchange_token": exchange_token, "exchange": exchange, "quantity": quantity, "product": product, "order_type": order_type, "transaction_type": transaction_type, "validity": validity, "price": price, "trigger_price": trigger_price, "disclosed_quantity": disclosed_quantity, "tag": tag}
        elif broker_name == "fyers":
            func = self._place_order_fyers
            func_agrs = {"account_id": user_data["account_id"], "position_type": _position_type, "broker_obj": api_instance, "exchange_token": exchange_token, "exchange": exchange, "quantity": quantity, "product": product, "order_type": order_type, "transaction_type": transaction_type, "validity": validity, "price": price, "trigger_price": trigger_price, "disclosed_quantity": disclosed_quantity, "tag": tag}
        elif broker_name == "zerodha":
            func = self._place_order_zerodha
            func_agrs = {"account_id": user_data["account_id"], "position_type": _position_type, "broker_obj": api_instance, "exchange_token": exchange_token, "exchange": exchange, "quantity": quantity, "product": product, "order_type": order_type, "transaction_type": transaction_type, "variety": variety, "validity": validity, "price": price, "trigger_price": trigger_price, "disclosed_quantity": disclosed_quantity, "tag": tag}
        elif broker_name == "upstock":
            func = self._place_order_upstocks
            func_agrs = {"account_id": user_data["account_id"], "position_type": _position_type, "broker_obj": api_instance, "exchange_token": exchange_token, "exchange": exchange, "quantity": quantity, "product": product, "validity": validity, "order_type": order_type, "transaction_type": transaction_type, "price": price, "trigger_price": trigger_price, "disclosed_quantity": disclosed_quantity, "is_amo": is_amo, "api_version": api_version, "tag": tag}
        elif broker_name == "angel":
            func = self._place_order_angel
            func_agrs = {"account_id": user_data["account_id"], "position_type": _position_type, "broker_obj": api_instance, "exchange_token": exchange_token, "exchange": exchange, "quantity": quantity, "product": product, "order_type": order_type, "transaction_type": transaction_type, "variety": variety, "validity": validity, "price": price, "trigger_price": trigger_price, "disclosed_quantity": disclosed_quantity, "tag": tag}
        elif broker_name == "finvasia":
            func = self._place_order_finvasia
            func_agrs = {"account_id": user_data["account_id"], "position_type": _position_type, "broker_obj": api_instance, "exchange_token": exchange_token, "exchange": exchange, "quantity": quantity, "product": product, "order_type": order_type, "transaction_type": transaction_type, "variety": variety, "validity": validity, "price": price, "trigger_price": trigger_price, "disclosed_quantity": disclosed_quantity, "tag": tag}
        elif broker_name == "xts":
            func = self._place_order_xts
            func_agrs = {"account_id": user_data["account_id"], "position_type": _position_type, "broker_obj": api_instance, "exchange_token": exchange_token, "exchange": exchange, "quantity": quantity, "product": product, "order_type": order_type, "transaction_type": transaction_type, "variety": variety, "validity": validity, "price": price, "trigger_price": trigger_price, "disclosed_quantity": disclosed_quantity, "tag": tag, "lotsize": lot_size}
            print("func_agrs == ",func_agrs)
        elif broker_name == "5paisa":
            func = self._place_order_5paisa
            func_agrs = {"account_id": user_data["account_id"], "position_type": _position_type, "broker_obj": api_instance, "exchange_token": exchange_token, "exchange": exchange, "quantity": quantity, "product": product, "order_type": order_type, "transaction_type": transaction_type, "variety": variety, "validity": validity, "price": price, "trigger_price": trigger_price, "disclosed_quantity": disclosed_quantity, "tag": tag, "lotsize": lot_size}
        elif broker_name == "dhan":
            func = self._place_order_dhan
            func_agrs = {"account_id": user_data["account_id"], "position_type": _position_type, "broker_obj": api_instance, "exchange_token": exchange_token, "exchange": exchange, "quantity": quantity, "product": product, "order_type": order_type, "transaction_type": transaction_type, "variety": variety, "validity": validity, "price": price, "trigger_price": trigger_price, "disclosed_quantity": disclosed_quantity, "tag": tag, "lotsize": lot_size}
        elif broker_name == "findoc":
            func = self._place_order_findoc
            func_agrs = {"account_id": user_data["account_id"], "position_type": _position_type, "broker_obj": api_instance, "exchange_token": exchange_token, "exchange": exchange, "quantity": quantity, "product": product, "order_type": order_type, "transaction_type": transaction_type, "variety": variety, "validity": validity, "price": price, "trigger_price": trigger_price, "disclosed_quantity": disclosed_quantity, "tag": tag, "lotsize": lot_size}
        elif broker_name == "mo":
            func = self._place_order_mo
            func_agrs = {"account_id": user_data["account_id"], "position_type": _position_type, "broker_obj": api_instance, "exchange_token": exchange_token, "exchange": exchange, "quantity": quantity, "product": product, "order_type": order_type, "transaction_type": transaction_type, "variety": variety, "validity": validity, "price": price, "trigger_price": trigger_price, "disclosed_quantity": disclosed_quantity, "tag": tag, "lotsize": lot_size}
        else:
            response_message = f"no_broker_matched_for_{broker_name}"
            self.write_logs("error", f"[_place_order] {response_message}")
            record = f"{datetime.now()},{strategy_name},{user_data['broker']},{user_id},{exchange},{exchange_token},{Symbol},{TradingSymbol},{position_type.upper()},{product},{transaction_type},{False},{None},{formated_qty},{0},{response_message},{tag}"
            self.safe_append(record)
            self.get_notification_payload(self.notification_topic,user_id,user_data['broker'],TradingSymbol,quantity,exchange,product,transaction_type,"FAILED",response_message,self.strategy_name)
            return self.internal_ord_res(response, user_id, None, "FAILED", 0, response_message, None)

        if func:
            str_order_ids = ""
            str_order_message = ""
            success_placed = False 
          
            if split_size == 0:
                res = func(**func_agrs)
                response[user_id]["result"].append(res)
                str_order_ids = str(res.get("order_id", ""))
                str_order_message = str(res.get("message", ""))
                if res["status"].upper() == "SUCCESS":
                    success_placed = True
                    response[user_id]['summary']['traded_qty'] = quantity
                else:
                    response[user_id]['summary']['failed_qty'] = quantity
            
            elif split_size > 0: 
                rem_qty = quantity
                chunk_sizes = []
                while rem_qty > 0:
                    q = min(rem_qty, split_size)
                    chunk_sizes.append(q)
                    rem_qty -= q

                lock = threading.Lock()
                str_order_ids_list = []
                str_order_msgs_list = []
                success_placed = False
                # Using the instance's persistent executor
                futures = [self.executor.submit(self._call_suborder, func, func_agrs.copy(), q) for q in chunk_sizes]
                for fut in as_completed(futures):
                    try:
                        res = fut.result()
                        with lock:
                            response[user_id]["result"].append(res)
                            str_order_ids_list.append(str(res.get("order_id", "")))
                            str_order_msgs_list.append(str(res.get("message", "") or ""))
                            if res["status"].upper() in ["SUCCESS","COMPLETE","COMPLETED","TRADED","FILLED"]:
                                response[user_id]['summary']['traded_qty'] += int(res.get("placed_qty", 0))
                                success_placed = True
                            else:
                                response[user_id]['summary']['failed_qty'] += int(res.get("placed_qty", 0))
                    except Exception as e:
                        self.write_logs("error", f"[_place_order] Chunk execution failed: {str(e)}")

                str_order_ids = "|".join(str_order_ids_list)
                str_order_message = "|".join(str_order_msgs_list)
                
            if self.maintain_records:
                str_order_message = str_order_message.replace(",", "_") if str_order_message else ""
                total_qty_w = -quantity if transaction_type == "SELL" else quantity
                traded_qty_w = -response[user_id]['summary']['traded_qty'] if transaction_type == "SELL" else response[user_id]['summary']['traded_qty']
                record = f"{datetime.now()},{strategy_name},{user_data['broker']},{user_id},{exchange},{exchange_token},{Symbol},{TradingSymbol},{position_type.upper()},{product},{transaction_type},{success_placed},{str_order_ids},{total_qty_w},{traded_qty_w},{str_order_message},{tag}"
                self.safe_append(record)
        
        if not success_placed:
            Order_message = f"[_place_order] Order placed {str_order_message} for {user_id} in {strategy_name} | token {exchange_token} | {TradingSymbol} & {quantity}"
            self._send_message(messageType="ALERT", messageDetails=Order_message)
            self.get_notification_payload(self.notification_topic,user_id,user_data['broker'],TradingSymbol,quantity,exchange,product,transaction_type,"FAILED",str_order_message,self.strategy_name)
            
        duration = time.time() - start_time
        self.write_logs("info", f"[_place_order] Completed for {user_id} in {duration:.2f}s, Success: {success_placed}")
        return response

               
            
    def _place_order_upstocks(self, account_id, position_type, broker_obj, exchange_token, exchange, quantity, product, validity, order_type, transaction_type, price=0, trigger_price=0, disclosed_quantity=0, is_amo=False, api_version='v2', tag=""):
        # start_time = time.time()
        # self.write_logs("info", f"[_place_order_upstocks] Started for {account_id}, Token: {exchange_token}")
        
        if exchange == "BFO":
            _exchange = "BSE"
        elif exchange == "NFO":
            _exchange = "NSE"
        else:
            _exchange = exchange
            
        configuration = upstox_client.Configuration()
        configuration.access_token = broker_obj
        instrument_data = self._get_instrument_data("upstock", exchange, exchange_token)
        # self.write_logs("info",f"instrument_data upstock {account_id} -> {instrument_data}")  
        if not instrument_data:
            self.write_logs("error", f"[_place_order_upstocks] Invalid exchange token: {exchange_token}")
            return {"status": "FAILED", "message": "invalid_exchange_token", "exchange_token": exchange_token}

        if exchange in ["NFO", "BFO"]:
            _quantity = int(quantity * int(instrument_data['lot_size']))
        else:
            _quantity = quantity            
            
        if product == "MIS":
            _product = "I"
        elif product == "CNC":
            _product = "D"
        else:
            self.write_logs("error", f"[_place_order_upstocks] Invalid product type: {product}")
            return {"status": "FAILED", "message": "invalid_product_type", "product_type": product, "valid_values": "MIS, CNC"}   
        
        _order_type = "LIMIT" if order_type == "ADJUST" else order_type
        
        if position_type.upper() == "CLOSE":
            api_instance = upstox_client.PortfolioApi(upstox_client.ApiClient(configuration))
            netwise = None
            for i in range(3):
                try:
                    positions = api_instance.get_positions(api_version).to_dict()
                    if "data" in positions:
                        netwise = positions["data"]
                        break
                except Exception as e:
                    self.write_logs("warning", f"[_place_order_upstocks] Position fetch attempt {i+1} failed: {str(e)}")
            
            if netwise is None or netwise == []:
                return {"status": "FAILED", "message": "No active positions found or failed to fetch", "error": "EMPTY_POSITIONS"}
            
            position = next((pos for pos in netwise if ((str(pos.get("instrument_token")) == str(instrument_data['instrument_key'])) and (pos.get("product") == _product))), None)
            if position is None:
                return {"status": "FAILED", "message": f"No active position to close for {exchange_token}"}
            
            p_qty = int(position["quantity"])
            if abs(p_qty) == 0:
                return {"status": "FAILED", "message": f"Available qty is 0"}
            
            if p_qty < 0 and transaction_type == "SELL":
                return {"status": "FAILED", "message": f"Cannot sell to close a short position"}
            elif p_qty > 0 and transaction_type == "BUY":
                return {"status": "FAILED", "message": f"Cannot buy to close a long position"}
            
            if abs(p_qty) < _quantity:
                _quantity = abs(p_qty)

        api_instance = upstox_client.OrderApi(upstox_client.ApiClient(configuration))
        if validity is None: validity = "DAY"
        if trigger_price is None: trigger_price=0
        body = upstox_client.PlaceOrderRequest(quantity=_quantity, product=_product, validity=validity, price=price, instrument_token=instrument_data["instrument_key"], order_type=_order_type, transaction_type=transaction_type, disclosed_quantity=disclosed_quantity, trigger_price=trigger_price, is_amo=is_amo, tag=tag)
        
        response = {}
        try:
            self.write_logs("info",f"[_place_order_upstocks] - ID:{account_id} - order request {body}")
            place_res = api_instance.place_order(body, api_version).to_dict()
            time.sleep(self.order_status_check_interval)
            self.write_logs("info",f"[_place_order_upstocks] - ID:{account_id} - order response {place_res}")
            if 'data' in place_res and 'order_id' in place_res['data']:
                o_id = place_res["data"]["order_id"]
                order_status_res = api_instance.get_order_status(order_id=o_id).to_dict()
                if 'data' in order_status_res:
                    response = order_status_res['data']
                    # self.write_logs("info", f"[_place_order_upstocks] Order placed: {o_id}, Status: {response.get('status')}")
                    
                    if (order_type in ["LIMIT", "ADJUST"]) and (response["status"] not in ["rejected", "complete", "failed", "reject", "cancelled", "completed"]):
                        modify_response, order_id = self._modify_order("upstock", account_id, broker_obj, instrument_data, o_id, quantity, exchange_token, exchange, product, order_type, transaction_type, position_type, validity, price, trigger_price)
                        if modify_response == "MARKET_SUCCESS":
                            response["status"] = "SUCCESS"
                            response["status_message_raw"] = "Changed from limit to market"
                            response['order_id'] = order_id
                            
                        elif "ZOMBIE_ORDER_MODIFIER" in modify_response:
                            response["status"] = "SUCCESS"
                            response["status_message_raw"] = f"Started {modify_response}"
                            
                        elif "FAILED" in modify_response or "PENDING" in modify_response:
                            response["status"] = "FAILED"
                            response["status_message_raw"] = modify_response
                            response["error"] = modify_response
                else:
                    response = {"order_id": o_id, "status": "FAILED", "status_message_raw": "FAILED TO GET ORDER DETAILS"}
            else:
                response = {"order_id": None, "status": "FAILED", "status_message_raw": str(place_res).replace("\n", "|").replace(",","-")}
        except ApiException as e:
            str_err = e.__str__() 
            str_exception = str_err if str_err else str(e)
            if ("order not found" in str_exception.lower()) or ("udapi100010" in str_exception.lower()):
                self.write_logs("warning", f"[_place_order_upstocks] API Exception But Order success : {str_exception}")
                response["status"] = "SUCCESS"
                response["status_message_raw"] = "exception Order not found which means its success"
                response['order_id'] = order_id
            else:    
                self.write_logs("error", f"[_place_order_upstocks] API Exception: {str_exception}")
                response = {"status": "FAILED", "order_id": None, "status_message_raw": str_exception}
            
        # duration = (time.time() - start_time)
        # self.write_logs("info", f"[_place_order_upstocks] Finished for {account_id} in {duration:.2f}s")
        
        final_status = response.get("status", "FAILED").lower()
        if final_status in ["rejected", "failed", "reject"]:
            return {"order_id": response.get("order_id"), "placed_qty": quantity, "status": "REJECTED" if response.get("order_id") else "FAILED", "message": response.get("status_message_raw"), "error": response.get("error")}
        else:
            return {"order_id": response.get("order_id"), "placed_qty": quantity, "status": "SUCCESS", "message": response.get("status_message_raw")}
   

    zerodha_master = None
    def _place_order_zerodha(self, account_id, position_type, broker_obj:KiteConnect, exchange_token: int, exchange: str, quantity: int, product: str, order_type: str, transaction_type: str, variety="regular", validity=None, price: float = None, trigger_price: float = None, disclosed_quantity: int = None, tag: str = None):
        # start_time = time.time()
        # self.write_logs("info", f"[_place_order_zerodha] Started for {account_id}, Token: {exchange_token}")
        
        instrument_item = self._get_instrument_data("zerodha", exchange, exchange_token)
        if not instrument_item:
            self.write_logs("error", f"[_place_order_zerodha] Invalid exchange token: {exchange_token}")
            return {"status": "FAILED", "message": "invalid_exchange_token", "exchange_token": exchange_token}

        _quantity = int(quantity * int(instrument_item['lot_size'])) if exchange in ["NFO", "BFO"] else quantity
        
        if product == "CNC" and exchange in ["MCX", "NFO", "BFO"]:
            _product = "NRML"
        else:
            _product = product

        if position_type.upper() == "CLOSE":
            try:
                netwise = broker_obj.positions().get("net", [])
                position = next((pos for pos in netwise if ((int(pos.get("instrument_token")) == int(instrument_item['instrument_token'])) and (pos.get("product") == _product))), None)
                
                if position:
                    p_qty = int(position["quantity"])
                    if abs(p_qty) == 0:
                        return {"status": "FAILED", "message": "Available qty is 0"}
                    if (p_qty < 0 and transaction_type.upper() == "SELL") or (p_qty > 0 and transaction_type.upper() == "BUY"):
                        return {"status": "FAILED", "message": f"Cannot close: existing {p_qty} same side as {transaction_type}"}
                    if abs(p_qty) < _quantity:
                        _quantity = abs(p_qty)
                else:
                    return {"status": "FAILED", "message": f"No active position to close for {exchange_token}"}
            except Exception as e:
                self.write_logs("error", f"[_place_order_zerodha] Position fetch error: {str(e)}")
                return {"status": "FAILED", "message": f"Failed to fetch positions: {str(e)}"}

        _order_type = "LIMIT" if order_type.upper() == "ADJUST" else order_type.upper()
        order_payload = dict(
            variety=variety, 
            exchange=exchange.upper(), 
            tradingsymbol=instrument_item['tradingsymbol'], 
            transaction_type=transaction_type.upper(), 
            quantity=int(_quantity), 
            product=_product.upper(), 
            order_type=_order_type,
            tag=tag[:19] if tag else "",
            price=price if price else 0.0,
            disclosed_quantity=disclosed_quantity if disclosed_quantity else 0,
            trigger_price=trigger_price if trigger_price else 0.0,
            validity=validity if validity else broker_obj.VALIDITY_DAY
        )
        
        response = {}
        try:
            self.write_logs("info", f"[_place_order_zerodha] ID:{account_id} Order payload : {order_payload}")
            o_id = broker_obj.place_order(**order_payload)
            self.write_logs("info", f"[_place_order_zerodha] ID:{account_id} Order response : {o_id}")
            if o_id:
                time.sleep(0.5)
                # history = broker_obj.order_history(o_id)
                history = broker_obj.orders()
                history = next((his for his in history if ((int(his.get("order_id")) == int(o_id)))), None)

                if history:
                    last_event = history
                    status = last_event.get("status", "").upper()
                    self.write_logs("info", f"[_place_order_zerodha] Order placed: {o_id}, Status: {status}")
                    
                    if (order_type.upper() in ["LIMIT", "ADJUST"]) and (status not in ["REJECTED", "COMPLETE", "FAILED", "REJECT", "CANCELLED", "COMPLETED"]):
                        modify_res, final_oid = self._modify_order("zerodha", account_id, broker_obj, instrument_item, o_id, quantity, exchange_token, exchange, product, order_type, transaction_type, position_type, validity, price, trigger_price)
                        if modify_res == "MARKET_SUCCESS":
                            last_event["status"] = "SUCCESS"
                            last_event["order_id"] = final_oid
                        elif "ZOMBIE_ORDER_MODIFIER" in modify_res:
                            last_event["status"] = "SUCCESS"
                            last_event["message"] = f"Started {modify_res}"
                        elif "FAILED" in modify_res:
                            last_event["status"] = "FAILED"
                            last_event["message"] = modify_res
                        else:
                            last_event["status"] = modify_res
                            last_event["order_id"] = final_oid
                    response = last_event
                else:
                    response = {"order_id": o_id, "status": "FAILED", "message": "Order history empty"}
            else:
                response = {"order_id": None, "status": "FAILED", "message": "No order_id returned"}
        except Exception as ex:
            self.write_logs("error", f"[_place_order_zerodha] Exception: {str(ex)}")
            response = {"status": "FAILED", "message": str(ex)}

        # duration = time.time() - start_time
        # self.write_logs("info", f"[_place_order_zerodha] Finished for {account_id} in {duration:.2f}s")
        
        final_status = response.get("status", "FAILED").upper()
        is_success = final_status in ["SUCCESS", "COMPLETE", "COMPLETED", "TRADED", "FILLED"]
        return {
            "order_id": response.get("order_id"),
            "placed_qty": quantity,
            "status": "SUCCESS" if is_success else "FAILED",
            "message": response.get("message") or response.get("status_message") or final_status,
            "error": response.get("error") if not is_success else None
        }

        # return response   
     
    # response  {'orderstatus': 'rejected', 'message': 'Your order has been rejected due to Insufficient Funds. Available funds - Rs. 0.00. You require Rs. 20.28 funds to execute this order.', 'order_id': '251031000562064'}
    def _place_order_trade_master(self, account_id, position_type, broker_obj: TradeHub, exchange_token: int, exchange: str, quantity: int, product: str, order_type: str, transaction_type: str, validity=None, price: float = None, trigger_price: float = None, disclosed_quantity: int = None, tag: str = None):
        # start_time = time.time()
        # self.write_logs("info", f"[_place_order_trade_master] Started for {account_id}, Token: {exchange_token}")
        
        instrument_item = self._get_instrument_data("alice", exchange, exchange_token)
        if not instrument_item:
            self.write_logs("error", f"[_place_order_trade_master] Invalid exchange token: {exchange_token}")
            return {"status": "FAILED", "message": "invalid_exchange_token", "exchange_token": exchange_token}

        _quantity = (quantity * instrument_item['Lot Size']) if exchange in ["NFO", "BFO"] else quantity
        
        if position_type.upper() == "CLOSE":
            try:
                netwise = broker_obj.get_positions().get("result", [])
                if not  netwise :
                    return {"status": "FAILED", "message": "No active position found to close"}
                
                _product = "NRML" if product == "CNC" and exchange in ["MCX", "NFO", "BFO"] else product
                position = next((pos for pos in netwise if (((str(int(pos.get("instrumentId"))) == str(int(exchange_token)))) and (pos.get("product") == _product))), None)

                if position:
                    p_qty_net = int(position["netQuantity"])
                    p_qty_base = abs(p_qty_net)
                    if exchange in ["MCX"]:
                        p_qty_base = abs(p_qty_net / int(position.get("multiplier", 1)))
                    
                    if p_qty_base == 0:
                        return {"status": "FAILED", "message": "Available qty is 0"}
                    if (p_qty_net < 0 and transaction_type.upper() == "SELL") or (p_qty_net > 0 and transaction_type.upper() == "BUY"):
                        return {"status": "FAILED", "message": "Side mismatch for close"}
                    if p_qty_base < _quantity:
                        _quantity = p_qty_base
                else:
                    return {"status": "FAILED", "message": "No active position found to close"}
            except Exception as e:
                error_str = str(e.__str__())
                self.write_logs("error", f"[_place_order_trade_master] Position fetch failed: {error_str}")
                return {"status": "FAILED", "message": f"Position fetch failed: {error_str}"}

        order_payload = {
            "instrumentId": int(instrument_item["Token"]),
            "exchange": exchange.upper(),
            "quantity": int(_quantity),
            "price": float(price) if price else 0.0,
            "orderComplexity": trade_oComplexity.Regular,
            "orderTag": tag[:19] if tag else "",
            "slTriggerPrice": 0,
            "slLegPrice": 0,
            "targetLegPrice": 0
        }
        # Transaction type mapping
        if transaction_type.upper() == "BUY":
            order_payload["transactionType"] = trade_ttype.Buy
        elif transaction_type.upper() == "SELL":
            order_payload["transactionType"] = trade_ttype.Sell
        else:
            return {"status": "FAILED", "message": "invalid_transaction_type"}

        order_payload["validity"] = "IOC" if validity and validity.upper() == "IOC" else "DAY"
        
        # Product mapping
        if product.upper() in ["CNC", "NRML"]:
            order_payload["product"] = trade_prod_type.Longterm
        elif product.upper() == "MIS":
            order_payload["product"] = trade_prod_type.Intraday
        else:
            return {"status": "FAILED", "message": "invalid_product_type"}

        # Order type mapping
        if order_type.upper() in ["LIMIT", "ADJUST"]:
            order_payload["orderType"] = trade_Otype.Limit
        elif order_type.upper() == "MARKET":
            order_payload["orderType"] = trade_Otype.Market
        elif order_type.upper() == "SL-M":
            order_payload["orderType"] = trade_Otype.StopLossMarket
        else:
            return {"status": "FAILED", "message": "invalid_order_type"}
        # print("order_payload == ",order_payload)
        response = {}
        try:
            self.write_logs("info", f"[_place_order_trade_master] ID: {account_id} Order payload: {order_payload}")
            res = broker_obj.placeOrder(**order_payload)
            self.write_logs("info", f"[_place_order_trade_master] ID: {account_id} Order placed response: {res}")
            # print("res == ", res )
            if (position_type.upper() == "CLOSE") and (not res.get('result') or res['result'][0].get("brokerOrderId") is None):
                for _ in range(self.order_retry):
                    res = broker_obj.placeOrder(**order_payload)
                    self.write_logs("info", f"[_place_order_trade_master] ID: {account_id} Order placed response retry {_}: {res}")
                    if res.get('result') and res['result'][0].get("brokerOrderId"):
                        break
                            
            if res.get('result') and res['result'][0].get("brokerOrderId"):
                time.sleep(self.order_status_check_interval)
                o_id = res['result'][0]["brokerOrderId"]
                history_res = broker_obj.get_orderHistory(o_id)
                
                if history_res.get('result'):
                    order_story = history_res['result'][0]
                    status = order_story.get('orderStatus', '').upper()
                    # self.write_logs("info", f"[_place_order_trade_master] Order placed: {o_id}, Status: {status}")
                    
                    if (order_type.upper() in ["LIMIT", "ADJUST"]) and (status not in ["REJECTED", "COMPLETE", "FAILED", "REJECT", "CANCELLED", "COMPLETED"]):
                        modify_res, final_oid = self._modify_order("trade_master", account_id, broker_obj, instrument_item, o_id, quantity, exchange_token, exchange, product, order_type, transaction_type, position_type, validity, price, trigger_price)
                        if modify_res == "MARKET_SUCCESS":
                            response = {"order_id": final_oid, "orderstatus": "SUCCESS"}
                        elif "ZOMBIE_ORDER_MODIFIER" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "SUCCESS", "message": f"Started {modify_res}"}
                        elif "FAILED" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "FAILED", "message": modify_res}
                        else:
                            response = {"order_id": o_id, "orderstatus": modify_res, "message": f"Modified order id: {final_oid}"}
                    else:
                        response = {"order_id": o_id, "orderstatus": status, "message": order_story.get("rejectionReason")}
                else:
                    response = {"order_id": o_id, "orderstatus": "FAILED", "message": "Failed to get history"}
            else:
                response = {"order_id": None, "orderstatus": "FAILED", "message": str(res).replace("\n", "|").replace(",","-")}
        except Exception as ex:
            self.write_logs("error", f"[_place_order_trade_master] Exception: {str(ex)}")
            response = {"orderstatus": "FAILED", "message": str(ex)}

        # duration = time.time() - start_time
        # self.write_logs("info", f"[_place_order_trade_master] Finished for {account_id} in {duration:.2f}s")
        
        final_status = response.get("orderstatus", "FAILED").upper()
        is_success = final_status in ["SUCCESS", "COMPLETE", "COMPLETED", "TRADED", "FILLED"]
        return {
            "order_id": response.get("order_id"),
            "placed_qty": quantity,
            "status": "SUCCESS" if is_success else "FAILED",
            "message": response.get("message") or final_status,
            "error": response.get("error") if not is_success else None
        }         

    def _place_order_alice(self, account_id, position_type, broker_obj: Aliceblue, exchange_token: int, exchange: str, quantity: int, product: str, order_type: str, transaction_type: str, validity=None, price: float = None, trigger_price: float = None, disclosed_quantity: int = None, tag: str = None):
        # start_time = time.time()
        # self.write_logs("info", f"[_place_order_alice] Started for {account_id}, Token: {exchange_token}")
        
        instrument_item = self._get_instrument_data("alice", exchange, exchange_token)
        if not instrument_item:
            self.write_logs("error", f"[_place_order_alice] Invalid exchange token: {exchange_token}")
            return {"status": "FAILED", "message": "invalid_exchange_token", "exchange_token": exchange_token}

        _quantity = (quantity * instrument_item['Lot Size']) if exchange in ["NFO", "BFO"] else quantity
        
        if position_type.upper() == "CLOSE":
            try:
                netwise = broker_obj.get_netwise_positions()
                if isinstance(netwise, list) and netwise:
                    _product = "NRML" if product == "CNC" and exchange in ["MCX", "NFO", "BFO"] else product
                    position = next((pos for pos in netwise if (((str(int(pos.get("Token"))) == str(int(exchange_token)))) and (pos.get("Pcode") == _product))), None)
                    
                    if position:
                        p_qty_net = int(position["Netqty"])
                        p_qty_base = abs(p_qty_net) if exchange in ["NFO", "BFO"] else abs(p_qty_net / int(position.get("BLQty", 1)))
                        
                        if p_qty_base == 0:
                            return {"status": "FAILED", "message": "Available qty is 0"}
                        if (p_qty_net < 0 and transaction_type.upper() == "SELL") or (p_qty_net > 0 and transaction_type.upper() == "BUY"):
                            return {"status": "FAILED", "message": "Side mismatch for close"}
                        if p_qty_base < _quantity:
                            _quantity = p_qty_base
                    else:
                        return {"status": "FAILED", "message": "No active position found"}
                else:
                    return {"status": "FAILED", "message": "Failed to fetch positions", "error": netwise.get("emsg") if isinstance(netwise, dict) else str(netwise)}
            except Exception as e:
                self.write_logs("error", f"[_place_order_alice] Position fetch failed: {str(e)}")

        order_payload = {
            "instrument": self.alice_instrument(instrument_item),
            "quantity": int(_quantity),
            "price": float(price) if price else 0.0,
            "trigger_price": float(trigger_price) if trigger_price else None,
            "order_tag": tag[:19] if tag else ""
        }

        # Transaction type mapping
        order_payload["transaction_type"] = TransactionType.Buy if transaction_type.upper() == "BUY" else TransactionType.Sell
        
        # Validity mapping
        order_payload["is_ioc"] = True if validity and validity.upper() == "IOC" else False
        
        # Product mapping
        prod_map = {"MIS": ProductType.Intraday, "CNC": ProductType.Delivery, "CO": ProductType.CoverOrder, "BO": ProductType.BracketOrder, "NRML": ProductType.Normal}
        order_payload["product_type"] = prod_map.get(product.upper(), ProductType.Intraday)

        # Order type mapping
        if order_type.upper() in ["LIMIT", "ADJUST"]:
            order_payload["order_type"] = OrderType.Limit
        elif order_type.upper() == "MARKET":
            order_payload["order_type"] = OrderType.Market
        elif order_type.upper() == "SL":
            order_payload["order_type"] = OrderType.StopLossLimit
        elif order_type.upper() == "SL-M":
            order_payload["order_type"] = OrderType.StopLossMarket
        else:
            return {"status": "FAILED", "message": "invalid_order_type"}

        response = {}
        try:
            self.write_logs("info", f"[_place_order_alice] ID:{account_id} - Order placed reponse : {order_payload}")
            res = broker_obj.place_order(**order_payload)
            self.write_logs("info", f"[_place_order_alice] ID:{account_id} - Order placed reponse : {res}")
            if (position_type.upper() == "CLOSE") and (not res.get('NOrdNo')):
                for _ in range(self.order_retry):
                    res = broker_obj.place_order(**order_payload)
                    self.write_logs("info", f"[_place_order_alice] ID:{account_id} - Order placed reponse : {res}")
                    if res.get('NOrdNo'):
                        break

            if res.get('NOrdNo'):
                time.sleep(self.order_status_check_interval)
                o_id = res['NOrdNo']
                order_story = broker_obj.get_order_history(o_id)
                # self.write_logs("info", f"[_place_order_alice] Order placed: {o_id}")
                
                if isinstance(order_story, dict) and "Status" in order_story:
                    status = order_story['Status'].upper()
                    if (order_type.upper() in ["LIMIT", "ADJUST"]) and (status not in ["REJECTED", "COMPLETE", "FAILED", "REJECT", "CANCELLED", "COMPLETED"]):
                        modify_res, final_oid = self._modify_order("alice", account_id, broker_obj, instrument_item, o_id, quantity, exchange_token, exchange, product, order_type, transaction_type, position_type, validity, price, trigger_price)
                        if modify_res == "MARKET_SUCCESS":
                            response = {"order_id": final_oid, "orderstatus": "SUCCESS"}
                        elif "ZOMBIE_ORDER_MODIFIER" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "SUCCESS", "message": f"Started {modify_res}"}
                        elif "FAILED" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "FAILED", "message": modify_res}
                        else:
                            response = {"order_id": o_id, "orderstatus": modify_res}
                    else:
                        response = {"order_id": o_id, "orderstatus": status, "message": order_story.get("RejReason")}
                else:
                    response = {"order_id": o_id, "orderstatus": "FAILED", "message": "Failed to get history"}
            else:
                response = {"orderstatus": "FAILED", "message": "order_id not received", "error": str(res).replace("\n", "|").replace(",","-")}
        except Exception as ex:
            self.write_logs("error", f"[_place_order_alice] Exception: {str(ex)}")
            response = {"orderstatus": "FAILED", "message": str(ex)}

        # duration = time.time() - start_time
        # self.write_logs("info", f"[_place_order_alice] Finished for {account_id} in {duration:.2f}s")
        
        final_status = response.get("orderstatus", "FAILED").upper()
        is_success = final_status in ["SUCCESS", "COMPLETE", "COMPLETED", "TRADED", "FILLED"]
        return {
            "order_id": response.get("order_id"),
            "placed_qty": quantity,
            "status": "SUCCESS" if is_success else "FAILED",
            "message": response.get("message") or final_status,
            "error": response.get("error") if not is_success else None
        }
     
    fyers_master = {}
    fyers_ord_res = {
            1 : "Canceled",
            2 : "Filled",
            4 : "Transit",
            5 : "Rejected",
            6 : "Pending",
            7 : "Expired"
            }
    def _place_order_fyers(self, account_id, position_type, broker_obj, exchange_token: int, exchange: str, quantity: int, product: str, order_type: str, transaction_type: str, validity=None, price: float = None, trigger_price: float = None, disclosed_quantity: int = None, tag: str = None):
        # start_time = time.time()
        # self.write_logs("info", f"[_place_order_fyers] Started for {account_id}, Token: {exchange_token}")
        
        instrument_item = self._get_instrument_data("fyers", exchange, exchange_token)
        if not instrument_item:
            self.write_logs("error", f"[_place_order_fyers] Invalid exchange token: {exchange_token}")
            return {"status": "FAILED", "message": "invalid_exchange_token", "exchange_token": exchange_token}

        _quantity = (quantity * int(instrument_item['MinimumLotSize'])) if exchange in ["NFO", "BFO"] else quantity
        
        if position_type.upper() == "CLOSE":
            try:
                positions = broker_obj.positions()
                if ("netPositions" in positions) and positions['netPositions']:
                    netwise = positions['netPositions']
                    p_token = int(instrument_item['Fytoken'])
                    position = next((pos for pos in netwise if int(pos.get("fyToken")) == p_token), None)
                    
                    if position:
                        p_qty_net = int(position["netQty"])
                        if abs(p_qty_net) == 0:
                            return {"status": "FAILED", "message": "Available qty is 0"}
                        # Fyers side: 1 for Buy, -1 for Sell
                        if (p_qty_net < 0 and transaction_type.upper() == "SELL") or (p_qty_net > 0 and transaction_type.upper() == "BUY"):
                            return {"status": "FAILED", "message": "Side mismatch for close"}
                        if abs(p_qty_net) < _quantity:
                            _quantity = abs(p_qty_net)
                    else:
                        return {"status": "FAILED", "message": "No active position found"}
                else:
                    return {"status": "FAILED", "message": "Failed to fetch positions", "error": positions}
            except Exception as e:
                self.write_logs("error", f"[_place_order_fyers] Position fetch failed: {str(e)}")

        order_payload = {
            "symbol": instrument_item['SymbolTicker'],
            "qty": int(_quantity),
            "limitPrice": float(price) if price else 0.0,
            "trigger_price": float(trigger_price) if trigger_price else 0.0,
            "stopPrice": 0,
            "disclosedQty": 0,
            "order_tag": tag[:19] if tag else "",
            "offlineOrder": False
        }
        
        # side map
        order_payload["side"] = 1 if transaction_type.upper() == "BUY" else -1
        # validity map
        order_payload["validity"] = "IOC" if validity and validity.upper() == "IOC" else "DAY"
        # product type map
        prod_map = {"MIS": "INTRADAY", "CNC": "MARGIN", "CO": "CO", "BO": "BO"}
        order_payload["productType"] = prod_map.get(product.upper(), "INTRADAY")

        # Order type map
        o_type_upper = order_type.upper()
        if o_type_upper in ["LIMIT", "ADJUST"]: order_payload["type"] = 1
        elif o_type_upper == "MARKET": order_payload["type"] = 2
        elif o_type_upper == "SL-M": order_payload["type"] = 3
        elif o_type_upper in ["SL", "SL-L"]: order_payload["type"] = 4
        else: return {"status": "FAILED", "message": "invalid_order_type"}

        response = {}
        try:
            self.write_logs("info", f"[_place_order_fyers] ID:{account_id} - Order placed reponse : {order_payload}")
            res = broker_obj.place_order(order_payload)
            time.sleep(self.order_status_check_interval)
            self.write_logs("info", f"[_place_order_fyers] ID:{account_id} - Order placed reponse : {res}")
            if res and "id" in res:
                o_id = res["id"]
                history_res = broker_obj.get_orders({"id": o_id})
                if history_res and "orderBook" in history_res and len(history_res['orderBook']) >= 1:
                    order_data = history_res['orderBook'][0]
                    status_code = order_data.get('status')
                    status = self.fyers_ord_res.get(status_code, "UNKNOWN").upper()
                    # self.write_logs("info", f"[_place_order_fyers] Order placed: {o_id}, Status: {status}")
                    
                    if (o_type_upper in ["LIMIT", "ADJUST"]) and (status not in ["REJECTED", "COMPLETE", "FILLED", "CANCELLED", "EXPIRED"]):
                        modify_res, final_oid = self._modify_order("fyers", account_id, broker_obj, instrument_item, o_id, quantity, exchange_token, exchange, product, order_type, transaction_type, position_type, validity, price, trigger_price)
                        if modify_res == "MARKET_SUCCESS":
                            response = {"order_id": final_oid, "orderstatus": "SUCCESS"}
                            
                        elif "ZOMBIE_ORDER_MODIFIER" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "SUCCESS", "message": f"Started {modify_res}"}
                            
                        elif "FAILED" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "FAILED", "message": modify_res}
                        else:
                            response = {"order_id": o_id, "orderstatus": modify_res}
                    else:
                        response = {"order_id": o_id, "orderstatus": status, "message": order_data.get("message")}
                else:
                    response = {"order_id": o_id, "orderstatus": "FAILED", "message": "Failed to get history"}
            else:
                response = {"orderstatus": "FAILED", "message": res.get("message") if res else "Order failed"}
        except Exception as ex:
            self.write_logs("error", f"[_place_order_fyers] Exception: {str(ex)}")
            response = {"orderstatus": "FAILED", "message": str(ex)}

        # duration = time.time() - start_time
        # self.write_logs("info", f"[_place_order_fyers] Finished for {account_id} in {duration:.2f}s")
        
        final_status = response.get("orderstatus", "FAILED").upper()
        # Note: Fyers 2 = Filled, but we map it via self.fyers_ord_res
        is_success = final_status in ["SUCCESS", "FILLED", "COMPLETE", "COMPLETED", "TRADED"]
        return {
            "order_id": response.get("order_id"),
            "placed_qty": quantity,
            "status": "SUCCESS" if is_success else "FAILED",
            "message": response.get("message") or final_status,
            "error": response.get("error") if not is_success else None
        }
            
        # return response    
            
    def get_angel_orderbook(self, brokerobject):
        max_retries = 20
        for attempt in range(1, max_retries + 1):
            try:
                response = brokerobject.orderBook()
                # print("get_angel_orderbook response === ",response)
                return response['data']
            except Exception as ex:
                self.write_logs(
                    "error",
                    f"[DEBUG][get_angel_orderbook] attempt [{attempt}/{max_retries}]: {ex}"
                )
                time.sleep(self.error_buffer)

        # Explicit failure
        self.write_logs(
            "error",
            "[DEBUG][get_angel_orderbook] Failed to fetch orderbook after all retries"
        )
        return None
    
    
    def get_angel_postions(self, brokerobject):
        max_retries = 20
    
        for attempt in range(1, max_retries + 1):
            try:
                response = brokerobject.position()

                return response['data']

            except Exception as ex:
                self.write_logs(
                    "error",
                    f"[DEBUG][get_angel_positions] attempt [{attempt}/{max_retries}]: {ex}"
                )
                time.sleep(self.error_buffer)
            
        # Explicit failure
        self.write_logs(
            "error",
            "[DEBUG][get_angel_positions] Failed to fetch orderbook after all retries"
        )
        return None
                  
    def _place_order_angel(self, account_id, position_type, broker_obj: SmartConnect, exchange_token: int, exchange: str, quantity: int, product: str, order_type: str, transaction_type: str, variety="regular", validity=None, price: float = None, trigger_price: float = None, disclosed_quantity: int = None, tag: str = None):
        # start_time = time.time()
        # self.write_logs("info", f"[_place_order_angel] Started for {account_id}, Token: {exchange_token}")
        
        instrument_item = self._get_instrument_data("angel", exchange, exchange_token)
        if not instrument_item:
            self.write_logs("error", f"[_place_order_angel] Invalid exchange token: {exchange_token}")
            return {"status": "FAILED", "message": "invalid_exchange_token", "exchange_token": exchange_token}

        # Variety mapping
        v_lower = variety.lower()
        if v_lower in ["regular", "normal"]: _variety = "NORMAL"
        elif v_lower == "co": _variety = "ROBO"
        else: return {"status": "FAILED", "message": "invalid_variety"}

        _quantity = (int(quantity) * int(instrument_item["lotsize"])) if exchange in ["MCX", "NFO", "BFO"] else quantity
        
        # Product mapping
        prod_map = {"MIS": "INTRADAY", "CNC": "DELIVERY" if exchange in ["NSE", "BSE"] else "CARRYFORWARD", "CO": "CO", "BO": "BO", "NRML": "CARRYFORWARD"}
        _product = prod_map.get(product.upper(), "INTRADAY")

        if position_type.upper() == "CLOSE":
            try:
                netwise = self.get_angel_postions(broker_obj)
                if netwise:
                    position = next((pos for pos in netwise if ((int(pos.get("symboltoken")) == int(instrument_item['token'])) and (pos.get("producttype") == _product))), None)
                    if position:
                        p_qty_net = int(position["netqty"])
                        if abs(p_qty_net) == 0:
                            return {"status": "FAILED", "message": "Available qty is 0"}
                        if (p_qty_net < 0 and transaction_type.upper() == "SELL") or (p_qty_net > 0 and transaction_type.upper() == "BUY"):
                            return {"status": "FAILED", "message": "Side mismatch for close"}
                        if abs(p_qty_net) < _quantity:
                            _quantity = abs(p_qty_net)
                    else:
                        return {"status": "FAILED", "message": "No active position found"}
                else:
                    return {"status": "FAILED", "message": "Failed to fetch positions"}
            except Exception as e:
                self.write_logs("error", f"[_place_order_angel] Position fetch failed: {str(e)}")

        # Order type mapping
        o_type_upper = order_type.upper()
        if o_type_upper in ["LIMIT", "ADJUST"]: _order_type = "LIMIT"
        elif o_type_upper == "MARKET": _order_type = "MARKET"
        elif o_type_upper == "SL": _order_type = "STOPLOSS_LIMIT"
        elif o_type_upper == "SL-M": _order_type = "STOPLOSS_MARKET"
        else: return {"status": "FAILED", "message": "invalid_order_type"}

        order_payload = {
            "variety": _variety,
            "tradingsymbol": instrument_item['symbol'],
            "symboltoken": int(instrument_item['token']),
            "transactiontype": transaction_type.upper(),
            "exchange": exchange.upper(),
            "ordertype": _order_type,
            "producttype": _product,
            "duration": validity if validity else "DAY",
            "price": float(price) if price else 0.0,
            "quantity": int(_quantity)
        }
        if trigger_price: order_payload["triggerprice"] = trigger_price
        if disclosed_quantity: order_payload["disclosedquantity"] = disclosed_quantity
        if tag: order_payload["ordertag"] = tag[:19]

        response = {}
        try:
            o_id = None
            self.write_logs("info", f"[_place_order_angel] ID:{account_id} - Order placed reponse : {order_payload}")
            for _ in range(self.order_retry):
                o_id = broker_obj.placeOrder(order_payload)
                self.write_logs("info", f"[_place_order_angel] ID:{account_id} - Order placed reponse : {o_id}")
                if o_id: break
            
            if o_id:
                time.sleep(self.order_status_check_interval)
                history = self.get_angel_orderbook(broker_obj)
                order_data = next((o for o in history if o.get("orderid") == o_id), None) if history else None
                
                if order_data:
                    status = order_data.get("orderstatus", "").upper()
                    self.write_logs("info", f"[_place_order_angel] Order placed: {o_id}, Status: {status}")
                    
                    if (o_type_upper in ["LIMIT", "ADJUST"]) and (status not in ["REJECTED", "TRADED", "FILLED", "COMPLETE", "FAILED", "CANCELLED"]):
                        modify_res, final_oid = self._modify_order("angel", account_id, broker_obj, instrument_item, o_id, quantity, exchange_token, exchange, product, order_type, transaction_type, position_type, validity, price, trigger_price, variety=_variety)
                        if modify_res == "MARKET_SUCCESS":
                            response = {"order_id": final_oid, "orderstatus": "SUCCESS"}
                        elif "ZOMBIE_ORDER_MODIFIER" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "SUCCESS", "message": f"Started {modify_res}"}
                        elif "FAILED" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "FAILED", "message": modify_res}
                        else:
                            response = {"order_id": o_id, "orderstatus": modify_res}
                    else:
                        response = {"order_id": o_id, "orderstatus": status, "message": order_data.get("text")}
                else:
                    response = {"order_id": o_id, "orderstatus": "FAILED", "message": "Order data not found in book"}
            else:
                response = {"orderstatus": "FAILED", "message": "Order placement failed, no ID"}
        except Exception as ex:
            self.write_logs("error", f"[_place_order_angel] Exception: {str(ex)}")
            response = {"orderstatus": "FAILED", "message": str(ex)}

        # duration = time.time() - start_time
        # self.write_logs("info", f"[_place_order_angel] Finished for {account_id} in {duration:.2f}s")
        
        final_status = response.get("orderstatus", "FAILED").upper()
        is_success = final_status in ["SUCCESS", "TRADED", "FILLED", "COMPLETE", "COMPLETED"]
        return {
            "order_id": response.get("order_id"),
            "placed_qty": quantity,
            "status": "SUCCESS" if is_success else "FAILED",
            "message": response.get("message") or final_status,
            "error": response.get("error") if not is_success else None
        }
    def _place_order_finvasia(self, account_id, position_type, broker_obj: ShoonyaApiPy, exchange_token: int, exchange: str, quantity: int, product: str, order_type: str, transaction_type: str, variety="regular", validity=None, price: float = None, trigger_price: float = None, disclosed_quantity: int = None, tag: str = None):
        # start_time = time.time()
        # self.write_logs("info", f"[_place_order_finvasia] Started for {account_id}, Token: {exchange_token}")
        
        instrument_item = broker_obj.get_security_info(exchange, str(int(exchange_token)))
        if not instrument_item:
            self.write_logs("error", f"[_place_order_finvasia] Invalid exchange token: {exchange_token}")
            return {"status": "FAILED", "message": "invalid_exchange_token", "exchange_token": exchange_token}

        _quantity = (quantity * int(instrument_item["ls"])) if exchange in ["NFO", "BFO"] else quantity
        
        # Product mapping
        prod_map = {"MIS": "I", "CNC": "C" if exchange in ["NSE", "BSE"] else "M", "CO": "H", "BO": "B", "NRML": "M"}
        _product = prod_map.get(product.upper(), "I")

        if position_type.upper() == "CLOSE":
            try:
                positions = broker_obj.get_positions()
                if isinstance(positions, list) and positions:
                    p_token = str(instrument_item['token'])
                    position = next((pos for pos in positions if (str(pos.get("token")) == p_token and pos.get("prd") == _product)), None)
                    if position:
                        p_qty_net = int(position["netqty"])
                        if abs(p_qty_net) == 0:
                            return {"status": "FAILED", "message": "Available qty is 0"}
                        if (p_qty_net < 0 and transaction_type.upper() == "SELL") or (p_qty_net > 0 and transaction_type.upper() == "BUY"):
                            return {"status": "FAILED", "message": "Side mismatch for close"}
                        if abs(p_qty_net) < _quantity:
                            _quantity = abs(p_qty_net)
                    else:
                        return {"status": "FAILED", "message": "No active position found"}
            except Exception as e:
                self.write_logs("error", f"[_place_order_finvasia] Position fetch failed: {str(e)}")

        # Order type mapping
        o_type_upper = order_type.upper()
        if o_type_upper in ["LIMIT", "ADJUST"]: _order_type = "LMT"
        elif o_type_upper == "MARKET": _order_type = "MKT"
        elif o_type_upper == "SL": _order_type = "SL-LMT"
        elif o_type_upper == "SL-M": _order_type = "SL-MKT"
        else: return {"status": "FAILED", "message": "invalid_order_type"}

        order_payload = {
            "buy_or_sell": "B" if transaction_type.upper() == "BUY" else "S",
            "product_type": _product,
            "exchange": exchange,
            "tradingsymbol": instrument_item["tsym"],
            "quantity": int(_quantity),
            "price_type": _order_type,
            "price": float(price) if price else 0.0,
            "retention": validity if validity else "DAY",
            "remarks": tag[:19] if tag else "",
            "discloseqty": int(disclosed_quantity) if disclosed_quantity else 0
        }
        if trigger_price: order_payload["trigger_price"] = trigger_price

        response = {}
        try:
            self.write_logs("info", f"[_place_order_finvasia] ID:{account_id} - Order placed payload : {order_payload}")
            res = broker_obj.place_order(**order_payload)
            self.write_logs("info", f"[_place_order_finvasia] ID:{account_id} - Order placed response : {res}")
            if res and res.get("stat") == "Ok" and "norenordno" in res:
                time.sleep(self.order_status_check_interval)
                o_id = res["norenordno"]
                history = broker_obj.get_order_book()
                history = next((his for his in history if ((int(his.get("norenordno")) == int(o_id)))), None)
                if history:
                    # Find terminal status in history
                    term_status = ["COMPLETE", "REJECT", "REJECTED", "FAILED", "CANCELLED"]
                    order_data = history
                    status = order_data.get("status", "").upper()
                    # self.write_logs("info", f"[_place_order_finvasia] Order placed: {o_id}, Status: {status}")
                    
                    if (o_type_upper in ["LIMIT", "ADJUST"]) and (status not in term_status):
                        modify_res, final_oid = self._modify_order("finvasia", account_id, broker_obj, instrument_item, o_id, quantity, exchange_token, exchange, product, order_type, transaction_type, position_type, validity, price, trigger_price)
                        if modify_res == "MARKET_SUCCESS":
                            response = {"order_id": final_oid, "orderstatus": "SUCCESS"}
                        elif "ZOMBIE_ORDER_MODIFIER" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "SUCCESS", "message": f"Started {modify_res}"}
                        elif "FAILED" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "FAILED", "message": modify_res}
                        else:
                            response = {"order_id": o_id, "orderstatus": modify_res}
                    else:
                        response = {"order_id": o_id, "orderstatus": status, "message": order_data.get("rejreason")}
                else:
                    response = {"order_id": o_id, "orderstatus": "FAILED", "message": "Failed to get history"}
            else:
                response = {"orderstatus": "FAILED", "message": str(res).replace("\n", "|").replace(",", "-") if res else "Order failed"}
        except Exception as ex:
            self.write_logs("error", f"[_place_order_finvasia] Exception: {str(ex)}")
            response = {"orderstatus": "FAILED", "message": str(ex)}

        # duration = time.time() - start_time
        # self.write_logs("info", f"[_place_order_finvasia] Finished for {account_id} in {duration:.2f}s")
        
        final_status = response.get("orderstatus", "FAILED").upper()
        is_success = final_status in ["SUCCESS", "COMPLETE", "COMPLETED", "TRADED", "FILLED"]
        return {
            "order_id": response.get("order_id"),
            "placed_qty": quantity,
            "status": "SUCCESS" if is_success else "FAILED",
            "message": response.get("message") or final_status,
            "error": response.get("error") if not is_success else None
        }
      
        
        # return response   
    fivepaisa_status = {"Rejected By 5P":"REJECTED",
                        "Fully Executed":"COMPLETED",
                        "Xmitted":"REJECTED",
                        "Rejected by Exch":"REJECTED",
                        "Cancelled":"CANCELLED",
                        "Pending":"PENDING",
                        "Failed":"FAILED"
                        }
    def _place_order_5paisa(self, account_id, position_type, broker_obj: FivepaisaBroker, exchange_token: int, exchange: str, quantity: int, product: str, order_type: str, transaction_type: str, lotsize: str, variety="regular", validity=None, price: float = None, trigger_price: float = None, disclosed_quantity: int = None, tag: str = None):
        # start_time = time.time()
        # self.write_logs("info", f"[_place_order_5paisa] Started for {account_id}, Token: {exchange_token}")
        
        _quantity = (quantity * int(lotsize)) if exchange in ["NFO", "BFO"] else quantity
        _exchange = exchange[0]
        
        # Product mapping
        prod_map = {"MIS": "MIS", "CNC": "NRML", "NRML": "NRML"}
        _product = prod_map.get(product.upper(), "MIS")

        if position_type.upper() == "CLOSE":
            try:
                netwise = broker_obj.get_netwise_positions()
                if netwise:
                    pos_prod = "I" if product == "MIS" else "D"
                    position = next((pos for pos in netwise if (int(pos.get("ScripCode")) == int(exchange_token) and pos.get("OrderFor") == pos_prod)), None)
                    if position:
                        p_qty_net = int(position["NetQty"])
                        if abs(p_qty_net) == 0:
                            return {"status": "FAILED", "message": "Available qty is 0"}
                        if (p_qty_net < 0 and transaction_type.upper() == "SELL") or (p_qty_net > 0 and transaction_type.upper() == "BUY"):
                            return {"status": "FAILED", "message": "Side mismatch for close"}
                        if abs(p_qty_net) < _quantity:
                            _quantity = abs(p_qty_net)
                    else:
                        return {"status": "FAILED", "message": "No active position found"}
            except Exception as e:
                self.write_logs("error", f"[_place_order_5paisa] Position fetch failed: {str(e)}")

        # Order type mapping
        o_type_upper = order_type.upper()
        if o_type_upper in ["LIMIT", "ADJUST"]: _order_type = "LIMIT"
        elif o_type_upper == "MARKET": _order_type = "MARKET"
        else: return {"status": "FAILED", "message": "invalid_order_type"}

        order_payload = {
            "scrip_code": str(exchange_token),
            "exchange": _exchange,
            "exchange_type": "D",
            "transaction_type": "B" if transaction_type.upper() == "BUY" else "S",
            "product_type": _product,
            "order_type": _order_type,
            "order_tag": tag[:19] if tag else str(int(time.time())),
            "quantity": int(_quantity),
            "price": float(price) if price else 0.0,
            "StopLossPrice": 0,
            "DisQty": int(disclosed_quantity) if disclosed_quantity else 0
        }

        response = {}
        try:
            self.write_logs("info", f"[_place_order_5paisa] ID:{account_id} Order placed payload : {order_payload}")
            res = broker_obj.place_order(**order_payload)
            self.write_logs("info", f"[_place_order_5paisa] ID:{account_id} Order placed response : {res}")
            time.sleep(0.5)
            if res and "BrokerOrderID" in res:
                o_id = res["BrokerOrderID"]
                history = broker_obj.get_order_history(o_id)
                if history:
                    raw_status = history.get("OrderStatus", "Failed")
                    status = self.fivepaisa_status.get(raw_status, raw_status).upper()
                    # self.write_logs("info", f"[_place_order_5paisa] Order placed: {o_id}, Status: {status}")
                    
                    if (o_type_upper in ["LIMIT", "ADJUST"]) and (status not in ["REJECTED", "COMPLETE", "COMPLETED", "FAILED", "CANCELLED"]):
                        modify_res, final_oid = self._modify_order("5paisa", account_id, broker_obj, {}, o_id, quantity, exchange_token, exchange, product, order_type, transaction_type, position_type, validity, price, trigger_price)
                        if modify_res == "MARKET_SUCCESS":
                            response = {"order_id": final_oid, "orderstatus": "SUCCESS"}
                        elif "ZOMBIE_ORDER_MODIFIER" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "SUCCESS", "message": f"Started {modify_res}"}
                        elif "FAILED" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "FAILED", "message": modify_res}
                        else:
                            response = {"order_id": o_id, "orderstatus": modify_res}
                    else:
                        response = {"order_id": o_id, "orderstatus": status, "message": history.get("Reason")}
                else:
                    response = {"order_id": o_id, "orderstatus": "FAILED", "message": "Failed to get history"}
            else:
                response = {"orderstatus": "FAILED", "message": str(res).replace("\n", "|").replace(",","-") if res else "Order failed"}
        except Exception as ex:
            self.write_logs("error", f"[_place_order_5paisa] Exception: {str(ex)}")
            response = {"orderstatus": "FAILED", "message": str(ex)}

        # duration = time.time() - start_time
        # self.write_logs("info", f"[_place_order_5paisa] Finished for {account_id} in {duration:.2f}s")
        
        final_status = response.get("orderstatus", "FAILED").upper()
        is_success = final_status in ["SUCCESS", "COMPLETE", "COMPLETED", "TRADED", "FILLED"]
        return {
            "order_id": response.get("order_id"),
            "placed_qty": quantity,
            "status": "SUCCESS" if is_success else "FAILED",
            "message": response.get("message") or final_status,
            "error": response.get("error") if not is_success else None
        }
      
        
    def _place_order_dhan(self, account_id, position_type, broker_obj: dhanhq, exchange_token: int, exchange: str, quantity: int, product: str, order_type: str, transaction_type: str, lotsize: str, variety="regular", validity=None, price: float = None, trigger_price: float = None, disclosed_quantity: int = None, tag: str = None):
        # start_time = time.time()
        # self.write_logs("info", f"[_place_order_dhan] Started for {account_id}, Token: {exchange_token}")
        
        _quantity = (quantity * int(lotsize)) if exchange in ["NFO", "BFO"] else quantity
        
        exch_map = {"NSE": "NSE_EQ", "BSE": "BSE_EQ", "MCX": "MCX_COMM", "NFO": "NSE_FNO", "BFO": "BSE_FNO"}
        _exchange = exch_map.get(exchange.upper())
        if not _exchange:
            return {"status": "FAILED", "message": f"invalid exchange: {exchange}"}

        # Order type mapping
        o_type_upper = order_type.upper()
        if o_type_upper in ["LIMIT", "ADJUST"]: _order_type = "LIMIT"
        elif o_type_upper == "MARKET": _order_type = "MARKET"
        else: return {"status": "FAILED", "message": "invalid_order_type"}

        # Product mapping
        if product.upper() == "MIS": _product = "INTRADAY"
        elif product.upper() == "CNC": _product = "MARGIN" if exchange in ["MCX", "NFO", "BFO"] else "CNC"
        else: return {"status": "FAILED", "message": "invalid_product"}

        if position_type.upper() == "CLOSE":
            try:
                pos_res = broker_obj.get_positions()
                if pos_res and ('data' in pos_res) and pos_res['data']:
                    netwise = pos_res['data']
                    position = next((pos for pos in netwise if (int(pos.get("securityId")) == int(exchange_token) and pos.get("productType") == _product)), None)
                    if position:
                        p_qty_net = int(position["netQty"])
                        if abs(p_qty_net) == 0:
                            return {"status": "FAILED", "message": "Available qty is 0"}
                        if (p_qty_net < 0 and transaction_type.upper() == "SELL") or (p_qty_net > 0 and transaction_type.upper() == "BUY"):
                            return {"status": "FAILED", "message": "Side mismatch for close"}
                        if abs(p_qty_net) < _quantity:
                            _quantity = abs(p_qty_net)
                    else:
                        return {"status": "FAILED", "message": "No active position found"}
            except Exception as e:
                self.write_logs("error", f"[_place_order_dhan] Position fetch failed: {str(e)}")

        order_payload = {
            "security_id": str(exchange_token),
            "exchange_segment": _exchange,
            "transaction_type": transaction_type.upper(),
            "product_type": _product,
            "order_type": _order_type,
            "tag": tag[:19] if tag else str(int(time.time())),
            "quantity": int(_quantity),
            "price": float(price) if price else 0.0,
        }
        if disclosed_quantity: order_payload["disclosed_quantity"] = int(disclosed_quantity)
        if trigger_price: order_payload["trigger_price"] = float(trigger_price)

        response = {}
        try:
            self.write_logs("info", f"[_place_order_dhan] ID:{account_id} Order placed payload : {order_payload}")
            res = broker_obj.place_order(**order_payload)
            self.write_logs("info", f"[_place_order_dhan] ID:{account_id} Order placed response : {res}")
            if res and res.get("status") == "success" and 'data' in res and 'orderId' in res['data']:
                time.sleep(self.order_status_check_interval)
                o_id = res['data']['orderId']
                history_res = broker_obj.get_order_by_id(o_id)
                if history_res and 'data' in history_res:
                    history = history_res['data'] if isinstance(history_res['data'], list) else [history_res['data']]
                    term_status = ["COMPLETE", "REJECT", "REJECTED", "FAILED", "CANCELLED", "TRADED"]
                    order_data = next((o for o in history if o.get("orderStatus") in term_status), history[-1])
                    status = order_data.get("orderStatus", "").upper()
                    # self.write_logs("info", f"[_place_order_dhan] Order placed: {o_id}, Status: {status}")
                    
                    if (o_type_upper in ["LIMIT", "ADJUST"]) and (status not in term_status):
                        modify_res, final_oid = self._modify_order("dhan", account_id, broker_obj, {}, o_id, quantity, exchange_token, exchange, product, order_type, transaction_type, position_type, validity, price, trigger_price)
                        if modify_res == "MARKET_SUCCESS":
                            response = {"order_id": final_oid, "orderstatus": "SUCCESS"}
                        elif "FAILED" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "FAILED", "message": modify_res}
                        elif "ZOMBIE_ORDER_MODIFIER" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "SUCCESS", "message": f"Started {modify_res}"}
                        else:
                            response = {"order_id": o_id, "orderstatus": modify_res}
                    else:
                        response = {"order_id": o_id, "orderstatus": status, "message": order_data.get("omsErrorDescription")}
                else:
                    response = {"order_id": o_id, "orderstatus": "FAILED", "message": "Failed to get history"}
            else:
                response = {"orderstatus": "FAILED", "message": str(res).replace("\n", "|").replace(",","-") if res else "Order failed"}
        except Exception as ex:
            self.write_logs("error", f"[_place_order_dhan] Exception: {str(ex)}")
            response = {"orderstatus": "FAILED", "message": str(ex)}

        # duration = time.time() - start_time
        # self.write_logs("info", f"[_place_order_dhan] Finished for {account_id} in {duration:.2f}s")
        
        final_status = response.get("orderstatus", "FAILED").upper()
        is_success = final_status in ["SUCCESS", "COMPLETE", "COMPLETED", "TRADED", "FILLED"]
        return {
            "order_id": response.get("order_id"),
            "placed_qty": quantity,
            "status": "SUCCESS" if is_success else "FAILED",
            "message": response.get("message") or final_status,
            "error": response.get("error") if not is_success else None
        }
      
        
        # return response   
    def _place_order_xts(self, account_id, position_type, broker_obj, exchange_token: int, exchange: str, quantity: int, product: str, order_type: str, transaction_type: str, lotsize: int, variety="regular", validity=None, price: float = None, trigger_price: float = None, disclosed_quantity: int = None, tag: str = None):
        start_time = time.time()
        self.write_logs("info", f"[_place_order_xts] Started for {account_id}, Token: {exchange_token}")
        
        # if not self.check_import_availability("Connect"):
        #     return {"status": "FAILED", "message": "xts connect Module does not exist", "error": "NO_XTS_CONNECT"}

        _quantity = (quantity * int(lotsize)) if exchange in ["NFO", "BFO"] else quantity
        exch_map = {"NSE": "NSECM", "BSE": "BSECM", "NFO": "NSEFO", "BFO": "BSEFO", "MCX": "MCXFO"}
        _exchange = exch_map.get(exchange.upper())
        if not _exchange:
            return {"status": "FAILED", "message": f"invalid exchange: {exchange}"}

        # Order type mapping
        o_type_upper = order_type.upper()
        if o_type_upper in ["LIMIT", "ADJUST"]: _order_type = "LIMIT"
        elif o_type_upper == "MARKET": _order_type = "MARKET"
        elif o_type_upper == "SL": _order_type = "STOPLIMIT"
        elif o_type_upper == "SL-M": _order_type = "STOPMARKET"
        else: return {"status": "FAILED", "message": "invalid_order_type"}

        # Product mapping
        if product.upper() == "MIS": _product = "MIS"
        elif product.upper() == "CNC": _product = "CNC" if exchange in ["NSE", "BSE"] else "NRML"
        else: return {"status": "FAILED", "message": "invalid_product"}

        if position_type.upper() == "CLOSE":
            try:
                pos_res = broker_obj.get_position_netwise(clientID="*****")
                if pos_res and "result" in pos_res and "positionList" in pos_res['result']:
                    netwise = pos_res['result']["positionList"]
                    position = next((pos for pos in netwise if (int(pos.get("ExchangeInstrumentId")) == int(exchange_token) and pos.get("ProductType") == _product)), None)
                    if position:
                        p_qty = int(position["Quantity"])
                        if abs(p_qty) == 0:
                            return {"status": "FAILED", "message": "Available qty is 0"}
                        if (p_qty < 0 and transaction_type.upper() == "SELL") or (p_qty > 0 and transaction_type.upper() == "BUY"):
                            return {"status": "FAILED", "message": "Side mismatch for close"}
                        if abs(p_qty) < _quantity:
                            _quantity = abs(p_qty)
                    else:
                        return {"status": "FAILED", "message": "No active position found"}
            except Exception as e:
                self.write_logs("error", f"[_place_order_xts] Position fetch failed: {str(e)}")

        order_payload = {
            "exchangeSegment": _exchange,
            "exchangeInstrumentID": int(exchange_token),
            "productType": _product,
            "orderType": _order_type,
            "orderSide": transaction_type.upper(),
            "timeInForce": "DAY",
            "disclosedQuantity": int(disclosed_quantity) if disclosed_quantity else 0,
            "orderQuantity": int(_quantity),
            "limitPrice": float(price) if price else 0.0,
            "stopPrice": float(trigger_price) if trigger_price else 0.0,
            "orderUniqueIdentifier": tag[:20] if tag else f"OE{int(time.time())}"[:20],
            "clientID": "*****"
        }
        # print("order_payload == ",order_payload)
        response = {}
        try:
            self.write_logs("info", f"[_place_order_xts] ID:{account_id} Order placed payload : {order_payload}")
            res = broker_obj.place_order(**order_payload)
            time.sleep(1)
            self.write_logs("info", f"[_place_order_xts] ID:{account_id} Order placed response : {res}")
            if res and "result" in res and res['result'].get("AppOrderID"):
                o_id = res['result']["AppOrderID"]
                history_res = broker_obj.get_order_history(o_id, clientID="*****")
                if history_res and "result" in history_res and history_res['result']:
                    history = history_res['result']
                    term_status = ["COMPLETE", "REJECT", "REJECTED", "FAILED", "CANCELLED", "FILLED"]
                    order_data = next((o for o in history if o.get("OrderStatus", "").upper() in term_status), history[-1])
                    status = order_data.get("OrderStatus", "").upper()
                    # self.write_logs("info", f"[_place_order_xts] Order placed: {o_id}, Status: {status}")
                    
                    if (o_type_upper in ["LIMIT", "ADJUST"]) and (status not in term_status):
                        modify_res, final_oid = self._modify_order("xts", account_id, broker_obj, {}, o_id, quantity, exchange_token, exchange, product, order_type, transaction_type, position_type, validity, price, trigger_price)
                        if modify_res == "MARKET_SUCCESS":
                            response = {"order_id": final_oid, "orderstatus": "SUCCESS"}
                        elif "FAILED" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "FAILED", "message": modify_res}
                        elif "ZOMBIE_ORDER_MODIFIER" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "SUCCESS", "message": f"Started {modify_res}"}
                        else:
                            response = {"order_id": o_id, "orderstatus": modify_res}
                    else:
                        response = {"order_id": o_id, "orderstatus": status, "message": order_data.get("CancelRejectReason")}
                else:
                    response = {"order_id": o_id, "orderstatus": "FAILED", "message": "Failed to get history"}
            else:
                response = {"orderstatus": "FAILED", "message": str(res).replace("\n", "|").replace(",","-") if res else "Order failed"}
        except Exception as ex:
            self.write_logs("error", f"[_place_order_xts] Exception: {str(ex)}")
            response = {"orderstatus": "FAILED", "message": str(ex)}

        # duration = time.time() - start_time
        # self.write_logs("info", f"[_place_order_xts] Finished for {account_id} in {duration:.2f}s")
        
        final_status = response.get("orderstatus", "FAILED").upper()
        is_success = final_status in ["SUCCESS", "COMPLETE", "COMPLETED", "TRADED", "FILLED"]
        return {
            "order_id": response.get("order_id"),
            "placed_qty": quantity,
            "status": "SUCCESS" if is_success else "FAILED",
            "message": response.get("message") or final_status,
            "error": response.get("error") if not is_success else None
        }
      
        
        # return response   
    
    def _place_order_findoc(self, account_id, position_type, broker_obj: FindocAPI, exchange_token: int, exchange: str, quantity: int, product: str, order_type: str, transaction_type: str, lotsize: int, variety="regular", validity=None, price: float = None, trigger_price: float = None, disclosed_quantity: int = None, tag: str = None):
        # start_time = time.time()
        # self.write_logs("info", f"[_place_order_findoc] Started for {account_id}, Token: {exchange_token}")
        
        _quantity = (quantity * int(lotsize)) if exchange in ["NFO", "BFO"] else quantity
        exch_map = {"NSE": "NSECM", "BSE": "BSECM", "NFO": "NSEFO", "BFO": "BSEFO", "MCX": "MCXFO"}
        _exchange = exch_map.get(exchange.upper())
        if not _exchange:
            return {"status": "FAILED", "message": f"invalid exchange: {exchange}"}

        # Order type mapping
        o_type_upper = order_type.upper()
        if o_type_upper in ["LIMIT", "ADJUST"]: _order_type = "LIMIT"
        elif o_type_upper == "MARKET": _order_type = "MARKET"
        elif o_type_upper == "SL": _order_type = "STOPLIMIT"
        elif o_type_upper == "SL-M": _order_type = "STOPMARKET"
        else: return {"status": "FAILED", "message": "invalid_order_type"}

        # Product mapping
        if product.upper() == "MIS": _product = "MIS"
        elif product.upper() == "CNC": _product = "CNC" if exchange in ["NSE", "BSE"] else "NRML"
        else: return {"status": "FAILED", "message": "invalid_product"}
        _product = "NRML"
        if position_type.upper() == "CLOSE":
            try:
                pos_res = broker_obj.get_positions(_type="NetWise")
                if pos_res and "positionList" in pos_res:
                    netwise = pos_res["positionList"]
                    position = next((pos for pos in netwise if (int(pos.get("ExchangeInstrumentId")) == int(exchange_token) and pos.get("ProductType") == _product)), None)
                    if position:
                        p_qty = int(position["Quantity"])
                        if abs(p_qty) == 0:
                            return {"status": "FAILED", "message": "Available qty is 0"}
                        if (p_qty < 0 and transaction_type.upper() == "SELL") or (p_qty > 0 and transaction_type.upper() == "BUY"):
                            return {"status": "FAILED", "message": "Side mismatch for close"}
                        if abs(p_qty) < _quantity:
                            _quantity = abs(p_qty)
                    else:
                        return {"status": "FAILED", "message": "No active position found"}
            except Exception as e:
                self.write_logs("error", f"[_place_order_findoc] Position fetch failed: {str(e)}")

        order_payload = {
            "exchange": _exchange,
            "exchange_token": int(exchange_token),
            "transaction_type": transaction_type.upper(),
            "product_type": _product,
            "order_type": _order_type,
            "quantity": int(_quantity),
            "price": float(price) if price else 0.0,
            "order_tag": tag[:19] if tag else f"OE{int(time.time())}"[:19]
        }

        response = {}
        try:
            self.write_logs("info", f"[_place_order_findoc] ID:{account_id} Order placed payload : {order_payload}")
            res = broker_obj.place_order(**order_payload)
            self.write_logs("info", f"[_place_order_findoc] ID:{account_id} Order placed response : {res}")
            if res and res.get("AppOrderID"):
                o_id = res["AppOrderID"]
                time.sleep(1)
                history_res = broker_obj.get_order_history_id(o_id)
                if history_res:
                    history = history_res
                    # print("findoc = history = ",history)
                    term_status = ["COMPLETE", "REJECT", "REJECTED", "FAILED", "CANCELLED", "FILLED"]
                    order_data = next((o for o in history if o.get("OrderStatus", "").upper() in term_status), history[-1])
                    status = order_data.get("OrderStatus", "").upper()
                    # self.write_logs("info", f"[_place_order_findoc] Order placed: {o_id}, Status: {status}")
                    # if (_order_type == "MARKET") and status in ["PENDINGNEW","PENDING","NEW"]:
                        # self.write_logs("warning",f"Order Status : {status} as it is market changing it to COMPLETE")
                    #     status = "COMPLETE"
                    if (o_type_upper in ["LIMIT", "ADJUST"]) and (status not in term_status):
                        modify_res, final_oid = self._modify_order("findoc", account_id, broker_obj, {}, o_id, quantity, exchange_token, exchange, product, order_type, transaction_type, position_type, validity, price, trigger_price)
                        if modify_res == "MARKET_SUCCESS":
                            response = {"order_id": final_oid, "orderstatus": "SUCCESS"}
                        elif "FAILED" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "FAILED", "message": modify_res}
                        elif "ZOMBIE_ORDER_MODIFIER" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "SUCCESS", "message": f"Started {modify_res}"}
                        else:
                            response = {"order_id": o_id, "orderstatus": modify_res}
                    else:
                        response = {"order_id": o_id, "orderstatus": status, "message": order_data.get("CancelRejectReason")}
                else:
                    response = {"order_id": o_id, "orderstatus": "FAILED", "message": "Failed to get history"}
            else:
                response = {"orderstatus": "FAILED", "message": str(res).replace("\n", "|").replace(",","-") if res else "Order failed"}
        except Exception as ex:
            self.write_logs("error", f"[_place_order_findoc] Exception: {str(ex)}")
            response = {"orderstatus": "FAILED", "message": str(ex)}

        # duration = time.time() - start_time
        # self.write_logs("info", f"[_place_order_findoc] Finished for {account_id} in {duration:.2f}s")
        
        final_status = response.get("orderstatus", "FAILED").upper()
        is_success = final_status in ["SUCCESS", "COMPLETE", "COMPLETED", "TRADED", "FILLED"]
        return {
            "order_id": response.get("order_id"),
            "placed_qty": quantity,
            "status": "SUCCESS" if is_success else "FAILED",
            "message": response.get("message") or final_status,
            "error": response.get("error") if not is_success else None
        }
      
    def _place_order_mo(self, account_id, position_type, broker_obj: MOFSLOPENAPI, exchange_token: int, exchange: str, quantity: int, product: str, order_type: str, transaction_type: str, lotsize: int, variety="regular", validity=None, price: float = None, trigger_price: float = None, disclosed_quantity: int = None, tag: str = None):
        # start_time = time.time()
        # self.write_logs("info", f"[_place_order_findoc] Started for {account_id}, Token: {exchange_token}")
        
        _quantity = quantity
        # _quantity = (quantity * int(lotsize)) if exchange in ["NFO", "BFO"] else quantity
        exch_map = {"NSE": "NSECM", "BSE": "BSECM", "NFO": "NSEFO", "BFO": "BSEFO", "MCX": "MCX"}
        _exchange = exch_map.get(exchange.upper())
        if not _exchange:
            return {"status": "FAILED", "message": f"invalid exchange: {exchange}"}

        # Order type mapping
        o_type_upper = order_type.upper()
        if o_type_upper in ["LIMIT", "ADJUST"]: _order_type = "LIMIT"
        elif o_type_upper == "MARKET": _order_type = "MARKET"
        elif o_type_upper == "STOPLOSS": _order_type = "STOPLIMIT"
        elif o_type_upper == "STOPLOSS-MARKET": _order_type = "STOPMARKET"
        else: return {"status": "FAILED", "message": "invalid_order_type"}

        # Product mapping
        if product.upper() == "MIS": _product = "VALUEPLUS"
        elif product.upper() == "CNC": _product = "DELIVERY" if exchange in ["NSE", "BSE"] else "NORMAL"
        else: return {"status": "FAILED", "message": "invalid_product"}

        if position_type.upper() == "CLOSE":
            try:
                pos_res = broker_obj.GetPosition(account_id)
                if pos_res and ("data" in pos_res) and pos_res['data']:
                    netwise = pos_res["data"]
                    position = next((pos for pos in netwise if (int(pos.get("symboltoken")) == int(exchange_token) and pos.get("productname").upper() == _product)), None)
                    if position:
                        position["Quantity"] = position["buyquantity"] - position["sellquantity"]
                        p_qty = int(position["Quantity"])
                        if abs(p_qty) == 0:
                            return {"status": "FAILED", "message": "Available qty is 0"}
                        if (p_qty < 0 and transaction_type.upper() == "SELL") or (p_qty > 0 and transaction_type.upper() == "BUY"):
                            return {"status": "FAILED", "message": "Side mismatch for close"}
                        if abs(p_qty) < _quantity:
                            _quantity = abs(p_qty)
                    else:
                        return {"status": "FAILED", "message": "No active position found"}
            except Exception as e:
                self.write_logs("error", f"[_place_order_mo] Position fetch failed: {str(e)}")

        order_payload = {
            "clientcode":account_id,
            "exchange": _exchange,
            "symboltoken": int(exchange_token),
            "buyorsell": transaction_type.upper(),
            "producttype": _product,
            "orderduration":"DAY",
            "ordertype": _order_type,
            "quantityinlot": int(_quantity),
            "price": float(price) if price else 0.0,
            "triggerprice":0,
            "disclosedquantity":0,
            "algoid":"",
            "amoorder":"N",
            "tag": tag[:10] if tag else f"OE{int(time.time())}"[:19]
        }
        # print("order_payload == ",order_payload)
        response = {}
        try:
            self.write_logs("info", f"[_place_order_mo] ID:{account_id} Order placed payload : {order_payload}")
            res = broker_obj.PlaceOrder(order_payload)
            self.write_logs("info", f"[_place_order_mo] ID:{account_id} Order placed response : {res}")
            # print("res == ",res)
            if res and res.get("uniqueorderid"):
                o_id = res["uniqueorderid"]
                time.sleep(1.5)
                history_res = broker_obj.GetOrderDetailByUniqueorderID(str(o_id),str(account_id))
                # print("history_res == ",history_res)
                if history_res and 'data' in history_res:
                    history = history_res['data']
                    # print("mo = history = ",history)
                    term_status = ["COMPLETE","TRADED","CONFIRM","CANCEL", "REJECT", "REJECTED", "FAILED", "CANCELLED", "FILLED","ERROR"]
                    order_data = next((o for o in history if o.get("orderstatus", "").upper() in term_status), history[-1])
                    status = order_data.get("orderstatus", "").upper()
                    # self.write_logs("info", f"[_place_order_findoc] Order placed: {o_id}, Status: {status}")
                    # if (_order_type == "MARKET") and status in ["PENDINGNEW","PENDING","NEW"]:
                        # self.write_logs("warning",f"Order Status : {status} as it is market changing it to COMPLETE")
                    #     status = "COMPLETE"
                    if (o_type_upper in ["LIMIT", "ADJUST"]) and (status not in term_status):
                        modify_res, final_oid = self._modify_order("mo", account_id, broker_obj, {}, o_id, quantity, exchange_token, exchange, product, order_type, transaction_type, position_type, validity, price, trigger_price)
                        if modify_res == "MARKET_SUCCESS":
                            response = {"order_id": final_oid, "orderstatus": "SUCCESS"}
                        elif "FAILED" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "FAILED", "message": modify_res}
                        elif "ZOMBIE_ORDER_MODIFIER" in modify_res:
                            response = {"order_id": o_id, "orderstatus": "SUCCESS", "message": f"Started {modify_res}"}
                        else:
                            response = {"order_id": o_id, "orderstatus": modify_res}
                    else:
                        response = {"order_id": o_id, "orderstatus": status, "message": order_data.get("CancelRejectReason")}
                else:
                    response = {"order_id": o_id, "orderstatus": "FAILED", "message": "Failed to get history"}
            else:
                response = {"orderstatus": "FAILED", "message": str(res).replace("\n", "|").replace(",","-") if res else "Order failed"}
        except Exception as ex:
            self.write_logs("error", f"[_place_order_mo] Exception: {str(ex)}")
            response = {"orderstatus": "FAILED", "message": str(ex)}

        # duration = time.time() - start_time
        # self.write_logs("info", f"[_place_order_mo] Finished for {account_id} in {duration:.2f}s")
        
        final_status = response.get("orderstatus", "FAILED").upper()
        is_success = final_status in ["SUCCESS", "COMPLETE", "COMPLETED", "TRADED", "FILLED"]
        return {
            "order_id": response.get("order_id"),
            "placed_qty": quantity,
            "status": "SUCCESS" if is_success else "FAILED",
            "message": response.get("message") or final_status,
            "error": response.get("error") if not is_success else None
        }
      
        
        # return response   
    
    def get_split_size(self,exchange,symbol, lot_size=1):
        
        if (symbol in ("NIFTY","SENSEX","BANKNIFTY","FINNIFTY","MIDCAPNIFTY","BANKEX")) and (exchange in ("NFO","BFO")):
            return math.floor(self._split_size / lot_size)
        elif exchange in ("NFO","BFO"):
            return 10 
        else:
            return 0

    def _modify_trade_master_order(self, account_id, broker_object: TradeHub, transaction_type, instrument_data, order_id, quantity, order_type, product, price):
        start_time = time.time()
        # self.write_logs("info", f"[_modify_trade_master_order] Started for {account_id}, Order: {order_id}")
        
        try:
            order_story = broker_object.get_orderHistory(str(order_id))
            if not order_story or 'result' not in order_story or not order_story['result']:
                return "FAILED"

            term_status = ["COMPLETE", "REJECTED", "REJECT", "CANCELLED"]
            order_data = next((o for o in order_story['result'] if o.get("orderStatus") in term_status), order_story['result'][0])
            status = order_data["orderStatus"].upper()

            if status not in term_status and status != "FILLED":

                response = broker_object.modifyOrder(brokerOrderId=str(order_id),quantity = int(quantity),orderType=order_type, price=price, slTriggerPrice=0, slLegPrice=0, targetLegPrice=0,disclosedQuantity = 0,validity="DAY")
                self.write_logs("info", f"[_modify_trade_master_order] account_id: {account_id}, Order: {order_id}, Response: {response}")
                time.sleep(self.modify_after_sleep)
                order_story = broker_object.get_orderHistory(str(order_id))
                if order_story and 'result' in order_story and order_story['result']:
                    order_data = next((o for o in order_story['result'] if o.get("orderStatus") in term_status), order_story['result'][0])
                    status = order_data["orderStatus"].upper()
                    
                    

            # self.write_logs("info", f"[_modify_trade_master_order] Order: {order_id}, Final Status: {status}")
            if status in ["COMPLETE", "COMPLETED", "TRADED", "FILLED"]: return "SUCCESS"
            if status in ["REJECTED", "REJECT", "CANCELLED"]: return "REJECTED"
            if status in ["OPEN", "PENDING"]:return "OPEN/PENDING"
                        
            if status in ["OPEN", "PENDING"]:
                
                pending_qty = int(order_data['pendingQuantity'])
                if (pending_qty != 0):
                    return f"PARTIALLY_FILLED_{pending_qty}"
                elif (pending_qty == 0):
                    return "SUCCESS"
                return "OPEN/PENDING"
            
            # Filled_qty = int(order_data['pendingQuantity'])
            # Total_qty = int(order_data['quantity'])
            # if status in ["OPEN", "PENDING"]:
            #     qty_0 = (Filled_qty != 0)
            #     if (Total_qty != Filled_qty) and qty_0:
            #         return "PARTIALLY_FILLED"
            #     elif (Total_qty == Filled_qty) and qty_0:
            #         return "SUCCESS"
            #     return "OPEN/PENDING"

            return "FAILED"
        except Exception as e:
            self.write_logs("error", f"[_modify_trade_master_order] Exception: {str(e)}")
            return "FAILED"
        finally:
            self.write_logs("info", f"[_modify_trade_master_order] Finished in {time.time()-start_time:.2f}s")

    def _modify_alice_order(self, account_id, broker_object, transaction_type, instrument_data, order_id, quantity, order_type, product, price):
        start_time = time.time()
        # self.write_logs("info", f"[_modify_alice_order] Started for {account_id}, Order: {order_id}")
        
        try:
            _transaction_type = TransactionType.Buy if transaction_type.upper() == "BUY" else TransactionType.Sell
            p_map = {"MIS": ProductType.Intraday, "CNC": ProductType.Delivery, "CO": ProductType.CoverOrder, "BO": ProductType.BracketOrder, "NRML": ProductType.Normal}
            _product_type = p_map.get(product.upper())
            if not _product_type: return "FAILED"

            o_map = {"ADJUST": OrderType.Limit, "MARKET": OrderType.Market, "LIMIT": OrderType.Limit, "SL": OrderType.StopLossLimit, "SL-M": OrderType.StopLossMarket}
            _order_type = o_map.get(order_type.upper())
            if not _order_type: return "FAILED"

            order_data = broker_object.get_order_history(str(order_id))
            if not order_data or ("Trsym" not in order_data and "Nstordno" not in order_data):
                return "FAILED"

            status = order_data.get('Status', '').lower()
            term_status = ["rejected", "complete", "failed", "reject", "cancelled", "completed"]
            
            if status not in term_status:
                response = broker_object.modify_order(transaction_type=_transaction_type, instrument=self.alice_instrument(instrument_data), order_id=str(order_id), quantity=int(quantity), order_type=_order_type, product_type=_product_type, price=float(price), trigger_price=None)
                self.write_logs("info", f"[_modify_alice_order] account_id: {account_id}, Order: {order_id}, Response: {response}")
                time.sleep(self.modify_after_sleep)
                order_data = broker_object.get_order_history(str(order_id))
                status = order_data.get('Status', '').lower() if order_data else "failed"
                
                
            # self.write_logs("info", f"[_modify_alice_order] Order: {order_id}, Final Status: {status}")
            if status in ["complete", "completed", "traded", "filled"]: return "SUCCESS"
            if status in ["rejected", "reject", "cancelled"]: return "REJECTED"
            
            if status in ["open", "pending"]:
                Filled_qty = int(order_data['Fillshares'])
                Total_qty = int(order_data['Qty'])
                qty_0 = (Filled_qty != 0)
                if (Total_qty != Filled_qty) and qty_0:
                    return f"PARTIALLY_FILLED_{Total_qty - Filled_qty}"
                elif (Total_qty == Filled_qty) and qty_0:
                    return "SUCCESS"
                return "OPEN/PENDING"
            
            return "FAILED"
        except Exception as e:
            self.write_logs("error", f"[_modify_alice_order] Exception: {str(e)}")
            return "FAILED"
        finally:
            self.write_logs("info", f"[_modify_alice_order] Finished in {time.time()-start_time:.2f}s")

    def _modify_zerodha_order(self, account_id, broker_object:KiteConnect, transaction_type, instrument_data, order_id, quantity, order_type, product, price, variety):
        start_time = time.time()
        # self.write_logs("info", f"[_modify_zerodha_order] Started for {account_id}, Order: {order_id}")
        
        try:
            history = broker_object.orders()
            history = next((his for his in history if ((int(his.get("order_id")) == int(order_id)))), None)

            # history = broker_object.order_history(order_id)
            if not history: return "FAILED"
            
            status = history["status"].lower()
            term_status = ["rejected", "complete", "failed", "reject", "cancelled", "completed"]
            
            if status not in term_status:
                response = broker_object.modify_order(variety=variety, order_id=order_id, price=price)
                self.write_logs("info", f"[_modify_zerodha_order] account_id: {account_id}, Order: {order_id}, Response: {response}")
                
                time.sleep(self.modify_after_sleep)
                history = broker_object.orders()
                history = next((his for his in history if ((int(his.get("order_id")) == int(order_id)))), None)

                # history = broker_object.order_history(order_id)
                status = history["status"].lower() if history else "failed"

            # self.write_logs("info", f"[_modify_zerodha_order] Order: {order_id}, Final Status: {status}")
            if status in ["complete", "completed", "traded", "filled"]: return "SUCCESS"
            if status in ["rejected", "reject", "cancelled"]: return "REJECTED"
            
            if status in ["open", "pending"]:
                Filled_qty = int(history.get('filled_quantity', 0))
                Total_qty = int(history.get('quantity', 0))
                qty_0 = (Filled_qty != 0)
                if (Total_qty != Filled_qty) and qty_0:
                    return f"PARTIALLY_FILLED_{Total_qty - Filled_qty}"
                elif (Total_qty == Filled_qty) and qty_0:
                    return "SUCCESS"
                return "OPEN/PENDING"
            return "FAILED"
        except Exception as e:
            self.write_logs("error", f"[_modify_zerodha_order] Exception: {str(e)}")
            return "FAILED"
        finally:
            self.write_logs("info", f"[_modify_zerodha_order] Finished in {time.time()-start_time:.2f}s")
            
    def _modify_upstock_order(self, account_id, broker_object, transaction_type, exchange, instrument_data, order_id, quantity, order_type, product, price, validity, api_version):
        start_time = time.time()
        # self.write_logs("info", f"[_modify_upstock_order] Started for {account_id}, Order: {order_id}")
        
        try:
            _qty = (quantity * int(instrument_data['lot_size'])) if exchange in ["NFO", "BFO"] else quantity
            order_status = broker_object.get_order_status(order_id=str(order_id)).to_dict()
            if 'data' not in order_status: return "FAILED"

            status = order_status['data'].get("status", "").lower()
            term_status = ["rejected", "complete", "failed", "reject", "cancelled", "completed"]

            if status not in term_status:
                modify_body = upstox_client.ModifyOrderRequest(quantity=int(_qty), validity=validity, price=float(price), order_id=order_id, order_type=order_type,trigger_price=0)
                response = broker_object.modify_order(modify_body, api_version).to_dict()
                self.write_logs("info", f"[_modify_upstock_order] account_id: {account_id}, Order: {order_id}, Response: {response}")
                time.sleep(self.modify_after_sleep)
                order_status = broker_object.get_order_status(order_id=str(order_id)).to_dict()
                status = order_status['data'].get("status", "").lower() if 'data' in order_status else "failed"

            # self.write_logs("info", f"[_modify_upstock_order] Order: {order_id}, Final Status: {status}")
            if status in ["complete", "completed", "traded", "filled"]: return "SUCCESS"
            if status in ["rejected", "reject", "cancelled"]: return "REJECTED"
            
            if status in ["open", "pending"]:
                Filled_qty = int(order_status['data'].get('filled_quantity', 0))
                Total_qty = int(order_status['data'].get('quantity', 0))
                qty_0 = (Filled_qty != 0)
                if (Total_qty != Filled_qty) and qty_0:
                    return f"PARTIALLY_FILLED_{Total_qty - Filled_qty}"
                elif (Total_qty == Filled_qty) and qty_0:
                    return "SUCCESS"
                return "OPEN/PENDING"
            
            return "FAILED"
        except Exception as e:
            self.write_logs("error", f"[_modify_upstock_order] Exception: {str(e)}")
            return "FAILED"
        finally:
            self.write_logs("info", f"[_modify_upstock_order] Finished in {time.time()-start_time:.2f}s")
        
    def _modify_finvasia_order(self, account_id, broker_object: ShoonyaApiPy, transaction_type, instrument_data, order_id, quantity, order_type, product, price, validity, api_version):
        start_time = time.time()
        # self.write_logs("info", f"[_modify_finvasia_order] Started for {account_id}, Order: {order_id}")
        
        try:
            _qty = (int(quantity) * instrument_data['ls']) if instrument_data.get('exch') in ["NFO", "BFO"] else quantity
            history = broker_object.get_order_book()
            history = next((his for his in history if ((int(his.get("norenordno")) == int(order_id)))), None)
            if not history: return "FAILED"

            term_status = ["COMPLETE", "REJECT", "REJECTED", "FAILED", "CANCELLED"]
            order_data =history
            status = order_data.get("status", "").upper()

            o_map = {"MARKET": "MKT", "LIMIT": "LMT", "ADJUST": "LMT", "SL": "SL-LMT", "SL-M": "SL-MKT"}
            _order_type = o_map.get(order_type.upper(), "LMT")

            if status not in term_status:
                response = broker_object.modify_order(order_id, instrument_data["exch"], instrument_data["tsym"], int(_qty), _order_type, float(price))
                self.write_logs("info", f"[_modify_finvasia_order] account_id: {account_id}, Order: {order_id}, Response: {response}")
                time.sleep(self.modify_after_sleep)
                history = broker_object.get_order_book()
                history = next((his for his in history if ((int(his.get("norenordno")) == int(order_id)))), None)
                if history:
                    order_data = history
                    status = order_data.get("status", "").upper()

            # self.write_logs("info", f"[_modify_finvasia_order] Order: {order_id}, Final Status: {status}")
            if status in ["COMPLETE", "COMPLETED", "TRADED", "FILLED"]: return "SUCCESS"
            if status in ["REJECT", "REJECTED", "FAILED", "CANCELLED"]: return "REJECTED"
            
            if status in ["OPEN", "PENDING"]:
                Filled_qty = int(order_data['fillshares'])
                Total_qty = int(order_data['qty'])
                qty_0 = (Filled_qty != 0)
                if (Total_qty != Filled_qty) and qty_0:
                    return f"PARTIALLY_FILLED_{Total_qty - Filled_qty}"
                elif (Total_qty == Filled_qty) and qty_0:
                    return "SUCCESS"
                return "OPEN/PENDING"
            
            return "FAILED"
        except Exception as e:
            self.write_logs("error", f"[_modify_finvasia_order] Exception: {str(e)}")
            return "FAILED"
        finally:
            self.write_logs("info", f"[_modify_finvasia_order] Finished in {time.time()-start_time:.2f}s")
        
    def _modify_findoc_order(self, account_id, broker_object: FindocAPI, transaction_type, exchange, order_id, quantity, order_type, product, price, validity, lotsize, tag):
        start_time = time.time()
        # self.write_logs("info", f"[_modify_findoc_order] Started for {account_id}, Order: {order_id}")
        
        try:
            _qty = (quantity * lotsize) if exchange in ["NFO", "BFO", "MCX"] else quantity
            history = broker_object.get_order_history_id(order_id)
            if not history: return "FAILED"

            term_status = ["COMPLETE", "REJECT", "REJECTED", "FAILED", "CANCELLED", "FILLED"]
            order_data = next((o for o in history if o.get("OrderStatus", "").upper() in term_status), history[-1])
            status = order_data.get("OrderStatus", "").upper()

            if status not in term_status:
                o_map = {"MARKET": "MARKET", "LIMIT": "LIMIT", "ADJUST": "LIMIT", "SL": "STOPLIMIT", "SL-M": "STOPMARKET"}
                _order_type = o_map.get(order_type.upper(), "LIMIT")
                p_map = {"MIS": "MIS", "CNC": "CNC" if exchange in ["NSE", "BSE"] else "NRML"}
                _product = p_map.get(product.upper(), "MIS")

                payload = {"order_id": order_id, "product_type": _product, "order_type": _order_type, "quantity": int(_qty), "price": float(price), "order_tag": tag}
                response = broker_object.modify_order(**payload)
                self.write_logs("info", f"[_modify_findoc_order] account_id: {account_id}, Order: {order_id}, Response: {response}")
                time.sleep(self.modify_after_sleep)
                history = broker_object.get_order_history_id(order_id)
                if history:
                    order_data = next((o for o in history if o.get("OrderStatus", "").upper() in term_status), history[-1])
                    status = order_data.get("OrderStatus", "").upper()

            # self.write_logs("info", f"[_modify_findoc_order] Order: {order_id}, Final Status: {status}")
            if status in ["COMPLETE", "COMPLETED", "TRADED", "FILLED"]: return "SUCCESS"
            if status in ["REJECT", "REJECTED", "FAILED", "CANCELLED"]: return "REJECTED"
            if status in ["OPEN", "PENDING", "NEW", "PENDINGNEW"]: return "OPEN/PENDING"
            return "FAILED"
        except Exception as e:
            self.write_logs("error", f"[_modify_findoc_order] Exception: {str(e)}")
            return "FAILED"
        finally:
            self.write_logs("info", f"[_modify_findoc_order] Finished in {time.time()-start_time:.2f}s")
            
            
    def _modify_mo_order(self, account_id, broker_object: MOFSLOPENAPI, transaction_type, exchange, order_id, quantity, order_type, product, price, validity, lotsize, tag):
        start_time = time.time()
        # self.write_logs("info", f"[_modify_mo_order] Started for {account_id}, Order: {order_id}")
                    # term_status = ["COMPLETE","TRADED","CONFIRM","SENT","CANCEL","PARTIAL", "REJECT", "REJECTED", "FAILED", "CANCELLED", "FILLED","ERROR"]
        try:
            _qty = (quantity * lotsize) if exchange in ["NFO", "BFO", "MCX"] else quantity
            history = broker_object.get_order_history_id(order_id)
            if not history: return "FAILED"

            term_status = ["COMPLETE","TRADED","CONFIRM","CANCEL","REJECT", "REJECTED", "FAILED", "CANCELLED", "FILLED","ERROR"]
            order_data = next((o for o in history if o.get("orderstatus", "").upper() in term_status), history[-1])
            status = order_data.get("OrderStatus", "").upper()

            if status not in term_status:
                o_map = {"MARKET": "MARKET", "LIMIT": "LIMIT", "ADJUST": "LIMIT", "SL": "STOPLIMIT", "SL-M": "STOPMARKET"}
                _order_type = o_map.get(order_type.upper(), "LIMIT")
                p_map = {"MIS": "VALUEPLUS", "CNC": "DELIVERY" if exchange in ["NSE", "BSE"] else "NORMAL"}
                _product = p_map.get(product.upper(), "MIS")
                payload = {
                    "clientcode":account_id,
                    "uniqueorderid":order_id,
                    "newordertype":order_type,
                    "neworderduration":validity,   
                    "newquantityinlot":quantity,
                    "newdisclosedquantity":0,
                    "newprice":price,
                    "newtriggerprice":0,
                    "newgoodtilldate": 0,
                    "lastmodifiedtime": str(datetime.now()),
                    "qtytradedtoday": 0
                }
                self.write_logs("info", f"_modify_mo_order - account_id: {account_id} - payload {payload}")
                response = broker_object.ModifyOrder(payload)
                self.write_logs("info", f"[_modify_mo_order] account_id: {account_id}, Order: {order_id}, Response: {response}")
                time.sleep(self.modify_after_sleep)
                history = broker_object.GetOrderDetailByUniqueorderID(order_id,account_id)
                if history:
                    order_data = next((o for o in history if o.get("orderstatus", "").upper() in term_status), history[-1])
                    status = order_data.get("orderstatus", "").upper()

            # self.write_logs("info", f"[_modify_mo_order] Order: {order_id}, Final Status: {status}")
            if status in ["COMPLETE", "COMPLETED", "TRADED", "FILLED", "CONFIRM"]: return "SUCCESS"
            if status in ["REJECT", "REJECTED", "FAILED", "CANCELLED","ERROR","REJECT","ERROR","CANCEL"]: return "REJECTED"
            if status in ["OPEN", "PENDING", "NEW", "PENDINGNEW", "PARTIAL"]: return "OPEN/PENDING"
            return "FAILED"
        except Exception as e:
            self.write_logs("error", f"[_modify_mo_order] Exception: {str(e)}")
            return "FAILED"
        finally:
            self.write_logs("info", f"[_modify_mo_order] Finished in {time.time()-start_time:.2f}s")
           
    def _modify_xts_order(self, account_id, broker_object, transaction_type, exchange, order_id, quantity, order_type, product, price, validity, lotsize, tag):
        start_time = time.time()
        # self.write_logs("info", f"[_modify_xts_order] Started for {account_id}, Order: {order_id}")
        
        try:
            _qty = (quantity * lotsize) if exchange in ["NFO", "BFO", "MCX"] else quantity
            history = broker_object.get_order_history(order_id, clientID="*****")
            if not history or 'result' not in history or not history['result']: return "FAILED"

            term_status = ["COMPLETE", "REJECT", "REJECTED", "FAILED", "CANCELLED", "FILLED"]
            order_data = next((o for o in history['result'] if o.get("OrderStatus", "").upper() in term_status), history['result'][-1])
            status = order_data.get("OrderStatus", "").upper()

            if status not in term_status:
                o_map = {"MARKET": "MARKET", "LIMIT": "LIMIT", "ADJUST": "LIMIT", "SL": "STOPLIMIT", "SL-M": "STOPMARKET"}
                _order_type = o_map.get(order_type.upper(), "LIMIT")
                p_map = {"MIS": "MIS", "CNC": "CNC" if exchange in ["NSE", "BSE"] else "NRML"}
                _product = p_map.get(product.upper(), "MIS")

                payload = {"appOrderID": order_id, "modifiedProductType": _product, "modifiedOrderType": _order_type, "modifiedOrderQuantity": int(_qty), "modifiedLimitPrice": float(price), "clientID": "*****", "orderUniqueIdentifier": tag}
                self.write_logs("info", f"[_modify_xts_order] - account_id: {account_id} - payload {payload}")
                response = broker_object.modify_order(**payload)
                self.write_logs("info", f"[_modify_xts_order] account_id: {account_id}, Order: {order_id}, Response: {response}")
                time.sleep(self.modify_after_sleep)
                history = broker_object.get_order_history(order_id, clientID="*****")
                if history and 'result' in history and history['result']:
                    order_data = next((o for o in history['result'] if o.get("OrderStatus", "").upper() in term_status), history['result'][-1])
                    status = order_data.get("OrderStatus", "").upper()

            self.write_logs("info", f"[_modify_xts_order] Order: {order_id}, Final Status: {status}")
            if status in ["COMPLETE", "COMPLETED", "TRADED", "FILLED"]: return "SUCCESS"
            if status in ["REJECT", "REJECTED", "FAILED", "CANCELLED"]: return "REJECTED"
            if status in ["OPEN", "PENDING", "NEW", "PENDINGNEW"]: return "OPEN/PENDING"
            return "FAILED"
        except Exception as e:
            self.write_logs("error", f"[_modify_xts_order] Exception: {str(e)}")
            return "FAILED"
        finally:
            self.write_logs("info", f"[_modify_xts_order] Finished in {time.time()-start_time:.2f}s")
           
    def _modify_5paisa_order(self, account_id, broker_object: FivepaisaBroker, transaction_type, exchange, order_id, quantity, order_type, product, price, validity, lotsize, tag):
        start_time = time.time()
        # self.write_logs("info", f"[_modify_5paisa_order] Started for {account_id}, Order: {order_id}")
        
        try:
            history = broker_object.get_order_history(order_id)
            if not history: return "FAILED"

            status = history.get("OrderStatus", "").upper()
            term_status = ["FULLY EXECUTED", "REJECTED", "CANCELLED", "FAILED"]

            if status not in term_status:
                response = broker_object.modify_order(order_id, str(price))
                self.write_logs("info", f"[_modify_5paisa_order] account_id: {account_id}, Order: {order_id}, Response: {response}")
                time.sleep(self.modify_after_sleep)
                history = broker_object.get_order_history(order_id)
                status = history.get("OrderStatus", "").upper() if history else "FAILED"

            # self.write_logs("info", f"[_modify_5paisa_order] Order: {order_id}, Final Status: {status}")
            if status == "FULLY EXECUTED": return "SUCCESS"
            if status in ["REJECTED", "CANCELLED", "FAILED"]: return "REJECTED"
            if status in ["PENDING", "PARTIALLY EXECUTED"]: return "OPEN/PENDING"
            
            if status in ["OPEN", "PENDING"]:
                Filled_qty = int(history['TradedQty'])
                Total_qty = int(history['Qty'])
                qty_0 = (Filled_qty != 0)
                if (Total_qty != Filled_qty) and qty_0:
                    return f"PARTIALLY_FILLED_{Total_qty - Filled_qty}"
                elif (Total_qty == Filled_qty) and qty_0:
                    return "SUCCESS"
                return "OPEN/PENDING"
            
            return "FAILED"
        except Exception as e:
            self.write_logs("error", f"[_modify_5paisa_order] Exception: {str(e)}")
            return "FAILED"
        finally:
            self.write_logs("info", f"[_modify_5paisa_order] Finished in {time.time()-start_time:.2f}s")
        
    def _modify_dhan_order(self, account_id, broker_object: dhanhq, transaction_type, exchange, order_id, quantity, order_type, product, price, validity, lotsize, tag):
        start_time = time.time()
        self.write_logs("info", f"[_modify_dhan_order] Started for {account_id}, Order: {order_id}")
        
        try:
            res = broker_object.get_order_by_id(order_id)
            if not res or 'data' not in res: return "FAILED"

            data = res['data'] if isinstance(res['data'], list) else [res['data']]
            term_status = ["COMPLETE", "REJECTED", "FAILED", "CANCELLED", "TRADED"]
            order_data = next((o for o in data if o.get("orderStatus") in term_status), data[-1])
            status = order_data.get("orderStatus", "").upper()

            if status not in term_status:
                response = broker_object.modify_order(order_id, order_type=order_type, price=str(price), validity=validity)
                self.write_logs("info", f"[_modify_dhan_order] account_id: {account_id}, Order: {order_id}, Response: {response}")
                time.sleep(self.modify_after_sleep)
                res = broker_object.get_order_by_id(order_id)
                if res and 'data' in res:
                    data = res['data'] if isinstance(res['data'], list) else [res['data']]
                    order_data = next((o for o in data if o.get("orderStatus") in term_status), data[-1])
                    status = order_data.get("orderStatus", "").upper()

            self.write_logs("info", f"[_modify_dhan_order] Order: {order_id}, Final Status: {status}")
            if status in ["COMPLETE", "TRADED"]: return "SUCCESS"
            if status in ["REJECTED", "FAILED", "CANCELLED"]: return "REJECTED"
            if status in ["PENDING", "TRANSIT"]: return "OPEN/PENDING"
            return "FAILED"
        except Exception as e:
            self.write_logs("error", f"[_modify_dhan_order] Exception: {str(e)}")
            return "FAILED"
        finally:
            self.write_logs("info", f"[_modify_dhan_order] Finished in {time.time()-start_time:.2f}s")
        
        
    def get_angel_modify(self, brokerobject,modify_params):
        max_retries = 20

        for attempt in range(1, max_retries + 1):
            try:
                response = brokerobject.modifyOrder(modify_params)

                return response['data']

            except Exception as ex:
                self.write_logs(
                    "error",
                    f"[DEBUG][get_angel_modify] attempt [{attempt}/{max_retries}]: {ex}"
                )
                time.sleep(self.error_buffer)

        # Explicit failure
        self.write_logs(
            "error",
            "[DEBUG][get_angel_modify] Failed to fetch orderbook after all retries"
        )
        return None   
         
    def _modify_angel_order(self, account_id, broker_object: SmartConnect, transaction_type, instrument_data, order_id, quantity, order_type, product, price, validity, variety):
        start_time = time.time()
        # self.write_logs("info", f"[_modify_angel_order] Started for {account_id}, Order: {order_id}")
        
        try:
            _qty = (quantity * int(instrument_data['lotsize'])) if instrument_data.get('exch_seg') in ["NFO", "BFO", "MCX"] else quantity
            history = self.get_angel_orderbook(broker_object)
            order_data = next((o for o in history if str(o.get("orderid")) == str(order_id)), None) if history else None
            if not order_data: return "FAILED"

            status = order_data.get("status", "").lower()
            term_status = ["complete", "rejected", "cancelled", "failed"]

            if status not in term_status:
                o_map = {"MARKET": "MARKET", "LIMIT": "LIMIT", "ADJUST": "LIMIT", "SL": "STOPLOSS_LIMIT", "SL-M": "STOPLOSS_MARKET"}
                _order_type = o_map.get(order_type.upper(), "LIMIT")
                p_map = {"MIS": "INTRADAY", "CNC": "DELIVERY" if instrument_data.get('exch_seg') in ["NSE", "BSE"] else "CARRYFORWARD", "CO": "CO", "BO": "BO"}
                _product = p_map.get(product.upper(), "INTRADAY")
                validity="DAY" 
                params = {"variety": variety, "orderid": order_id, "ordertype": _order_type, "producttype": _product, "duration": validity, "price": str(price), "quantity": str(int(_qty)), "tradingsymbol": instrument_data['symbol'], "symboltoken": int(instrument_data['token']), "exchange": instrument_data['exch_seg']}
                response = self.get_angel_modify(broker_object, params)
                self.write_logs("info", f"[_modify_angel_order] account_id: {account_id}, Order: {order_id}, Response: {response}")
                time.sleep(self.modify_after_sleep)
                history = self.get_angel_orderbook(broker_object)
                order_data = next((o for o in history if str(o.get("orderid")) == str(order_id)), None) if history else None
                status = order_data.get("status", "").lower() if order_data else "failed"

            self.write_logs("info", f"[_modify_angel_order] Order: {order_id}, Final Status: {status}")
            if status in ["complete", "completed", "traded", "filled"]: return "SUCCESS"
            if status in ["rejected", "cancelled", "failed"]: return "REJECTED"
            
            if status in ["open", "pending"]:
                Filled_qty = int(order_data['filledshares'])
                Total_qty = int(order_data['quantity'])
                qty_0 = (Filled_qty != 0)
                if (Total_qty != Filled_qty) and qty_0:
                    return f"PARTIALLY_FILLED_{Total_qty - Filled_qty}"
                elif (Total_qty == Filled_qty) and qty_0:
                    return "SUCCESS"
                return "OPEN/PENDING"
            
            return "FAILED"
        except Exception as e:
            self.write_logs("error", f"[_modify_angel_order] Exception: {str(e)}")
            return "FAILED"
        finally:
            self.write_logs("info", f"[_modify_angel_order] Finished in {time.time()-start_time:.2f}s")
            
    def calculate_price(self,price,transaction_type):
        # if datafeed_key in self.modify_price_size:
        if transaction_type == "BUY":
            rprice = price + (price * (self.limit_price_diff_percent/100))
            # rprice = price + self.modify_price_size[datafeed_key]
        else:
            rprice = price - (price * (self.limit_price_diff_percent/100))
            if rprice < 0:
                rprice = 1
        return rprice
        # else:
        #     self.write_logs("info", f"[_modify_angel_order] No modify price size found for {datafeed_key}, using make_limit_price_diff")
        #     return price + (self.make_limit_price_diff if transaction_type == "BUY" else - self.make_limit_price_diff)
            
    def _zombie_order_modifyer(self,broker,tsym,symbol,exchange,lotsize,tick_size,account_id, _broker_object, transaction_type, instrument_data, order_id, _quantity, _order_type, product, price, variety, validity, api_version,_tag):
        self.write_logs("info", f"[_zombie_order_modifyer] Starting modification for {broker}:{account_id}, OrderID: {order_id}, Type: {_order_type}")
        inital_ltp = None
                
        count =0
        try_except = 0
        while True:
            count +=1
            try:
                for i in range(3):
                    ltp = self.fetch_ltp(instrument_data, exchange, symbol, tsym, instrument_data['Instrument Type'])
                    if inital_ltp is None:
                        inital_ltp = ltp
                        
                    if (ltp != 0):
                        inital_ltp = ltp
                        
                    if (ltp == 0):
                        updated_price = self.calculate_price(ltp, transaction_type)
                        updated_price = self.round_to_tick(updated_price, tick_size)
                        message = f"price not received for {tsym}"
                        self.write_logs("error", f"[_zombie_order_modifyer] {message}")
                        break
                    else:
                        updated_price = self.calculate_price(ltp, transaction_type)
                        updated_price = self.round_to_tick(updated_price, tick_size)
                        break
                self.write_logs("info",f"[_zombie_order_modifyer] Modified {tsym} @ {price} for {broker}:{account_id} Done {i} attempts")    
                if broker.lower() == "alice":
                    status = self._modify_alice_order(account_id, _broker_object, transaction_type, instrument_data, order_id, _quantity, _order_type, product, updated_price)
                elif broker.lower() == "trade_master":
                    status = self._modify_trade_master_order(account_id, _broker_object, transaction_type, instrument_data, order_id, _quantity, _order_type, product, updated_price)
                elif broker.lower() == "zerodha":
                    status = self._modify_zerodha_order(account_id, _broker_object, transaction_type, instrument_data, order_id, _quantity, _order_type, product, updated_price, variety)
                elif broker.lower() == "upstock":
                    status = self._modify_upstock_order(account_id, _broker_object, transaction_type, exchange, instrument_data, order_id, _quantity, _order_type, product, updated_price, validity, api_version)
                elif broker.lower() == "finvasia":
                    status = self._modify_finvasia_order(account_id, _broker_object, transaction_type, instrument_data, order_id, _quantity, _order_type, product, updated_price, validity, api_version)
                elif broker.lower() == "fyers":
                    status = self._modify_fyers_order(account_id, _broker_object, transaction_type, instrument_data, order_id, _quantity, _order_type, product, updated_price, validity, api_version)
                elif broker.lower() == "angel":
                    status = self._modify_angel_order(account_id, _broker_object, transaction_type, instrument_data, order_id, _quantity, _order_type, product, updated_price, validity, variety)
                elif broker.lower() == "xts":
                    status = self._modify_xts_order(account_id, _broker_object, transaction_type, exchange, order_id, _quantity, _order_type, product, updated_price, validity, lotsize=lotsize, tag=_tag)
                elif broker.lower() == "5paisa":
                    status = self._modify_5paisa_order(account_id, _broker_object, transaction_type, exchange, order_id, _quantity, _order_type, product, updated_price, validity, lotsize=lotsize, tag=_tag)
                elif broker.lower() == "dhan":
                    status = self._modify_dhan_order(account_id, _broker_object, transaction_type, exchange, order_id, _quantity, _order_type, product, updated_price, validity, lotsize=lotsize, tag=_tag)
                elif broker.lower() == "findoc":
                    status = self._modify_findoc_order(account_id, _broker_object, transaction_type, exchange, order_id, _quantity, _order_type, product, updated_price, validity, lotsize=lotsize, tag=_tag)
                
                elif broker.lower() == "mo":
                    status = self._modify_mo_order(account_id, _broker_object, transaction_type, exchange, order_id, _quantity, _order_type, product, updated_price, validity, lotsize=lotsize, tag=_tag)
                
                self.write_logs("info", f"[_zombie_order_modifyer] {tsym} @ {price} for {broker}:{account_id} modification status: {status}")
                if status and status.upper() in ["FILLED", "TRADED", "COMPLETED", "COMPLETE", "SUCCESS"]:
                    break
            except Exception as ex:
                if try_except > 3:
                    self.write_logs("error",f"[_zombie_order_modifyer] Done all attempts for {broker}:{account_id} for {tsym} @ {price} got Exception as {str(ex)}")
                    break
                
                self.write_logs("error",f"[_zombie_order_modifyer] {broker}:{account_id} for {tsym} @ {price} got Exception as {str(ex)}")
                try_except+=1
            
    def _modify_fyers_order(self, account_id, broker_object, transaction_type, instrument_data, order_id, quantity, order_type, product, price, validity, api_version):
        start_time = time.time()
        # self.write_logs("info", f"[_modify_fyers_order] Started for {account_id}, Order: {order_id}")
        
        try:
            res = broker_object.orderbook(data={"id": order_id})
            if not res or 'orderBook' not in res: return "FAILED"

            term_status = ["COMPLETE", "FILLED", "TRADED", "REJECTED", "REJECT", "CANCELLED"]

            def get_mapped_status(o):
                return self.fyers_ord_res.get(int(o.get("status", 0)), "REJECTED").upper()

            order_data = next((o for o in res['orderBook'] if get_mapped_status(o) in term_status), res['orderBook'][-1])
            status = get_mapped_status(order_data)

            if status not in term_status:
                response = broker_object.modify_order(data={"id": order_id, "limitPrice": float(price)})
                self.write_logs("info", f"[_modify_fyers_order] account_id: {account_id}, Order: {order_id}, Response: {response}")
                time.sleep(self.modify_after_sleep)
                res = broker_object.orderbook(data={"id": order_id})
                if res and 'orderBook' in res:
                    order_data = next((o for o in res['orderBook'] if get_mapped_status(o) in term_status), res['orderBook'][-1])
                    status = get_mapped_status(order_data)

            # self.write_logs("info", f"[_modify_fyers_order] Order: {order_id}, Final Status: {status}")
            if status in ["COMPLETE", "FILLED", "TRADED"]: return "SUCCESS"
            if status in ["REJECTED", "REJECT", "CANCELLED"]: return "REJECTED"
            if status in ["OPEN", "PENDING"]: return "OPEN/PENDING"
            return "FAILED"
        except Exception as e:
            self.write_logs("error", f"[_modify_fyers_order] Exception: {str(e)}")
            return "FAILED"
        finally:
            self.write_logs("info", f"[_modify_fyers_order] Finished in {time.time()-start_time:.2f}s")
            
    def _modify_order(self, broker, account_id, broker_object, instrument_data, order_id, quantity, exchange_token: int, exchange: str, product: str, order_type: str, transaction_type: str, position_type: Literal["CLOSE", "OPEN"], validity: str = None, price: float = 0, trigger_price: float = 0, api_version: str = 'v2', variety: str = "regular", _tag = ""):
        start_time = time.time()
        self.write_logs("info", f"[_modify_order] Starting modification for {broker}:{account_id}, OrderID: {order_id}, Type: {order_type}")
        
        if (variety.lower() == "regular") and (broker == "angel"):
            variety = "NORMAL"
        elif (variety.lower() == "co") and (broker == "angel"):
            variety = "ROBO"
            
        alic_instrument_data = self._get_instrument_data("alice", exchange, token=exchange_token)
            
        if broker.lower() == "upstock":
            configuration = upstox_client.Configuration()
            configuration.access_token = broker_object
            _broker_object = upstox_client.OrderApi(upstox_client.ApiClient(configuration))
        else:
            _broker_object = broker_object
            
        if alic_instrument_data['Exch'] in ["NFO", "BFO"]:
            _quantity = int(quantity) * int(alic_instrument_data['Lot Size'])
        else:
            _quantity = int(quantity)
        
        if (product == "MIS") and (broker == "upstock"):
            _product = "I"
        elif (product == "CNC") and (broker == "upstock"):
            _product = "D"
        else:
            _product = product
            
        _order_type = "LIMIT" if order_type == "ADJUST" else order_type
            
        lotsize = int(alic_instrument_data['Lot Size'])
        tsym = alic_instrument_data['Trading Symbol']
        symbol = alic_instrument_data['Symbol']
        tick_size = alic_instrument_data["Tick Size"]
        elligible_modify = {"modify": True, "message": ""}
        is_adjust_order = (order_type.upper() == "ADJUST")
        status = None
        
        if position_type.upper() == "OPEN":
            retry = self.limit_retry
        else:
            retry = self.exit_limit_retry
                
        for i in range(retry):
            ltp = self.fetch_ltp(alic_instrument_data, exchange, symbol, tsym, alic_instrument_data['Instrument Type'])
            if ltp == 0:
                if is_adjust_order:
                    self.write_logs("warning", f"[_modify_order] LTP is 0, switching ADJUST to MARKET for {tsym}")
                    order_type = "MARKET"
                    break
                else:
                    elligible_modify['modify'] = False
                    elligible_modify['message'] = f"price not received for {tsym}"
                    self.write_logs("error", f"[_modify_order] {elligible_modify['message']}")
                    break
            else:
                updated_price = self.calculate_price(ltp, transaction_type)
                updated_price = self.round_to_tick(updated_price, tick_size)
            
            if elligible_modify['modify']:
                self.write_logs("info", f"[_modify_order] Attempt {i+1}/{retry}: Modifying {broker} order {order_id} to price {updated_price}")
                try:
                    if broker.lower() == "alice":
                        status = self._modify_alice_order(account_id, _broker_object, transaction_type, alic_instrument_data, order_id, _quantity, _order_type, product, updated_price)
                    elif broker.lower() == "trade_master":
                        status = self._modify_trade_master_order(account_id, _broker_object, transaction_type, alic_instrument_data, order_id, _quantity, _order_type, product, updated_price)
                    elif broker.lower() == "zerodha":
                        status = self._modify_zerodha_order(account_id, _broker_object, transaction_type, instrument_data, order_id, quantity, _order_type, product, updated_price, variety)
                    elif broker.lower() == "upstock":
                        status = self._modify_upstock_order(account_id, _broker_object, transaction_type, exchange, instrument_data, order_id, quantity, _order_type, _product, updated_price, validity, api_version)
                    elif broker.lower() == "finvasia":
                        status = self._modify_finvasia_order(account_id, _broker_object, transaction_type, instrument_data, order_id, quantity, _order_type, _product, updated_price, validity, api_version)
                    elif broker.lower() == "fyers":
                        status = self._modify_fyers_order(account_id, _broker_object, transaction_type, instrument_data, order_id, quantity, _order_type, product, updated_price, validity, api_version)
                    elif broker.lower() == "angel":
                        status = self._modify_angel_order(account_id, _broker_object, transaction_type, instrument_data, order_id, quantity, _order_type, product, updated_price, validity, variety)
                    elif broker.lower() == "xts":
                        status = self._modify_xts_order(account_id, _broker_object, transaction_type, exchange, order_id, quantity, _order_type, product, updated_price, validity, lotsize=lotsize, tag=_tag)
                    elif broker.lower() == "5paisa":
                        status = self._modify_5paisa_order(account_id, _broker_object, transaction_type, exchange, order_id, quantity, _order_type, product, updated_price, validity, lotsize=lotsize, tag=_tag)
                    elif broker.lower() == "dhan":
                        status = self._modify_dhan_order(account_id, _broker_object, transaction_type, exchange, order_id, quantity, _order_type, product, updated_price, validity, lotsize=lotsize, tag=_tag)
                    elif broker.lower() == "findoc":
                        status = self._modify_findoc_order(account_id, _broker_object, transaction_type, exchange, order_id, quantity, _order_type, product, updated_price, validity, lotsize=lotsize, tag=_tag)
                    elif broker.lower() == "mo":
                        status = self._modify_mo_order(account_id, _broker_object, transaction_type, exchange, order_id, quantity, _order_type, product, updated_price, validity, lotsize=lotsize, tag=_tag)
                    
                    self.write_logs("info", f"[_modify_order] {broker}:{account_id} modification status: {status}")
                    if status and (status.upper() in ["FILLED", "TRADED", "COMPLETED", "COMPLETE", "SUCCESS"]) or ("PARTIALLY_FILLED" in status.upper()):
                        break
                    
                except Exception as ex:
                    self.write_logs("error", f"[_modify_order] Exception during modification: {str(ex)}")
            
            time.sleep(self.modify_after_sleep)
            
        reorder_res = {"status": status, "order_id": order_id}
        
        # Check if we should switch to MARKET (for ADJUST orders or if LIMIT retries exhausted)
        if (reorder_res['status'].upper() not in ["FILLED", "TRADED", "COMPLETED", "COMPLETE", "SUCCESS"]) or (not elligible_modify["modify"]) or ("PARTIALLY_FILLED" in status.upper()):
            self.write_logs("warning", f"[_modify_order] Final state for order {order_id} is {status}. is_adjust_order={is_adjust_order}")
            
            # Cancel order before switching to Market or giving up
            self.write_logs("info", f"[_modify_order] Cancelling order {order_id} for {broker}")
            try:
                if ("PARTIALLY_FILLED" in status.upper()) or broker.lower() in ["trade_master"]:
                    thread = threading.Thread(target = self._zombie_order_modifyer, args =(broker,tsym,symbol,exchange,lotsize,tick_size,account_id, _broker_object, transaction_type, instrument_data, order_id, _quantity, _order_type, product, price, variety, validity, api_version,_tag),daemon=True)
                    thread.start()
                    self.write_logs("info", f"[_modify_order] Zombie order modifyer started for order {order_id} for {broker}:{account_id}")
                    msg_prefix = "close" if position_type.upper() == "CLOSE" else "place"
                    self._send_message("ALERT", f"Started Zombie Modifyer to {msg_prefix} order after retries\nAccount [{broker} : id - {account_id}]\n(Symbol - {tsym} ** Side - {transaction_type} ** Ordertype - {order_type} ** ProductType - {product})")
            
                    return f"ZOMBIE_ORDER_MODIFIER_{status}", order_id
                else:    
                    if broker.lower() == "alice":
                        _broker_object.cancel_order(str(order_id))
                    elif broker.lower() == "trade_master":
                        _broker_object.cancelOrder(str(order_id))
                    elif broker.lower() == "zerodha":
                        _broker_object.cancel_order(variety=variety, order_id=str(order_id))
                    elif broker.lower() == "upstock":
                        _broker_object.cancel_order(order_id=order_id, api_version=api_version)
                    elif broker.lower() == "finvasia":
                        _broker_object.cancel_order(order_id)
                    elif broker.lower() == "fyers":
                        _broker_object.cancel_order(data={"id": order_id})
                    elif broker.lower() == "angel":
                        broker_object.cancelOrder(order_id=order_id, variety=variety)
                    elif broker.lower() == "xts":
                        _broker_object.cancel_order(order_id, _tag, clientID="*****")
                    elif broker.lower() == "5paisa":
                        broker_object.cancel_order(order_id)
                    elif broker.lower() == "dhan":
                        broker_object.cancel_order(order_id)
                    elif broker.lower() == "findoc":
                        broker_object.cancel_order(order_id)
                    elif broker.lower() == "mo":
                        broker_object.CancelOrder(order_id,account_id)
                    
            except Exception as ex:
                self.write_logs("error", f"[_modify_order] Failed to cancel order {order_id}: {str(ex)}")

            if is_adjust_order and ("PARTIALLY_FILLED" not in status.upper()):
                self.write_logs("info", f"[_modify_order] ADJUST order not filled, placing fresh MARKET order for {broker}:{account_id}")
                try:
                    # Normalized quantity back to base units for _place_order_* functions if they expect it
                    _place_qty = quantity
                    if broker.lower() == "alice":
                        reorder_res = self._place_order_alice(account_id, position_type, _broker_object, exchange_token, exchange, _place_qty, product, "MARKET", transaction_type)
                    elif broker.lower() == "trade_master":
                        reorder_res = self._place_order_trade_master(account_id, position_type, _broker_object, exchange_token, exchange, _place_qty, product, "MARKET", transaction_type)
                    elif broker.lower() == "zerodha":
                        reorder_res = self._place_order_zerodha(account_id, position_type, _broker_object, exchange_token, exchange, int(_place_qty), product, "MARKET", transaction_type)
                    elif broker.lower() == "upstock":
                        reorder_res = self._place_order_upstocks(account_id, position_type, broker_object, exchange_token, exchange, int(_place_qty), product, validity, "MARKET", transaction_type)
                    elif broker.lower() == "finvasia":
                        reorder_res = self._place_order_finvasia(account_id, position_type, broker_object, exchange_token, exchange, int(_place_qty), product, "MARKET", transaction_type, variety, validity)
                    elif broker.lower() == "fyers":
                        reorder_res = self._place_order_fyers(account_id, position_type, broker_object, exchange_token, exchange, int(_place_qty), product, "MARKET", transaction_type, variety, validity)
                    elif broker.lower() == "angel":
                        reorder_res = self._place_order_angel(account_id, position_type, broker_object, exchange_token, exchange, int(_place_qty), product, "MARKET", transaction_type, variety, validity)
                    elif broker.lower() == "xts":
                        reorder_res = self._place_order_xts(account_id, position_type, broker_object, exchange_token, exchange, int(_place_qty), product, "MARKET", transaction_type, lotsize, variety, validity, tag=_tag)
                    elif broker.lower() == "5paisa":
                        reorder_res = self._place_order_5paisa(account_id, position_type, broker_object, exchange_token, exchange, int(_place_qty), product, "MARKET", transaction_type, lotsize, variety, validity, tag=_tag)
                    elif broker.lower() == "dhan":
                        reorder_res = self._place_order_dhan(account_id, position_type, broker_object, exchange_token, exchange, int(_place_qty), product, "MARKET", transaction_type, lotsize, variety, validity, tag=_tag)
                    elif broker.lower() == "findoc":
                        reorder_res = self._place_order_findoc(account_id, position_type, broker_object, exchange_token, exchange, int(_place_qty), product, "MARKET", transaction_type, lotsize, variety, validity, tag=_tag)
                    elif broker.lower() == "mo":
                        reorder_res = self._place_order_mo(account_id, position_type, broker_object, exchange_token, exchange, int(_place_qty), product, "MARKET", transaction_type, lotsize, variety, validity, tag=_tag)
                    
                    self.write_logs("info", f"[_modify_order] Fresh MARKET order result: {reorder_res}")
                    if reorder_res.get('status', '').upper() == "SUCCESS":
                        duration = time.time() - start_time
                        self.write_logs("info", f"[_modify_order] Completed in {duration:.2f}s with MARKET fallback")
                        return "MARKET_SUCCESS", reorder_res.get("order_id")
                except Exception as ex:
                    self.write_logs("error", f"[_modify_order] Exception during fresh MARKET order placement: {str(ex)}")

            # If it comes here, it means modification failed and (it wasn't an ADJUST order OR market placement failed)
            
            final_status = "CLOSE_LIMIT_FAILED" if position_type.upper() == "CLOSE" else "OPEN_LIMIT_FAILED"
            msg_prefix = "close" if position_type.upper() == "CLOSE" else "place"
            self._send_message("ALERT", f"Failed to {msg_prefix} order after retries\nAccount [{broker} : id - {account_id}]\n(Symbol - {tsym} ** Side - {transaction_type} ** Ordertype - {order_type} ** ProductType - {product})")
            
            duration = time.time() - start_time
            self.write_logs("error", f"[_modify_order] FAILED in {duration:.2f}s: {final_status}")
            return final_status, reorder_res.get("order_id")

        duration = time.time() - start_time
        self.write_logs("info", f"[_modify_order] SUCCESS in {duration:.2f}s for {broker}:{account_id}")
        return reorder_res['status'].upper(), reorder_res.get("order_id")


    def get_jarvis_ttype(self,transaction_type,position_type):
        is_buy = (transaction_type.upper() == "BUY")
        is_sell = (transaction_type.upper() == "SELL")
        is_closing = (position_type.upper() == "CLOSE")
        is_opeing = (position_type.upper() == "OPEN")
        
        if is_buy and is_opeing: return "BUY"
        elif is_sell and is_closing: return "SELL"
        elif is_sell and is_opeing: return "SHORT"
        elif is_buy and is_closing: return "COVER"
        else:raise ValueError(f"ttype patterns does not match...")
        
    # =====================================================
    # UTILITIES
    # =====================================================
    def get_public_ip(self):
        try:
            return True,requests.get("https://api.ipify.org").text
        except Exception as e:
            return False,str(e)   
        
    def place_kafka_order(self,instrument_data,users_ids,exchange_token:int,exchange:str,product:str,order_type:str,transaction_type:str,position_type:Literal["CLOSE","OPEN"],validity:str=None,price:float=0,trigger_price:float=0,disclosed_quantity:int=0,is_amo:bool=False,api_version:str = 'v2',variety:str="regular",tag:str=""):
        
        # url_ = "http://localhost:8001/place-order"
        qty = users_ids[0][2]
        _product = "NRML" if product in ["CNC","NRML"] else product
        option_type = instrument_data["Option Type"]
        instype = option_type if pd.notna(option_type) and (option_type.upper() in ["CE","PE"]) else "FUT"
        ipstat,source_ip = self.get_public_ip()
        if ipstat:
            payload = {
            "Exc": exchange,
            "SymbolId": instrument_data["Token"],
            "Symbol": instrument_data["Symbol"],
            "Side":  self.get_jarvis_ttype(transaction_type,position_type),
            "OrderType": order_type,
            "ProductType": _product,
            "qty": qty,
            "Price": price, 
            "CallBy": "PythonAlgo",
            "PlaceOrder": True,
            "StrategyName": self.strategy_name,
            "PClose":price,
            "Signature": "JarvisAlgo@123",
            "InstrumentType":instype,
            "SourceIp": source_ip,
            "OrderTag": tag,
            "timestamp":str(datetime.now())
            }
        else:
            print(f"error: Failed to get ip: {str(e)}")
            return False
        try:
            self.producer.send("trading-signals",key ="expiry",value = payload)
            return True
        except Exception as e:
            print(f"error: Failed to send order to kafka: {str(e)}")
            return False
      
    def threaded_place_order(self,users_ids:list,exchange_token:int,exchange:str,product:str,order_type:str,transaction_type:str,position_type:Literal["CLOSE","OPEN"],validity:str=None,price:float=0,trigger_price:float=0,disclosed_quantity:int=0,is_amo:bool=False,api_version:str = 'v2',variety:str="regular",tag:str=""):
        self.write_logs("info",f"Order Executor trigged for {exchange}:{exchange_token} to place {position_type}")
        results={
            "status":"SUCCESS",
            "token":exchange_token,
            "users":{}
            }
        instrument_data = self._get_instrument_data("alice",exchange,exchange_token)
        if not instrument_data:
            return {"status":"REJECTED","message":"invalid_exchange_token","token":exchange_token}
        
        results["symbol"]= instrument_data["Trading Symbol"]
        if len(users_ids) <= 1:
            if users_ids[0][0] == "jarvis":
                return self.place_kafka_order(instrument_data,users_ids,exchange_token,exchange,product,order_type,transaction_type,position_type,validity,price,trigger_price,disclosed_quantity,is_amo,api_version,variety,tag)
        
        for i in range(0, len(users_ids), self.max_workers):
            chunk = users_ids[i:i+self.max_workers]
            futures = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:

                for broker,account_id, qty in chunk:
                    if broker not in results['users']:
                        results['users'][broker] = []
                    try:
                        future = executor.submit(
                            self._place_order,
                            account_id, exchange_token, exchange, qty, product, order_type, transaction_type, position_type, validity, price, trigger_price, disclosed_quantity, is_amo, api_version, variety, tag, self.strategy_name,instrument_data
                        )
                        # Validate future object was created
                        if not future:
                            raise RuntimeError(f"Failed to submit task for {account_id}")
                        futures.append((broker, account_id, future))
                    except Exception as e:
                        self.write_logs("error", f"Task submission failed for {account_id} ({broker}): {str(e)}")
                        results['users'][broker].append({
                            "error": "SUBMIT_FAILED",
                            "message": str(e),
                            "account_id": account_id,
                            "broker": broker,
                            "timestamp": str(datetime.now())
                        })

                for broker_name, account_id, future in futures:
                    try:
                        res = future.result()
                        results['users'][broker_name].append(res)
                    except Exception as e:
                        self.write_logs("error", f"Error in order {account_id} ({broker_name}): {str(e)}")
                        results['users'][broker_name].append({
                            "error": str(e),
                            "account_id": account_id,
                            "broker": broker_name,
                            "timestamp": str(datetime.now())
                        })

            self.write_logs("info", f"[_threaded_place_order] Processed {len(users_ids)} orders")
        return results
    
    def _validate_login_users(self, brokers):
        validate_users = self.merged_df[self.merged_df["broker"].isin(brokers)].reset_index(drop=True)
        unauth = []
        validators = {
            "alice": self._check_alice,
            "trade_master": self._check_trade_master,
            "finvasia": self._check_finvasia,
            "upstock": self._check_upstock,
            "zerodha": self._check_zerodha,
            "angel": self._check_angel,
            "fyers": self._check_fyers,
            "xts": self._check_xts,
            "5paisa": self._check_5paisa,
            "dhan": self._check_dhan
        }

        for i in range(len(validate_users)):
            row = validate_users.loc[i]
            broker = row["broker"]
            uid = row["id"]
            try:
                if broker in validators:
                    status, msg = validators[broker](row)
                    if not status:
                        unauth.append((broker, uid, msg))
                else:
                    unauth.append((broker, uid, "Validator not implemented"))

            except Exception as ex:
                unauth.append((broker, uid, f"Exception: {ex}"))

        return unauth


    # -------- Individual Broker Validators -------- #

    def _check_alice(self, row):
        obj = dill.loads(eval(row["object"]))
        res = obj.get_profile()
        # exit(0)
        if res.get("accountStatus", "").lower() != "activated":
            return False, res.get("emsg")
        return True, None


    def _check_trade_master(self, row):
        obj = dill.loads(eval(row["object"]))
        res = obj.get_profile()
        # print("res -- ",res )
        if res.get("status", "").upper() != "OK":
            return False, res.get("message")
        return True, None


    def _check_finvasia(self, row):
        api = ShoonyaApiPy()
        api.set_session(userid=row["account_id"], password=row["password"], usertoken=row["session_token"])
        res = {}
        _res = api.get_order_book()
        if ((_res == []) or (_res !=[])) and _res is not None:
            res['stat'] = "OK"
        else:
            res['emsg'] = "session expired"
            
        if res.get("stat", "").upper() != "OK" and "session" in str(res.get("emsg", "")).lower():
            return False, res.get("emsg")
        return True, None


    def _check_upstock(self, row):
        try:
            configuration = upstox_client.Configuration()
            configuration.access_token = row["session_token"]
            api = upstox_client.UserApi(upstox_client.ApiClient(configuration))
            res = api.get_profile('2.0').__dict__
        except Exception as ex:
            return False, str(ex)

        if res.get("_status", "").lower() != "success":
            return False, res.get("discriminator")
        return True, None


    def _check_zerodha(self, row):
        obj = dill.loads(eval(row["object"]))
        res = obj.profile()
        # print("res === ", res)
        if 'user_type' not in res:
        # if res.get("status", "").lower() != "success":
            return False, "Session Invalid"
        return True, None


    def _check_angel(self, row):
        obj = dill.loads(eval(row["object"]))
        res = obj.getProfile(obj.refresh_token)
        if res.get("status") is not True:
            return False, res.get("message")
        return True, None


    def _check_fyers(self, row):
        api = fyersModel.FyersModel(client_id=row["app_id"], token=row["session_token"])
        res = api.get_profile()
        if res.get("s", "").lower() != "ok":
            return False, res.get("message")
        return True, None


    def _check_xts(self, row):
        obj = dill.loads(eval(row["object"]))
        res = obj.get_profile()
        if res.get("type", "").lower() != "success":
            return False, res.get("description")
        return True, None


    def _check_5paisa(self, row):
        # print("broker = ", row["broker"])
        api:FivepaisaBroker = dill.loads(eval(row["object"]))
        api.session = httpx.Client(verify=False)
        # print("api == ", api)
        res= {}
        try:
            _res = api.get_orders_history()
        except Exception as ex:
            _res = None
            res['message'] = f"Failed-session-Unauthorized-{str(ex)}"
            
        if ((_res == []) or (_res != [])) and (res is not None) and ('message' not in res):
            res['message']="success"
        msg = str(res.get("message", "")).lower()
        if "session" in msg or "expir" in msg:
            return False, res.get("message")
        return True, None


    def _check_dhan(self, row):
        api = dill.loads(eval(row["object"]))
        res = api.get_fund_limits()
        if res.get("status", "").lower() == "failure":
            return False, res.get("data", {}).get("errorMessage")
        return True, None

    def get_positions(self, user_id, token=None, api_instance=None, broker=None, max_retries=3, timeout=5):
        try:
            with self.merged_df_lock:
                if broker:
                    user_data = self.merged_df[(self.merged_df["id"]==str(user_id)) & (self.merged_df["broker"]==broker)].copy(deep=True)
                else:
                    user_data = self.merged_df[self.merged_df["id"]==str(user_id)].copy(deep=True)
            
            if user_data.empty:
                self.write_logs("error", f"get_positions: User {user_id} not found")
                return []
            
            user_data = user_data.iloc[0]
            
            if api_instance is None:
                api_instance = self._get_broker_instance(user_data)
                
            if not api_instance:
                self.write_logs("error", f"get_positions: Failed to get broker instance for {user_id}")
                return []

            broker_name = user_data["broker"].lower()
            method_name = f"_get_positions_{broker_name}"
            
            if hasattr(self, method_name):
                method = getattr(self, method_name)
                
                # Retry Loop
                for attempt in range(max_retries):
                    try:
                        # Submit to executor for timeout handling
                        future = self.executor.submit(method, api_instance, user_data, token)
                        return future.result(timeout=timeout)
                    
                    except TimeoutError:
                        self.write_logs("warning", f"get_positions: Timeout for {user_id} on attempt {attempt + 1}")
                        if attempt == max_retries - 1:
                            raise TimeoutError(f"get_positions timed out after {max_retries} attempts")
                            
                    except Exception as e:
                        self.write_logs("error", f"get_positions: Error for {user_id} on attempt {attempt + 1}: {str(e)}")
                        if attempt == max_retries - 1:
                             # Gracefully return empty list on non-timeout usage/errors
                             return []

                    # Exponential backoff: 1s, 2s, 4s...
                    time.sleep(2 ** attempt)
                    
                return []
            else:
                self.write_logs("error", f"get_positions: No handler for broker {broker_name}")
                return []
        except TimeoutError:
            raise # Re-raise timeout to caller as requested
        except Exception as e:
            self.write_logs("error", f"get_positions: Exception for {user_id}: {str(e)}")
            return []

    def get_order_history(self, user_id, orders=None, api_instance=None, broker=None, max_retries=3, timeout=5):
        try:
            with self.merged_df_lock:
                if broker:
                    user_data = self.merged_df[(self.merged_df["id"]==str(user_id)) & (self.merged_df["broker"]==broker)].copy(deep=True)
                else:
                    user_data = self.merged_df[self.merged_df["id"]==str(user_id)].copy(deep=True)
            
            if user_data.empty:
                self.write_logs("error", f"get_order_history: User {user_id} not found")
                return []
            
            user_data = user_data.iloc[0]
            
            if api_instance is None:
                api_instance = self._get_broker_instance(user_data)
                
            if not api_instance:
                self.write_logs("error", f"get_order_history: Failed to get broker instance for {user_id}")
                return []

            broker_name = user_data["broker"].lower()
            method_name = f"_get_order_history_{broker_name}"
            if hasattr(self, method_name):
                method = getattr(self, method_name)
                
                # Retry Loop
                for attempt in range(max_retries):
                    try:
                        # Submit to executor for timeout handling
                        future = self.executor.submit(method, api_instance, user_data, orders)
                        return future.result(timeout=timeout)
                    
                    except TimeoutError:
                        self.write_logs("warning", f"get_order_history: Timeout for {user_id} on attempt {attempt + 1}")
                        if attempt == max_retries - 1:
                            raise TimeoutError(f"get_order_history timed out after {max_retries} attempts")
                            
                    except Exception as e:
                        self.write_logs("error", f"get_order_history: Error for {user_id} on attempt {attempt + 1}: {str(e)}")
                        if attempt == max_retries - 1:
                             return []

                    # Exponential backoff: 1s, 2s, 4s...
                    time.sleep(2 ** attempt)
                    
                return []
            else:
                self.write_logs("error", f"get_order_history: No handler for broker {broker_name}")
                return []
        except TimeoutError:
            raise
        except Exception as e:
            self.write_logs("error", f"get_order_history: Exception for {user_id}: {str(e)}")
            return []

    # --- Broker Specific Implementations ---

    def _get_positions_alice(self, broker_obj, user_data, token=None):
        try:
            positions = broker_obj.get_netwise_positions()
            if isinstance(positions, dict) and 'emsg' in positions:
                 self.write_logs("warning", f"Alice positions error: {positions['emsg']}")
                 return []
            if token:
                positions = [p for p in positions if str(p.get("Token")) == str(token)]
            return positions
        except Exception as e:
            self.write_logs("error", f"Alice get_positions error: {str(e)}")
            return []

    def _get_order_history_alice(self, broker_obj, user_data, orders=None):
        try:
            history = broker_obj.get_order_history('')
            if isinstance(history, dict) and 'emsg' in history:
                return []
            if orders:
                history = [o for o in history if o.get("Nstordno") in orders]
            return history
        except Exception as e:
            self.write_logs("error", f"Alice get_order_history error: {str(e)}")
            return []

    def _get_positions_zerodha(self, broker_obj, user_data, token=None):
        try:
            positions = broker_obj.positions()
            net_positions = positions.get("net", [])
            if token:
                net_positions = [p for p in net_positions if str(p.get("instrument_token")) == str(token)]
            return net_positions
        except Exception as e:
            self.write_logs("error", f"Zerodha get_positions error: {str(e)}")
            return []

    def _get_order_history_zerodha(self, broker_obj, user_data, orders=None):
        try:
            history = broker_obj.orders()
            if orders:
                history = [o for o in history if o.get("order_id") in orders]
            return history
        except Exception as e:
             self.write_logs("error", f"Zerodha get_order_history error: {str(e)}")
             return []

    def _get_positions_fyers(self, broker_obj, user_data, token=None):
        try:
            response = broker_obj.positions()
            if response.get("s") != "ok":
                 self.write_logs("warning", f"Fyers positions error: {response.get('message')}")
                 return []
            positions = response.get("netPositions", [])
            if token:
                 positions = [p for p in positions if str(p.get("id")) == str(token) or str(p.get("symbol")) == str(token)]
            return positions
        except Exception as e:
            self.write_logs("error", f"Fyers get_positions error: {str(e)}")
            return []

    def _get_order_history_fyers(self, broker_obj, user_data, orders=None):
        try:
            response = broker_obj.orderbook()
            if response.get("s") != "ok":
                return []
            history = response.get("orderBook", [])
            if orders:
                history = [o for o in history if o.get("id") in orders]
            return history
        except Exception as e:
            self.write_logs("error", f"Fyers get_order_history error: {str(e)}")
            return []

    def _get_positions_upstock(self, broker_obj, user_data, token=None):
        try:
            configuration = upstox_client.Configuration()
            configuration.access_token = broker_obj
            api_instance = upstox_client.PortfolioApi(upstox_client.ApiClient(configuration))
            response = api_instance.get_positions(api_version='v2').to_dict()
            positions = []
            if response.get("data"):
                 positions = response.get("data")
            
            if token:
                 positions = [p for p in positions if str(p.get("instrument_token")) == str(token)] 
            return positions
        except Exception as e:
            self.write_logs("error", f"Upstox get_positions error: {str(e)}")
            return []

    def _get_order_history_upstock(self, broker_obj, user_data, orders=None):
        try:
            configuration = upstox_client.Configuration()
            configuration.access_token = broker_obj
            api_instance = upstox_client.OrderApi(upstox_client.ApiClient(configuration))
            response = api_instance.get_order_book(api_version='v2').to_dict()
            history = response.get("data", [])
            if orders:
                history = [o for o in history if o.get("order_id") in orders]
            return history
        except Exception as e:
            self.write_logs("error", f"Upstox get_order_history error: {str(e)}")
            return []

    def _get_positions_angel(self, broker_obj, user_data, token=None):
        try:
             for i in range(3):
                 positions = broker_obj.position()
                 if positions.get("status") == False:
                     if "access denied" in positions.get("message", "").lower():
                        time.sleep(1)
                        continue
                     return []
                 data = positions.get("data", [])
                 if token:
                     data = [p for p in data if str(p.get("symboltoken")) == str(token)]
                 return data
             return []
        except Exception as e:
             self.write_logs("error", f"Angel get_positions error: {str(e)}")
             return []

    def _get_order_history_angel(self, broker_obj, user_data, orders=None):
        try:
            for i in range(3):
                history = broker_obj.orderBook()
                if history.get("status") == False:
                    if "access denied" in history.get("message", "").lower():
                        time.sleep(1)
                        continue
                    return []
                data = history.get("data", [])
                if orders:
                    data = [o for o in data if o.get("orderid") in orders]
                return data
            return []
        except Exception as e:
             self.write_logs("error", f"Angel get_order_history error: {str(e)}")
             return []

    def _get_positions_finvasia(self, broker_obj, user_data, token=None):
        try:
            positions = broker_obj.get_positions()
            if positions is None: return []
            if token:
                positions = [p for p in positions if str(p.get("token")) == str(token)]
            return positions
        except Exception as e:
            self.write_logs("error", f"Finvasia get_positions error: {str(e)}")
            return []

    def _get_order_history_finvasia(self, broker_obj, user_data, orders=None):
        try:
            history = broker_obj.get_order_book()
            if history is None: return []
            if orders:
                history = [o for o in history if o.get("nordno") in orders]
            return history
        except Exception as e:
            self.write_logs("error", f"Finvasia get_order_history error: {str(e)}")
            return []

    def _get_positions_5paisa(self, broker_obj, user_data, token=None):
        try:
            positions = broker_obj.get_netwise_positions()
            if token:
                positions = [p for p in positions if str(p.get("ScripCode")) == str(token)]
            return positions
        except Exception as e:
             self.write_logs("error", f"5paisa get_positions error: {str(e)}")
             return []

    def _get_order_history_5paisa(self, broker_obj, user_data, orders=None):
        try:
             history = broker_obj.get_orders_history()
             if orders:
                 history = [o for o in history if str(o.get("Broker order ID")) in orders]
             return history
        except Exception as e:
             self.write_logs("error", f"5paisa get_order_history error: {str(e)}")
             return []

    def _get_positions_dhan(self, broker_obj, user_data, token=None):
        try:
            positions = broker_obj.get_positions()
            if positions.get("status") == "failure": return []
            data = positions.get("data", [])
            if token:
                 data = [p for p in data if str(p.get("securityId")) == str(token)]
            return data
        except Exception as e:
             self.write_logs("error", f"Dhan get_positions error: {str(e)}")
             return []

    def _get_order_history_dhan(self, broker_obj, user_data, orders=None):
        try:
            history = broker_obj.get_order_list()
            if history.get("status") == "failure": return []
            data = history.get("data", [])
            if orders:
                 data = [o for o in data if o.get("orderId") in orders]
            return data
        except Exception as e:
             self.write_logs("error", f"Dhan get_order_history error: {str(e)}")
             return []

    def _get_positions_trade_master(self, broker_obj:TradeHub, user_data, token=None):
        try:
            positions = broker_obj.get_positions().get("result", [])
            
            if isinstance(positions, dict) and 'message' in positions:
                self.write_logs("warning", f"Trade master positions error: {positions['emsg']}")
                return []
            if token:
                positions = [p for p in positions if str(int(p.get("instrumentId"))) == str(int(token))]
            return positions
        except Exception as e:
            self.write_logs("error", f"Trade master get_positions error: {str(e)}")
            return []
        
        return self._get_positions_alice(broker_obj, user_data, token)

    def _get_order_history_trade_master(self, broker_obj:TradeHub, user_data, orders=None):
        try:
            history = broker_obj.get_orderbook().get("result",[])
            if isinstance(history, dict) and 'message' in history:
                return []
            if orders:
                history = [o for o in history if o.get("brokerOrderId") in orders]
            return history
        except Exception as e:
            self.write_logs("error", f"Trade master get_order_history error: {str(e)}")
            return []
        return self._get_order_history_alice(broker_obj, user_data, orders)

    def _get_positions_xts(self, broker_obj, user_data, token=None):
         try:
             response = broker_obj.get_position_netwise()
             if response.get("type") != "success": return []
             data = response.get("result", {}).get("positionList", [])
             if token:
                  data = [p for p in data if str(p.get("ExchangeInstrumentID")) == str(token)]
             return data
         except Exception as e:
             self.write_logs("error", f"XTS get_positions error: {str(e)}")
             return []

    def _get_positions_findoc(self, broker_obj:FindocAPI, user_data, token=None):
         try:
             response = broker_obj.get_positions(_type="NetWise")
             data = response.get("positionList", [])
             if token:
                  data = [p for p in data if str(p.get("ExchangeInstrumentID")) == str(token)]
             return data
         except Exception as e:
             self.write_logs("error", f"findoc get_positions error: {str(e)}")
             return []
         
    def _get_positions_mo(self, broker_obj:MOFSLOPENAPI, user_data, token=None):
         try:
             response = broker_obj.GetPosition(user_data['account_id'])
             data = response.get("data", [])
             if token:
                  data = [p for p in data if str(p.get("symboltoken")) == str(token)]
             return data
         except Exception as e:
             self.write_logs("error", f"motilal oswal get_positions error: {str(e)}")
             return []

    def _get_order_history_xts(self, broker_obj, user_data, orders=None):
        try:
             response = broker_obj.get_order_book()
             if response.get("type") != "success": return []
             data = response.get("result", [])
             if orders:
                  data = [o for o in data if o.get("AppOrderID") in orders]
             return data
        except Exception as e:
             self.write_logs("error", f"XTS get_order_history error: {str(e)}")
             return []

    def _get_order_history_findoc(self, broker_obj:FindocAPI, user_data, orders=None):
        try:
             response = broker_obj.get_order_history()
             if not response : return []
             data = response
             if orders:
                  data = [o for o in data if o.get("AppOrderID") in orders]
             return data
        except Exception as e:
             self.write_logs("error", f"XTS get_order_history error: {str(e)}")
             return []
         
    def _get_order_history_mo(self, broker_obj:MOFSLOPENAPI, user_data, orders=None):
        try:
             response = broker_obj.GetOrderBook(user_data['account_id'])
             if "data" not in response : return []
             data = response['data']
             if orders:
                 
                  data = [o for o in data if o.get("orderid") in orders]
             return data
        except Exception as e:
             self.write_logs("error", f"Motilal Oswal get_order_history error: {str(e)}")
             return []

        
                
    def validate_users(self,brokers):
        unauth_users =  self._validate_login_users(brokers)   
        if unauth_users != []:
            message = f"Unauthorized users \n{unauth_users}"
            self._send_message("ERROR",message)
            exit(0)

def main():
    OrderManager = OrderDispatcher(f"logged_users{datetime.now().date()}.csv","users_details.csv","MK","redis")
    OrderManager.enable_logging = True

    print("Started at ",datetime.now())

    exchange_token = 44465
  
    users_ids = [
    #    ( "trade_master","1805656",1),
       ( "jarvis","1805656",0),
    #    ( "angel","J252505",1),
    #    ( "5paisa","56988153",1),
    #    ( "fyers","YJ06745",1),
    #    ( "upstock","4MAUQ9",1),
    #    ( "zerodha","ILR269",1),
    #    ( "dhan","1109097409",1),
    ]
  
    print("Order placement".center(50,"*"))
    print("Time start ",datetime.now())
    respone = OrderManager.threaded_place_order(users_ids,exchange_token,"NFO","CNC","LIMIT",transaction_type="BUY",position_type="OPEN")
    # print(respone)
    logging.info(f"Response: {respone}")
    print(f"Response: {respone}")
    logging.info(f"Time end {datetime.now()}")
    print("".center(50,"*"))
    print("Time end ",datetime.now())
    time.sleep(10)

if __name__ == "__main__":
    # pass
    main()
 
