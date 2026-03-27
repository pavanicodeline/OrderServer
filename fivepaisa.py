"""
5paisa Broker Template Class
Provides a template structure for interacting with 5paisa broker API
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum
import logging
from copy import deepcopy
import requests
from pyotp import TOTP
import httpx,dill
from typing import Literal
# Configure logging
logger = logging.getLogger(__name__)




class FivepaisaBroker():
    """
    Abstract base class for 5paisa broker integration
    
    Provides template methods for:
    - Authentication
    - Order placement and management
    - Position tracking
    - Market data retrieval
    """

    def __init__(self, user_id: str, user_key: str,app_password:str, encryption_key:str, client_code: str,pin:str,totp_key:str,app_name:str = None,redirect_url:str = None):
        """
        Initialize 5paisa broker connection
        
        Args:
            api_key: API key for authentication
            api_secret: API secret for authentication
            client_code: Client code for the broker account
        """
        self.app_name = app_name
        self.user_id = user_id
        self.user_key = user_key
        self.app_password = app_password
        self.encryption_key = encryption_key
        self.client_code = client_code
        self.pin = pin
        self.TOTP = TOTP(totp_key)
        self.request_token = None
        self.access_token = None
        logger.info(f"Initializing 5paisa Broker for client: {client_code}")
        self.authenticated = False
        self.session = httpx.Client(verify=False)

        # payloads
        self.GENERIC_PAYLOAD = {"head":{"Key":self.user_key},"body":{}}
        self.headers = {'Content-Type': 'application/json'}
        # urls
        self.BaseUrl = 'https://Openapi.5paisa.com/VendorsAPI/Service1.svc/'
        self.LOGIN_ROUTE = f'{self.BaseUrl}V4/LoginRequestMobileNewbyEmail'
        self.SCRIP_MASTER_ROUTE=f'{self.BaseUrl}ScripMaster/segment/All'
        self.ORDER_BOOK_ROUTE = f'{self.BaseUrl}V3/OrderBook'
        self.HOLDINGS_ROUTE = f'{self.BaseUrl}V3/Holding'
        self.POSITIONS_ROUTE = f'{self.BaseUrl}V2/NetPositionNetWise'
        self.ORDER_PLACEMENT_ROUTE = f'{self.BaseUrl}V1/PlaceOrderRequest'
        self.ORDER_MODIFY_ROUTE = f'{self.BaseUrl}V1/ModifyOrderRequest'
        self.ORDER_CANCEL_ROUTE = f'{self.BaseUrl}V1/CancelOrderRequest'
        self.ORDER_STATUS_ROUTE = f'{self.BaseUrl}V2/OrderStatus'
        self.TRADE_INFO_ROUTE = f'{self.BaseUrl}TradeInformation'
        self.JWT_VALIDATION_ROUTE = "https://Openapi.indiainfoline.com/VendorsAPI/Service1.svc/JWTOpenApiValidation"
        self.HISTORICAL_DATA_ROUTE = "https://openapi.5paisa.com/V2/historical/"
        self.GET_REQUEST_TOKEN_ROUTE = f'{self.BaseUrl}TOTPLogin'
        self.ACCESS_TOKEN_ROUTE = f'{self.BaseUrl}GetAccessToken'
        self.NETPOSITION_ROUTE= f'{self.BaseUrl}V4/NetPosition'
        # self.NETPOSITION_ROUTE= f'{self.BaseUrl}V2/NetPositionNetWise'
        self.MULTIORDERMARGIN_ROUTE=f'{self.BaseUrl}MultiOrderMargin'
        self.MARGIN_ROUTE=f'{self.BaseUrl}V4/Margin'
        self.ORDER_WEBHOOK_ROUTE=f'{self.BaseUrl}feeds/api'

# def parse_response(response):
        
        
    def generate_request_token(self):
        payload = deepcopy(self.GENERIC_PAYLOAD)
        payload["body"] = {
                "Email_ID": self.client_code,
                "TOTP": self.TOTP.now(),
                "PIN": self.pin
            }
        reponse = self.session.post(self.GET_REQUEST_TOKEN_ROUTE,json=payload).json()
        
        # print("reponse === ",reponse)
        
        if ("body" in reponse) and reponse["body"] and ("Message" in reponse["body"]):
            if  reponse["body"]["Message"].lower() == "success":
                self.request_token = reponse["body"]["RequestToken"]
            else:
                raise Exception(reponse["body"]["Message"])
        else:
            if "head" in reponse and reponse['head'] and "StatusDescription" in reponse['head']:
                raise Exception(reponse['head']['StatusDescription'])
            else:
                raise Exception("failed to generate request token")
            
    def generate_access_token(self):
        if self.request_token is None:
            self.generate_request_token()
            
        payload = deepcopy(self.GENERIC_PAYLOAD)
        payload["body"] = {
                "RequestToken": self.request_token,
                "EncryKey": self.encryption_key,
                "UserId": self.user_id
            }
        # print("payload ==== ", payload)
        
        reponse = self.session.post(self.ACCESS_TOKEN_ROUTE,json=payload).json()
        # print("reponse(json) ==== ", reponse)
        if ("body" in reponse) and reponse["body"] and ("Message" in reponse["body"]):
            if  reponse["body"]["Message"].lower() == "success":
                self.access_token = reponse["body"]["AccessToken"]
                self.headers['Authorization']=f'bearer {self.access_token}'
            else:
                raise Exception(reponse["body"]["message"])
        else:
            if "head" in reponse and reponse['head'] and "StatusDescription" in reponse['head']:
                raise Exception(reponse['head']['StatusDescription'])
            else:
                raise Exception("failed to generate request token")
            
        
   
    def authenticate(self) -> bool:
        self.generate_access_token()
        self.authenticated= True    

   
    def place_order(self, scrip_code,exchange:Literal["N","B","M"],exchange_type:Literal["D","C","U"],transaction_type:Literal["B","S"],quantity,product_type:Literal["NRML","MIS"],order_type:Literal["LIMIT","MARKET"],order_tag ="",price=0,DisQty = 0 ,StopLossPrice = 0) -> str:
        """_summary_

        Args:
            scrip_code (str): exchange token by exchange
            exchange (Literal[N,B,M]): This is the exchange of the instrument
                N: NSE
                B: BSE
                M: MCX
            exchange_type (Literal[C,D,U]): This is the exchange segment of the instrument
                C: Cash
                D: Derivatives (FnO of NSE, BSE & MCX)
                U: Currency
            transaction_type (Literal[B,S]): Represents if it is buy or sell order.
                B: Buy 
                S: Sell
            quantity (int): qty to place order
            product_type (Literal[NRML,MIS]): _description_
            order_type (Literal[LIMIT,MARKET]): _description_
            order_tag (str, optional): This is a unique ID which user can generate for his/her reference. Defaults to "".
            price (float, optional): It is the price at which order needs to be placed. (0 for market orders).
            DisQty (int, optional): It is the quantity to be disclosed publicly . Defaults to 0.
            StopLossPrice (int, optional): It is the stop loss price for the order. Defaults to 0.

        Raises:
            Exception: only NRML and MIS order are allowed
            Exception: only N,B and M exchange are allowed
            Exception: only B and S transcation type are allowed
            Exception: only MARKET and LIMIT Order type are allowed

        Returns:
            json: response by 5paisa
        """
        
        if product_type not in ["NRML","MIS"]:
            raise Exception(f"only NRML and MIS order are allowed")
        if exchange not in ["N","B","M"]:
            raise Exception(f"only N,B and M exchange are allowed")
        if transaction_type not in ["B","S"]:
            raise Exception(f"only B and S transcation type are allowed")
        if order_type.upper() not in ["MARKET","LIMIT"]:
            raise Exception(f"only MARKET and LIMIT Order type are allowed")
        
        payload = deepcopy(self.GENERIC_PAYLOAD)
        
        if (product_type == "MIS"):
            is_intraday = True
        else:
            is_intraday = False
            
        payload['body'] = {
            "OrderType": transaction_type,              #/* Buy */
            "Exchange": exchange,               #/* N = NSE */
            "ExchangeType": exchange_type,           #/* C = Cash */
            "ScripCode": str(scrip_code),         # /* Numeric Scrip Code */
            "ScripData": "",               #/* Optional – use if you have ScripData format */
            "Price": str(price),               #/* Order price (use 0 for market orders) */
            "Qty": str(quantity),                      # /* Number of Shares */
            "StopLossPrice": str(StopLossPrice),            # /* Optional SL price */
            "DisQty": DisQty,                   # /* Optional disclosed quantity */
            "IsIntraday": is_intraday,           #  /* true = Intraday; false = Delivery */
            "AHPlaced": "N",               # /* After market order? Y/N */
            "RemoteOrderID": order_tag   # /* Unique client reference */
        }
        if order_type == "MARKET":
            payload['body']['Price'] = 0
        elif (order_type == "LIMIT") and (price <= 0):
            raise Exception("price cant be 0 for LIMIT order")
        response = self.session.post(self.ORDER_PLACEMENT_ROUTE,json=payload,headers=self.headers).json()
        # print("response == ", response)
        # print("response(vars) == ", vars(response))
        # print("response(json) == ", response)
        if ("body" in response) and response['body']:
            return response['body'] 
        else:
            if ("head" in response) and response["head"]:
                return response["head"]
            else:
                return response


   
    def cancel_order(self, order_id: str) -> bool:
        payload = deepcopy(self.GENERIC_PAYLOAD)
        payload["body"]["ExchOrderID"] = order_id
        response = self.session.post(url=self.ORDER_CANCEL_ROUTE,json=payload,headers=self.headers).json()
        # print("response == ", response)
        return response
    
    def modify_order(self, order_id: str, price: str) -> bool:
        if float(price) <=0:
            raise Exception("New price cant be 0.")
        payload = deepcopy(self.GENERIC_PAYLOAD)
        payload["body"]["ExchOrderID"] = order_id
        payload["Price"] = price
        response = self.session.post(url=self.ORDER_MODIFY_ROUTE,json=payload,headers=self.headers).json()
        # print("response == ", response)
        return response
    

   
    def get_tradebook(self) -> list:
        payload = deepcopy(self.GENERIC_PAYLOAD)
        payload["body"]["ClientCode"] = self.client_code
        response = self.session.post(url=self.POSITIONS_ROUTE,json=payload,headers=self.headers).json()
        # print("response == ", response)
        if ("body" in response) and response["body"] and ("TradeBookDetail" in response["body"]):
            return response["body"]["TradeBookDetail"]
        else:
            return response
        
    def get_netwise_positions(self) -> list:
        payload = deepcopy(self.GENERIC_PAYLOAD)
        payload["body"]["ClientCode"] = self.client_code
        response = self.session.post(url=self.NETPOSITION_ROUTE,json=payload,headers=self.headers).json()
        # print("response == ", response)
        if ("body" in response) and response["body"] and ("NetPositionDetail" in response["body"]):
            return response["body"]["NetPositionDetail"]
        else:
            return response
   
    def get_orders_history(self) -> list:
        payload = deepcopy(self.GENERIC_PAYLOAD)
        payload["body"]["ClientCode"] = self.client_code
        response = self.session.post(url=self.ORDER_BOOK_ROUTE,json=payload,headers=self.headers).json()
        # print("response == ", response)
        if ("body" in response) and response["body"] and ("OrderBookDetail" in response["body"]):
            return response["body"]["OrderBookDetail"]
        else:
            return response
        
    def get_order_history(self,order_id) -> list:
        payload = deepcopy(self.GENERIC_PAYLOAD)
        payload["body"]["ClientCode"] = self.client_code
        response = self.session.post(url=self.ORDER_BOOK_ROUTE,json=payload,headers=self.headers).json()
        # print("response == ", response)
        if ("body" in response) and response["body"] and ("OrderBookDetail" in response["body"]):
            orderhistory = response["body"]["OrderBookDetail"]

            order_id = int(order_id)

            # Try Exchange Order ID first
            order = next(
                (o for o in orderhistory if str(o.get("ExchOrderID", "")).isdigit() and int(o["ExchOrderID"]) == order_id),
                None
            )

            # If not found, try Broker Order ID
            if order is None:
                order = next(
                    (o for o in orderhistory if str(o.get("BrokerOrderId", "")).isdigit() and int(o["BrokerOrderId"]) == order_id),
                    None
                )

            return order or {}
            # return next((order for order in orderhistory if (int(order.get("BrokerOrderId")) == int(order_id))), {})
        else:
            return response
   
    def get_account_balance(self) -> Dict[str, float]:
        payload = deepcopy(self.GENERIC_PAYLOAD)
        payload["body"]["ClientCode"] = self.client_code
        payload["body"]["ClientCode"] = self.client_code
        response = self.session.post(url=self.MULTIORDERMARGIN_ROUTE,json=payload,headers=self.headers).json()
        print("response = = ",response)
        if ("body" in response) and response['body']:
            return response['body']
        else:
            return response
        
    def get_order_status(self,order_id,exch:Literal["M","N","B"]) -> Dict[str, float]:
        payload = deepcopy(self.GENERIC_PAYLOAD)
        payload["body"]["ClientCode"] = self.client_code
        payload["body"]["OrdStatusReqList"] = [{"Exch":exch,"RemoteOrderID":order_id}]
        
        response = self.session.post(url=self.ORDER_STATUS_ROUTE,json=payload,headers=self.headers).json()
        # print(response)
        if ("body" in response) and response['body'] and ("OrdStatusResLst" in response['body']):
            return response['body']['OrdStatusResLst']
        else:
            return response
        
        
    def get_holdings(self) -> List[Dict[str, Any]]:
        payload = deepcopy(self.GENERIC_PAYLOAD)
        payload["body"]["ClientCode"] = self.client_code
        response = self.session.post(url=self.HOLDINGS_ROUTE,json=payload,headers=self.headers).json()
        # print("response == ", response)
        if ("body" in response) and response["body"] and ("Data" in response["body"]):
            return response["body"]["Data"]
        else:
            return response
        


if __name__ == "__main__":
    class_parms = {
    "user_id": "oQAzvA2GowP",
    "user_key": "nxNxc6UPyjbeiVUMDZFINmNFYcMyxm4X",
    "app_password": "gLI00xNJJTn",
    "encryption_key": "6dd4Gs3HmuLqAR4N2j4nVnDbl7HpeExG",
    "client_code": "56988153",
    "pin": "032002",
    "totp_key": "GU3DSOBYGE2TGXZVKBDUWRKZ",
    "app_name": "ORDERMED"
    }
    fpai = FivepaisaBroker(**class_parms)
    fpai.authenticate()
    
    print(vars(fpai))
