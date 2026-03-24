import requests
from typing import Optional, Dict, Any,Literal
from datetime import datetime

class FindocAPI:
    """
    Production-ready API client for FINDOC B2C REST API.
    Based on official OpenAPI specification.
    """

    def __init__(self,client_id):
        """
        Parameters:
            base_url (str): API base URL (Example: https://stockz.findoc.com)
        """
        # self.base_url = "https://ctrade.jainam.in:3001"
        self.base_url = "https://xts.myfindoc.com"
        self.session = requests.Session()
        self.access_token: Optional[str] = None
        self.broadcast_token: Optional[str] = None
        self.client_id = client_id
        self.payload_client_id = "AD71"
        # self.payload_client_id = client_id[-4:]
    # ==========================================================
    # Internal Helpers
    # ==========================================================

    def _headers(self) -> Dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            # "Authorization":self.access_token
        }
        if self.access_token:
            headers["Authorization"] = f"{self.access_token}"
        return headers

    def _handle_response(self, response: requests.Response) -> Dict[str, Any]:
        """
        Standard response handler based on API response structure.
        """
        # try:
        #     response.raise_for_status()
        # except requests.HTTPError as e:
        #     oass
        #     raise Exception(f"HTTP Error {response.status_code}: {response.text}")

        data = response.json()
        # print("data === ", data)
        # if data.get("type") != "success":
        #     raise Exception(f"API Error: {data.get('description')}")

        return data.get("result", data)

    # ==========================================================
    # 🔐 Authentication
    # ==========================================================

    def login(
        self,
        secretKey: str,
        api_key: str,
        source: str = "WEBAPI",
    ) -> Dict[str, Any]:
        """
        Login and generate access token.

        Endpoint:
            POST /authentication/v1/user/session

        Required Parameters:
            user_id (CAPS)
            password
            second_auth
            api_key
            source (WEBAPI / MOBILEAPI)

        Returns:
            dict: Login response including access_token
        """

        url = f"{self.base_url}/interactive/user/session"

        payload = {
            "appKey": api_key,
            "secretKey": secretKey,
            "source": source
        }
        response = self.session.post(url, json=payload, headers=self._headers())
        data = self._handle_response(response)
        self.access_token = data["token"]

        return data

    def logout(self) -> Dict[str, Any]:
        """
        Logout and destroy session.

        Endpoint:
            DELETE /authentication/v1/user/session
        """
        url = f"{self.base_url}/interactive/user/session"
        response = self.session.delete(url, headers=self._headers())
        return self._handle_response(response)

    # ==========================================================
    # 💰 Balance / Profile
    # ==========================================================

    def get_balance(self) -> Dict[str, Any]:
        """
        Get segment-wise balance.

        Endpoint:
            GET /authentication/v1/user/balance
        """
        url = f"{self.base_url}/interactive/user/balance?clientID=*****"
        payload = {}
        
        response = self.session.get(url,json=payload, headers=self._headers())
        return self._handle_response(response)
    
    def get_profile(self) -> Dict[str, Any]:
        """
        Get segment-wise balance.

        Endpoint:
            GET /authentication/v1/user/balance
        """
        url = f"{self.base_url}/interactive/user/profile?clientID=*****"
        payload = {}
        
        response = self.session.get(url,json=payload, headers=self._headers())
        return self._handle_response(response)

    # ==========================================================
    # 📌 Orders
    # ==========================================================

    def place_order(
        self,
        exchange:Literal["NSECM","BSECM","NSEFO","BSEFO","MCXFO"],
        exchange_token: int,
        transaction_type: Literal["BUY","SELL"],
        product_type: Literal["MIS","NRML","CNC"],
        order_type: Literal["LIMIT","MARKET"],
        quantity: int,
        price: float = 0,
        stop_price: float = 0,
        disclosed_quantity: int = 0,
        validity: str = "DAY",
        order_tag :Optional[str]=None,
        # is_amo: bool = False,
    ) -> Dict[str, Any]:
        """
        Place regular order.

        Endpoint:
            POST /interactive/orders
        """

        url = f"{self.base_url}/interactive/orders"

        payload = payload = {
        "exchangeSegment": exchange,
        "exchangeInstrumentID": exchange_token,
        "productType": product_type,
        "orderType": order_type,
        "orderSide": transaction_type,
        "timeInForce": validity,
        "disclosedQuantity": disclosed_quantity,
        "orderQuantity": quantity,
        "limitPrice": price,
        "stopPrice": stop_price,
        "clientID":self.payload_client_id,
        # "orderUniqueIdentifier": order_tag
        }
        if order_tag:
            payload["orderUniqueIdentifier"] = order_tag
        with open("findoc_test.log","a") as fL:
            log_ = f"Order Place | url : {url} | {datetime.now()} | payload : {payload}" 
            fL.write(log_)
        print("self._headers() == ", self._headers())
        response = self.session.post(url, json=payload, headers=self._headers())
        with open("findoc_test.log","a") as fL:
            log_ = f"Order Place | url : {url} | {datetime.now()} | response : {vars(response)}\n" 
            fL.write(log_)
        return self._handle_response(response)

    def modify_order(
        self,
        order_id: str,
        product_type: Literal["MIS","NRML","CNC"],
        quantity: int,
        order_type: Literal["LIMIT","MARKET"],
        price: float,
        stop_price: Optional[float] = 0,
        disclosed_quantity: Optional[int] = 0,
        order_tag  : Optional[str]= None
    ) -> Dict[str, Any]:
        """
        Modify pending order.

        Endpoint:
            PUT /interactive/orders
        """

        url = f"{self.base_url}/interactive/orders"

        payload = {
            "appOrderID": order_id,
            "modifiedProductType": product_type,
            "modifiedOrderType": order_type,
            "modifiedOrderQuantity": quantity,
            "modifiedDisclosedQuantity": disclosed_quantity,
            "modifiedLimitPrice": price,
            "modifiedStopPrice": stop_price,
            "modifiedTimeInForce": "DAY",
            "orderUniqueIdentifier": order_tag,
            "clientID":self.payload_client_id
            }
        if not order_tag:
            payload['orderUniqueIdentifier'] = "024870146"
        with open("findoc_test.log","a") as fL:
            log_ = f"modify Order | url : {url} | {datetime.now()} | payload : {payload}" 
            fL.write(log_)
        response = self.session.put(url, json=payload, headers=self._headers())
        with open("findoc_test.log","a") as fL:
            log_ = f"modify Order | url : {url} | {datetime.now()} | response : {vars(response)}\n" 
            fL.write(log_)
        return self._handle_response(response)

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """
        Cancel order.

        Endpoint:
            DELETE /interactive/orders?appOrderID={order_id}clientID={self.payload_client_id}
        """

        url = f"{self.base_url}/interactive/orders?appOrderID={order_id}&clientID={self.payload_client_id}"
        payload = {}
        # payload = {"appOrderID": order_id,"clientID":self.payload_client_id}
        response = self.session.delete(url,json=payload, headers=self._headers())
        with open("findoc_test.log","a") as fL:
            log_ = f"cancel Order | url : {url} | {datetime.now()} | response : {vars(response)}\n" 
            fL.write(log_)
        return self._handle_response(response)

    def cancel_all_order(self, exchange :Literal["NSECM","BSECM","NSEFO","BSEFO","MCXFO"],exchange_token=0) -> Dict[str, Any]:
        """
        Cancel All order.

        Endpoint:
            POST /interactive/orders/cancelall
        """

        url = f"{self.base_url}/interactive/orders/cancelall"
        payload =   {
            "exchangeSegment": exchange,
            "exchangeInstrumentID": exchange_token
        }
        response = self.session.post(url,json=payload, headers=self._headers())
        return self._handle_response(response)

    # ==========================================================
    # 📜 Order Book / History
    # ==========================================================

    def get_order_history_id(self, order_id: str) -> Dict[str, Any]:
        """
        Get specific order history.

        Endpoint:
            GET /transactional/v1/orders/regular/{exchange}/{order_id}
        """
        url = f"{self.base_url}/interactive/orders?appOrderID={order_id}&clientID={self.payload_client_id}"
        payload = {}
        response = self.session.get(url,json=payload, headers=self._headers())
        with open("findoc_test.log","a") as fL:
            log_ = f"Order history id | url : {url} | {datetime.now()} | response : {vars(response)}\n" 
            fL.write(log_)
        return self._handle_response(response)

    def get_order_history(self) -> Dict[str, Any]:
        """
        Get order history.

        Endpoint:
            GET /interactive/orders?clientID=*****
        """
        # 'https://xts.myfindoc.com/interactive/orders/dealerorderbook?clientID=AD71'
        url = f"{self.base_url}/interactive/orders/dealerorderbook?clientID={self.payload_client_id}"
        # url = f"{self.base_url}/interactive/orders?clientID=*****"
        payload = {}
        response = self.session.get(url,json=payload, headers=self._headers())
        with open("findoc_test.log","a") as fL:
            log_ = f"Order history | url : {url} | {datetime.now()} | response : {vars(response)}\n" 
            fL.write(log_)
        return self._handle_response(response)

    # ==========================================================
    # 📊 Portfolio
    # ==========================================================

    def get_positions(self,_type:Literal["DayWise","NetWise"] = "NetWise") -> Dict[str, Any]:
        """
        Get netwise positions.

        Endpoint (Portfolio Section):
            GET /interactive/portfolio/positions?dayOrNet={_type}clientID={self.payload_client_id}
        """
        url = f"{self.base_url}/interactive/portfolio/positions?dayOrNet={_type}&clientID={self.payload_client_id}"
        payload = {"clientID":self.payload_client_id}
        with open("findoc_test.log","a") as fL:
            log_ = f"positions | url : {url} | {datetime.now()} | payload : {payload}" 
            fL.write(log_)
        response = self.session.get(url,json=payload, headers=self._headers())
        
        with open("findoc_test.log","a") as fL:
            log_ = f"positions | url : {url} | {datetime.now()} | response : {vars(response)}\n" 
            fL.write(log_)
        return self._handle_response(response)
    
    
def main():
    credentials = {
        "secretKey": "Vpcj025@L5",
        "api_key": "3bd58b3ffa0c5bf3a59b72",
    }
    finobj = FindocAPI("X14AD71")
    print(finobj.login(**credentials))
    print("finobj == ",finobj.access_token)
    # print(finobj.place_order("NSEFO","5866","BUY","NRML","MARKET",1,0))
    # {'AppOrderID': 1210049745, 'ClientID': '*****'}
    # {'AppOrderID': 1210049744, 'ClientID': '*****'}
    # {'status': 400, 'statusText': 'Bad Request', 'errors': [{'field': ['orderSide'], 'location': 'body', 'messages': ['"orderSide" must be one of [BUY, SELL]'], 'types': ['any.allowOnly']}]}
    # 1210049744
    # print(finobj.modify_order("1210049744","NRML",75,"MARKET","5"))
    # print(finobj.get_order_history_id("1210049746"))
    # [{'LoginID': 'X14AD71', 'ClientID': 'Pro', 'AppOrderID': 1210030263, 'OrderReferenceID': '', 'GeneratedBy': 'TWSAPI', 'ExchangeOrderID': '', 'OrderCategoryType': 'NORMAL', 'ExchangeSegment': 'NSEFO', 'ExchangeInstrumentID': 42622, 'OrderSide': 'BUY', 'OrderType': 'Limit', 'ProductType': 'NRML', 'TimeInForce': 'DAY', 'OrderPrice': 1.45, 'OrderQuantity': 65, 'OrderStopPrice': 0, 'OrderStatus': 'Rejected', 'OrderAverageTradedPrice': '', 'LeavesQuantity': 65, 'CumulativeQuantity': 0, 'OrderDisclosedQuantity': 0, 'OrderGeneratedDateTime': '04-02-2026 14:22:11', 'ExchangeTransactTime': '04-02-2026 14:22:11', 'LastUpdateDateTime': '04-02-2026 14:22:11', 'OrderExpiryDate': '01-01-1980 00:00:00', 'CancelRejectReason': "Gateway:DMA order can't be placed, for PRO Orders", 'OrderUniqueIdentifier': '2026-02-04 14:22:1', 'OrderLegStatus': 'SingleOrderLeg', 'TradingSymbol': 'NIFTY 10FEB2026 CE 27500', 'ApiOrderSource': '', 'IsSpread': False, 'MessageCode': 9004, 'MessageVersion': 4, 'TokenID': 0, 'ApplicationType': 0, 'SequenceNumber': 0, 'IsAMO': False}]
    print(finobj.get_order_history())
    # [{'LoginID': 'X14AD71', 'ClientID': 'AD71', 'AppOrderID': 1210030180, 'OrderReferenceID': '', 'ExchangeOrderID': '', 'OrderCategoryType': 'NORMAL', 'ExchangeSegment': 'NSEFO', 'ExchangeInstrumentID': 42622, 'OrderSide': 'BUY', 'OrderType': 'Limit', 'ProductType': 'NRML', 'TimeInForce': 'DAY', 'OrderPrice': 1.5, 'OrderQuantity': 65, 'OrderStopPrice': 0, 'OrderStatus': 'Rejected', 'OrderAverageTradedPrice': '', 'LeavesQuantity': 65, 'CumulativeQuantity': 0, 'OrderDisclosedQuantity': 0, 'OrderGeneratedDateTime': '04-02-2026 13:45:23', 'ExchangeTransactTime': '04-02-2026 13:45:23', 'TradingSymbol': 'NIFTY 10FEB2026 CE 27500', 'LastUpdateDateTime': '04-02-2026 13:45:23', 'OrderExpiryDate': '01-01-1980 00:00:00', 'CancelRejectReason': 'OEMS:RMS : Margin Exceeds :  - Set Limit:[0] Total Required Margin:[97.5] Available Margin[0] Margin Shortfall[97.5] for entity [Client]-[AD71] across [ALL|ALL|ALL]', 'OrderUniqueIdentifier': '2026-02-04 13:45:2', 'OrderLegStatus': 'SingleOrderLeg', 'BoLegDetails': 0, 'IsSpread': False, 'BoEntryOrderId': '', 'ApiOrderSource': '', 'MessageCode': 9004, 'MessageVersion': 4, 'TokenID': 0, 'ApplicationType': 0, 'SequenceNumber': 0, 'IsAMO': False}]
    # print(finobj.get_order_history_id("1210030263"))
    # print(finobj.cancel_order(order_id="1210049744"))
    # print(finobj.cancel_all_order(exchange="NSEFO"))
    # print(finobj.get_balance())
    # print(finobj.get_profile())
    # print(finobj.get_positions())
    

if __name__ == "__main__":
    main()


"""curl -i -X GET \
   -H "Authorization:eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOiJYMTRBRDcxXzNCRDU4QjNGRkEwQzVCRjNBNTlCNzIiLCJwdWJsaWNLZXkiOiIzYmQ1OGIzZmZhMGM1YmYzYTU5YjcyIiwiaXNJbnRlcmFjdGl2ZSI6dHJ1ZSwiaWF0IjoxNzcwMTk3NTA3LCJleHAiOjE3NzAyODM5MDd9.vb_Fy-LFnLAHUArS3b6lEFM38x_X5MXF-1uPPr-bX4Y" \
 'https://xts.myfindoc.com//interactive/orders?appOrderID=1210030180&clientID=*****'
 
 curl -i -X GET \
   -H "Authorization:eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySUQiOiJYMTRBRDcxXzNCRDU4QjNGRkEwQzVCRjNBNTlCNzIiLCJwdWJsaWNLZXkiOiIzYmQ1OGIzZmZhMGM1YmYzYTU5YjcyIiwiaXNJbnRlcmFjdGl2ZSI6dHJ1ZSwiaWF0IjoxNzcwMjAyMzA2LCJleHAiOjE3NzAyODg3MDZ9.i7VZysy0yymzBZ7NmW6xIHvLCkWUBaylmS3Bih2GA7A" \
 'https://xts.myfindoc.com/interactive/orders?clientID=*****'
 
 """