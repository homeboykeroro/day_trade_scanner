class Alert:
    def __init__(self, ticker: str, con_id: str, trigger_price: float) -> None:
       self.__ticker = ticker
       self.__con_id = con_id
       self.__trigger_price = trigger_price
       
    def create_alert_conditions(self):
        alert_msg = {
            'alertName': f'{self.__ticker.upper()}',
            'alertMessage': f'{self.__ticker.upper()} {self.__trigger_price}',
            'outsideRth': 1,
            'alertRepeatable': 1,
            'iTWSOrdersOnly': 1,
            'showPopup': 1,
            'tif': 'GTD',
            'conditions': [{
                'conidex': f'{self.__con_id}@SMART',
                'logicBind': 'n',
                'operator': '=',
                'triggerMethod': '0',
                'type': 1,
                'value': f'{str(self.__trigger_price)}'
            }]
        }
        
        return alert_msg