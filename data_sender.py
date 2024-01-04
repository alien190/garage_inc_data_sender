import json
import requests

class DataSender(object):
    def request_last_uploaded_timetamp(self, remote_host: str, sensor_id:int, sensor_type:int):
        request = requests.get("http://" + remote_host + self.get_last_timestamp_endpoint(sensor_type),  
                            params= {'sensor_id': sensor_id})
        if(request.status_code != 200): 
            raise Exception('Error! Can not get last_uploaded_timestamp. Status code:' + str(request.status_code))

        data = json.loads(request.text)
        return data['last_timestamp']     

    def send_data(self, remote_host: str, data, sensor_type:int):
        data_to_send = {'values' : data}
        request = requests.post("http://" + remote_host + self.get_update_endpoint(sensor_type), json=data_to_send)
        if(request.status_code != 200): 
            raise Exception('Can not upload data. Staus code:' + str(request.status_code))        
        

    def get_last_timestamp_endpoint(self, sensor_type:int):
        if(sensor_type == 0):
            return "/get_data/temperatures_last_timestamp/"
        if(sensor_type == 1):
            return "/get_data/air_flows_last_timestamp/"
        return ""
    
    def get_update_endpoint(self, sensor_type:int):
        if(sensor_type == 0):
            return "/update_data/temperatures/"
        if(sensor_type == 1):
            return "/update_data/air_flows/"
        return ""