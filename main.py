import yaml
from logger import Logger
from data_getter import DataGetter
from data_sender import DataSender
from time import sleep

sql_limit = 25
sleep_seconds_default = 60
sleep_seconds = sleep_seconds_default

def main():
    while(True):
        try:
            with open('config.yaml', 'r') as file:
                config = yaml.safe_load(file)

            db_user = config['db_user']
            db_password = config['db_password']
            remote_host = config['remote_host']
            is_debug_enabled = True if config['is_debug_enabled'] == 1 else False
            sleep_seconds = config['sleep_seconds'] if 'sleep_seconds' in config.keys() else sleep_seconds_default
            logger = Logger(filename='data_sender.log', is_debug_enabled=is_debug_enabled)
            dataSender = DataSender()

            with DataGetter(db_user, db_password, logger) as dataGetter:
                sensors = dataGetter.getSensors()
                for sensor in sensors:
                    sensor_id = sensor['sensor_id']
                    sensor_type = sensor['sensor_type']

                    last_timestamp = dataSender.request_last_uploaded_timestamp(remote_host, sensor_id, sensor_type)

                    logger.log_info("sensor_id:" + str(sensor_id) 
                                    + ", sensor_type:" + str(sensor_type) 
                                    + ", last_timestamp:" + str(last_timestamp))

            
                    while(True):
                        data = dataGetter.getData(last_timestamp, sensor_id, sensor_type, sql_limit)
                        dataSender.send_data(remote_host, data, sensor_type)
                        if(len(data) == sql_limit): 
                            last_timestamp = data[sql_limit-1]['timestamp']
                            logger.log_info(str(last_timestamp))
                        else:    
                            break
                
        except Exception as error:
            logger.log_error(str(error))   

        sleep(sleep_seconds)    
        
if __name__ == "__main__":
    main()