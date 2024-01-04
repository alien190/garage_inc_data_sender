import mysql.connector
from logger import Logger

class DataGetter(object):
    
    def __init__(self, user, password, logger: Logger):
        self.user = user
        self.password = password
        self.mydb = None
        self.logger = logger
     
    def __enter__(self):
        try:
            self.mydb = mysql.connector.connect(host='localhost',
                                       user=self.user,
                                       password=self.password,
                                       database = 'monitor')
            self.logger.log_info('Connected to DB')
        except Exception as error:
            self.logger.log_error(str(error))     
        return self
 
    def __exit__(self, *args):
        if self.mydb == None:
            self.logger.log_error('Can not close connection to DB')
            return
        
        self.mydb.close() 
        self.logger.log_info('DB connection is closed')

    def getData(self, last_timestamp: int, sensor_id: int, sensor_type: int, limit:int):
        if self.mydb == None:
            self.logger.log_error('Can not use DB')
            return

        mycursor = self.mydb.cursor(dictionary=True)

        sql = self.getQuery(sensor_type)
        val = (last_timestamp, sensor_id, limit)

        mycursor.execute(sql, val)
        return mycursor.fetchall()
    
    def getQuery(sensor_type: int):
        if(sensor_type == 0):
            return """SELECT * FROM temperatures 
                    WHERE timestamp >= %s
                    AND sensor_id = %s
                    ORDER BY timestamp ASC
                    LIMIT %s
                    """
        
        if(sensor_type == 1):
            return """SELECT * FROM airflows 
                    WHERE timestamp >= %s
                    AND sensor_id = %s
                    ORDER BY timestamp ASC
                    LIMIT %s
                    """
    
    def getSensors(self):
        if self.mydb == None:
            self.logger.log_error('Can not use DB')
            return

        mycursor = self.mydb.cursor(dictionary=True)

        sql = "SELECT * FROM sensors WHERE is_active = %s"
        
        val = (1)
        mycursor.execute(sql, val)
        return mycursor.fetchall()