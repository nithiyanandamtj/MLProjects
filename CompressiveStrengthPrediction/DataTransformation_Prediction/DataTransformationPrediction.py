from os import listdir
import pandas
from Application_Logging.logger import App_Logger

class DataTransformPredict:
    """This class is for transforming good raw Prediction data before loading into databse"""

    def __init__(self):
        self.goodDataPath="Prediction_Raw_Files_Validated/Good_Raw"
        self.logger=App_Logger()

    def AddQuotesToStringValuesInColumn(self):
        """This method is used to add quotes to string values in columns"""
        log_file = open('Prediction_Logs/DataTransformationLog.txt', 'a+')
        try:
            onlyfiles=[f for f in listdir(self.goodDataPath)]
            for file in onlyfiles:
                data=pandas.read_csv(self.goodDataPath+"/"+file)
                data['Date']=data['Date'].apply(lambda x:"'"+str(x)+"'")
                data.to_csv(self.goodDataPath+"/"+file,index=None,header=True)
                self.logger.log(log_file,'%s: Quotes added to string column successfully'%file)
        except Exception as e:
            self.logger.log(log_file,'Data transformation failed because %s'%e)
            raise e
        finally:
            log_file.close()