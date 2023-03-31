from os import listdir
from Application_Logging.logger import  App_Logger
import pandas as pd

class DataTransform:
    """
    This class will be used to transform Good_Raw_Data before loading to the database
    """

    def __init__(self):
        self.goodDataPath="Training_Raw_Files_Validated/Good_Raw"
        self.logger=App_Logger()

    def AddQuotesToStringValuesInColumn(self):
        """
        This method appends all the columns with string datatype within the quotes
        This is done to avoid the error while inserting the data in database
        """
        logfile=open("Training_Logs/AddQuotesToStringValuesInColumn.txt","a+")
        try:
            onlyfiles=[f for f in listdir(self.goodDataPath)]
            for file in onlyfiles:
                data=pd.read_csv(self.goodDataPath+"/"+file)
                data['DATE']=data['DATE'].apply(lambda x: "'"+str(x)+"'")
                data.to_csv(self.goodDataPath+"/"+file,index=None,header=True)
                self.logger.log(logfile,"%s Quote Added Successfully"%file)
        except Exception as e:
            self.logger.log(logfile,"Data_Preprocessing Transformation Failed %s"%e)

        logfile.close()
