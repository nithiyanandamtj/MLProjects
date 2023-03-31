import pandas as pd

class Data_Getter_Pred:
    """
    This class is used to get data from source for training
    """

    def __init__(self,file_object,logger_object):
        self.prediction_file='Prediction_FileFromDB/InputFile.csv'
        self.file_object=file_object
        self.logger_object=logger_object

    def Get_Data(self):
        """
        This methods reads data from the source
        """
        self.logger_object.log(self.file_object,'Entered the Get_Data method of Data_Getter class')
        try:
            self.data=pd.read_csv(self.prediction_file)
            self.logger_object.log(self.file_object,'Data_Preprocessing loaded successfully in Get_Data method from Data_Getter class')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,'Data_Preprocessing load unsuccessful. Exited the Get_data method and Data_Getter class %s' %e)
            raise e
        