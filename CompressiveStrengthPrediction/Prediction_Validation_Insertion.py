from Prediction_Raw_Data_Validation.PredictionRawDataValidation import  Prediction_Data_Validation
from DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import DBOperation
from DataTransformation_Prediction.DataTransformationPrediction import DataTransformPredict
from Application_Logging import logger

class Pred_Validation:
    def __init__(self,path):
        self.raw_data=Prediction_Data_Validation(path)
        self.dataTransform=DataTransformPredict()
        self.dBOpertaion=DBOperation()
        self.file_object=open("Prediction_Logs/Prediction_Main_Log.txt",'a+')
        self.log_writer=logger.App_Logger()

    def Prediction_Validation(self):
        try:
            self.log_writer.log(self.file_object,'Start of Validation on files for Prediction')
            ##Extractng values from Prediction Schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns =self.raw_data.ValuesFromSchema()
            regex=self.raw_data.ManualRegexCreation()
            self.raw_data.ValidationFileNameRaw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)
            self.raw_data.ValidateColumnLength(noofcolumns)
            self.raw_data.ValidateMissingValuesInWholeColumn()
            self.log_writer.log(self.file_object,"Raw Data Validaton Complete")

            self.log_writer.log(self.file_object, "Creating Prediction Database and tables on the basis of given schema")
            self.dBOpertaion.CreateTableDB('Prediction',column_names)
            self.log_writer.log(self.file_object, "Table Creation Completed")
            self.log_writer.log(self.file_object, "Insertion of data into table started")
            self.dBOpertaion.InsertIntoTableGoodData('Prediction')
            self.log_writer.log(self.file_object, "Insertion in table is completed")
            self.log_writer.log(self.file_object, "Deleting good data folder")
            self.raw_data.DeleteExistingGoodDataPredictionFolder()
            self.log_writer.log(self.file_object, "Good data folder deleted")
            self.log_writer.log(self.file_object, "Moving bad files to archive and deleting bad data folder")
            self.raw_data.MoveBadFilesToArchiveBad()
            self.log_writer.log(self.file_object, "Bad files moved to archive. Bad folder deleted")
            self.log_writer.log(self.file_object, "Validation operation completed")
            self.log_writer.log(self.file_object, "Extracting CSV file from table")
            self.dBOpertaion.SelectingDataFromTableIntoCSV('Prediction')
            self.log_writer.log(self.file_object, "Extracting CSV file from table")
            self.log_writer.log(self.file_object,'End of Validation on files for Prediction')
            self.file_object.close()

        except Exception as e:
            raise e