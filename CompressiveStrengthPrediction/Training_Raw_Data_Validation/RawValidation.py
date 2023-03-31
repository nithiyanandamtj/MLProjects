from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from Application_Logging.logger import App_Logger




class Raw_Data_Validation:
    """
    This class is used for validating the raw data
    """
    def __init__(self,path):
        self.Batch_Directory=path
        self.schema_path='schema_training.json'
        self.logger=App_Logger()

    def ValuesFromSchema(self):
        """
        Extracts all relevant information from pre-defined schema file
        """
        try:
            with open(self.schema_path, 'r') as f:
                dic=json.load(f)
                f.close()
            pattern=dic['SampleFileName']
            LengthOfDateStampInFile=dic['LengthOfDateStampInFile']
            LenghtOfTimeStampInFile=dic['LengthOfTimeStampInFile']
            column_names=dic['ColName']
            NumberOfColumns=dic['NumberOfColumns']

            file=open("Training_Logs/ValuesFromSchemaValidationLog.txt",'a+')
            message="LengthOfDateStampInFile: %s" %LengthOfDateStampInFile +"\t"+ 'LengthOfTimeStampInFile: %s'%LenghtOfTimeStampInFile +"\t" +'NumberOfColumns: %s'%NumberOfColumns + "\n"
            self.logger.log(file,message)

            file.close()

        except ValueError:
            file=open("Training_Logs/ValuesFromSchemaValidationLog.txt",'a+')
            self.logger.log(file,"ValueError: Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file=open("Training_Logs/ValuesFromSchemaValidationLog.txt",'a+')
            self.logger.log(file,"KeyError: Key Value error, incorrect key pressed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Training_Logs/ValuesFromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,str(e))
            file.close()
            raise e

        return LengthOfDateStampInFile,LenghtOfTimeStampInFile,column_names,NumberOfColumns



    def ManualRegexCreation(self):
        """
        This method is a manually defined regex based on the FileName give in Schema File. It is used to validate the filename of the training data
        """
        regex="['cement_strength']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def CreateDirectoryForGoodBadRawData(self):
        """
        This method creates directory for good and bad raw data to store after the training validation data
        """
        try:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Good_Raw_Folder_Nithi_Created")
            file.close()
            path=os.path.join("Training_Raw_Files_Validated/","Good_Raw/")
            if not os.path.isdir(path):
                    os.makedirs(path)
            path = os.path.join("Training_Raw_Files_Validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file=open("Training_Logs/GeneralLog.txt",'a+')
            self.logger.log(file,"Error while creating Directory %s" %ex)
            file.close()
            raise OSError

    def DeleteExistingGoodDataTrainingFolder(self):
        """
        This method is used to delete the existing folder after the data is loaded into database table
        This is done as a best practise for space optimization
        """
        try:
            path="Training_Raw_Files_Validated/Good_Raw/"
            if os.path.isdir(path):
                shutil.rmtree(path)
                file=open("Training_Logs/General_Log.txt","a+")
                self.logger.log(file,"Good Raw directory deleted successfully")
                file.close()
        except OSError as e:
            file=open("Training_Logs/General_Log.txt","a+")
            self.logger.log(file,"Error while good raw deleting directory %s" %e)
            file.close()
            raise OSError

    def DeleteExistingBadDataTrainingFolder(self):
        """
        This method is used to delete the existing folder after the data is loaded into database table
        This is done as a best practise for space optimization
        """
        try:
            path="Training_Raw_Files_Validated/Bad_Raw/"
            if os.path.isdir(path):
                shutil.rmtree(path)
                file=open("Training_Logs/General_Log.txt","a+")
                self.logger.log(file,"Bad Raw directory deleted successfully")
                file.close()
        except OSError as e:
            file=open("Training_Logs/General_Log.txt","a+")
            self.logger.log(file,"Error while deleting bad raw directory %s" %e)
            file.close()
            raise OSError

    def MoveBadFilesToArchiveBad(self):
        """
        This method is used to move the bad data to archived folder and delete the directory made.
        Archive bad files is sent back to the clients
        """
        now=datetime.now()
        date=now.date()
        time=now.strftime("%H%M%S")

        try:
            source='Training_Raw_Files_Validated/Bad_Raw/'
            if os.path.isdir(source):
                path="TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest='TrainingArchiveBadData/BadData_'+str(date)+'_'+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files=os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source+f,dest)
                file=open('Training_Logs/GeneralLog.txt','a+')
                self.logger.log(file,'Bad Files moved to Archive')
                path='Training_Raw_Files_Validated/'
                if os.path.isdir(path+'Bad_Raw/'):
                    shutil.rmtree(path+'Bad_Raw/')
                self.logger.log(file,"Bad Raw Data_Preprocessing Folder Deleted Successfully")
                file.close()
        except Exception as e:
            file=open('Training_Logs/GeneralLog.txt','a+')
            self.logger.log(file,"Error while moving bad files to archive %s"%e)
            file.close()
            raise e


    def ValidationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
        This methos is used to validate the input file provided. If the file name pattern doesn't match the pattern in schema, the file is moved to bad data.
        """
        self.DeleteExistingBadDataTrainingFolder()
        self.DeleteExistingGoodDataTrainingFolder()

        onlyfiles=[f for f in listdir(self.Batch_Directory)]
        try:
            #Creating new directories
            self.CreateDirectoryForGoodBadRawData()
            f=open("Training_Logs/NameValidationLog.txt","a+")
            for filename in onlyfiles:
                if(re.match(regex,filename)):
                    splitatdot=re.split('.csv',filename)
                    splitatdot=re.split('_',splitatdot[0])
                    if len(splitatdot[2])==LengthOfDateStampInFile:
                        if len(splitatdot[3]) == LengthOfTimeStampInFile:
                            shutil.copy("Training_Batch_Files/"+filename,"Training_Raw_Files_Validated/Good_Raw")
                            self.logger.log(f,"Valid file name. File moved to Good_Raw_Folder %s"%filename)
                        else:
                            shutil.copy("Training_Batch_Files/"+filename,"Training_Raw_Files_Validated/Bad_Raw")
                            self.logger.log(f,"Invalid file name. File moved to Bad_Raw Folder %s"%filename)
                    else:
                        shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(f, "Invalid file name. File moved to Bad_Raw Folder %s" % filename)
                else:
                    shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(f, "Invalid file name. File moved to Bad_Raw Folder %s" % filename)
            f.close()
        except Exception as e:
            f=open("Training_Logs/NameValidationLog.txt","a+")
            self.logger.log(f,"Error Occured while validating filename %s"%e)
            f.close()
            raise e

    def ValidateColumnLength(self, NumberOfColumns):
        """
        This function is use to validate the number of columns in the CSV file.
        The number of columns should be same as the schema file.
        If the number of columns are not equal the file is moved to Bad_Raw data folder.
        If the number of columns matches the schema file, it is moved to Good_Raw data folder
        """
        try:
            f=open("Training_Logs/ColumnValidationLog.txt","a+")
            self.logger.log(f,"Column Length Validation Started")
            for file in listdir("Training_Raw_Files_Validated/Good_Raw/"):
                csv=pd.read_csv("Training_Raw_Files_Validated/Good_Raw/"+file)
                if csv.shape[1]==NumberOfColumns:
                    pass
                else:
                    shutil.move("Training_Raw_Files_Validated/Good_Raw/"+file,"Training_Raw_Files_Validated/Bad_raw")
                    self.logger.log(f,"Invalid Column Length for the file. File moved to Bad_Raw folder %s" %file)
            self.logger.log(f, "Column Length Validation Completed")
            f.close()

        except OSError:
            lf = open("Training_Logs/ColumnValidationLog.txt", "a+")
            self.logger.log(lf, "Error occured while moving the file %s" % OSError)
            lf.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/ColumnValidationLog.txt", "a+")
            self.logger.log(f, "Error occured %s" % e)
            f.close()
            raise e

    def ValidateMissingValuesInWholeColumn(self):
        """
        This method is used to check if any of the column has missing values in all the rows, if that is the case.
        The file is not suitable for prcoessing. It should be considered as Bad_Raw data and moved to Bad_Raw folder
        """
        try:
            f=open("Training_Logs/MissingValuesInColumn.txt",'a+')
            self.logger.log(f,"Missing Values Validation Started")

            for file in listdir('Training_Raw_Files_Validated/Good_Raw/'):

                csv=pd.read_csv('Training_Raw_Files_Validated/Good_Raw/'+file)
                count=0
                for columns in csv:
                    if len(csv[columns])-csv[columns].count()==len(csv[columns]):
                        count+=1
                        shutil.move('Training_Raw_Files_Validated/Good_Raw/'+file,'Training_Raw_Files_Validated/Bad_Raw')
                        self.logger.log(f,"Invalid Column Length for the file. File moved to bad raw folder %s" %file)
                if count==0:
                    csv.rename(columns={'Unnamed: 0':'Wafer'},inplace=True)
                    csv.to_csv("Training_Raw_Files_Validated/Good_Raw/"+file,index=None,header=True)
            f.close()
        except OSError:
            f = open("Training_Logs/MissingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file %s" %OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/MissingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured %s" %e)
            f.close()
            raise e
