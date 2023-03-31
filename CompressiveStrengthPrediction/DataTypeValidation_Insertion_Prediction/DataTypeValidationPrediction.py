import shutil
import sqlite3
import tarfile
from os import listdir
import os
import csv
from Application_Logging.logger import App_Logger
import pandas
class DBOperation:
    """This class will be used for handling all DB operations"""
    def __init__(self):
        self.path='Prediction_Database/'
        self.badFilePath="Prediction_Raw_Files_Validated/Bad_Raw"
        self.goodFilePath="Prediction_Raw_Files_Validated/Good_Raw"
        self.logger=App_Logger()

    def DataBaseConnection(self,DatabaseName):
        """
        This method craetes the database with the given database name and if the database exists then opens the connection with DB
        """
        file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
        try:
            conn=sqlite3.connect(self.path+DatabaseName+'.db')
            self.logger.log(file,'Opened %s database successfully'%DatabaseName)
        except ConnectionError:
            self.logger.log(file,'Error while connecting to database %s'%ConnectionError)
            raise ConnectionError
        finally:
            file.close()
        return conn

    def CreateTableDB(self,DatabaseName,column_names):
        try:
            conn=self.DataBaseConnection(DatabaseName)
            conn.execute('Drop table if exists Good_Raw_Data;')

            for key in column_names.keys():
                type=column_names[key]

                try:
                    conn.execute('Alter table Good_Raw_Data ADD column "{column_name}" {dataType}'.format(column_name=key,dataType=type))
                except:
                    conn.execute("Create table Good_Raw_Data ({column_name} {dataType})".format(column_name=key,dataType=type))

                    #conn.close()
            file=open("Prediction_Logs/DatabaseTableCreateLog.txt",'a+')
            self.logger.log(file,'Table Created Successfully')
            file.close()

        except Exception as e:
            file = open("Prediction_Logs/DatabaseTableCreateLog.txt", 'a+')
            self.logger.log(file,'Error while creating table %s'%e)
            file = open("Prediction_Logs/DatabaseConnectionLog.txt", 'a+')
            self.logger.log(file, 'Database connection closed successfully %s' % DatabaseName)
            file.close()
            raise e
        finally:
            conn.close()

    def InsertIntoTableGoodData(self,Database):
        """This method inserts good data files from the good raw folder into the above created table"""
        conn=self.DataBaseConnection(Database)
        goodFilePath=self.goodFilePath
        badFilePath=self.badFilePath
        onlyfiles=[f for f in listdir(goodFilePath)]
        log_file=open('Prediction_Logs/DBInsertLog.txt','a+')

        for file in onlyfiles:
            try:
                with open(goodFilePath+'/'+file,"r") as f:
                    next(f)
                    reader=csv.reader(f,delimiter='\n')
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('Insert into Good_Raw_Data values ({values})'.format(values=(list_)))
                                self.logger.log(log_file,'File loaded successfully %s' %file)
                                conn.commit()
                            except Exception as e:
                                self.logger.log(log_file, 'File not loaded %s' % e)
                                raise e
            except Exception as e:
                conn.rollback()
                self.logger.log(log_file,"Error while creating table %s"%e)
                shutil.move(goodFilePath+'/'+file,badFilePath)
                self.logger.log(log_file,"File moved successfully %s"%file)
                log_file.close()
                conn.close()
                raise e
        conn.close()
        log_file.close()

    def SelectingDataFromTableIntoCSV(self,Database):
        """This method is used to export data from database to CSV for perdicting"""
        self.FileFromDB='Prediction_FileFromDB/'
        self.FileName='InputFile.csv'
        log_file=open('Prediction_Logs/ExportToCSV.txt','a+')
        try:
            conn=self.DataBaseConnection(Database)
            sql="Select * from Good_Raw_Data"
            cursor=conn.cursor()

            cursor.execute(sql)
            results=cursor.fetchall()

            #Get the headers of the CSV file
            headers=[i[0] for i in cursor.description]

            #Make the CSV output directory
            if not os.path.isdir(self.FileFromDB):
                os.makedirs(self.FileFromDB)

            #Open CSV file for writing
            csvFile=csv.writer(open(self.FileFromDB+self.FileName,'w',newline=''),delimiter=',',lineterminator='\r\n',quoting=csv.QUOTE_ALL,escapechar='\\')

            #Add the headers and data to CSV file
            csvFile.writerow(headers)
            csvFile.writerows(results)

            self.logger.log(log_file,'File Exported Successfully')

        except Exception as e:
            self.logger.log(log_file,"File Exporting Failed. Error %s"%e)
            raise  e




