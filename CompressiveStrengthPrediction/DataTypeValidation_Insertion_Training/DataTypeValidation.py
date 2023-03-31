import shutil
import sqlite3
from datetime import datetime
from os import listdir
import os
import csv
from Application_Logging.logger import App_Logger

class DBOperation:
    #DB operations

    def __init__(self):
        self.path='Training_Database/'
        self.badFilePath='Training_Raw_Files_Validated/Bad_Raw'
        self.goodFilePath = 'Training_Raw_Files_Validated/Good_Raw'
        self.logger=App_Logger()

    def DataBaseConnection(self,DatabaseName):
        """
        This methods creates a database if its not existing, else if the database exists then opens the connection to DB
        """

        try:
            conn=sqlite3.connect(self.path+DatabaseName+'.db')

            file=open('Training_Logs/DatabaseConnectionLog.txt','a+')
            self.logger.log(file,"Opened Database Successfully %s" % DatabaseName)
            file.close()
        except ConnectionError:
            file=open('Training_Logs/DatabaseConnectionLog.txt','a+')
            self.logger.log(file, "Error While Connection to Database %s" % DatabaseName)
            file.close()
            raise ConnectionError
        return conn


    def CreateTableDB(self,DatabaseName,column_names):
        """
        This method is used to create a table in the database which is used to insert good data after validation.
        """

        file = open('Training_Logs/DatabaseTableCreateLog.txt', 'a+')
        self.logger.log(file, "Entered CreateTableDB method")
        file.close()
        try:
            conn=self.DataBaseConnection(DatabaseName)
            c=conn.cursor()
            c.execute("select count(name) from sqlite_master where type='table' and name='Good_Raw_Data'")
            if c.fetchone()[0]==1:
                conn.close()
                file=open('Training_Logs/DatabaseTableCreateLog.txt','a+')
                self.logger.log(file,"Tables Available, No need to create new table")
                file.close()

                file = open('Training_Logs/DatabaseConnectionLog.txt', 'a+')
                self.logger.log(file, "Closed %s database Successfully"%DatabaseName)
                file.close()

            else:
                file = open('Training_Logs/DatabaseTableCreateLog.txt', 'a+')
                self.logger.log(file, "No Tables Available, Need to create new table")
                file.close()
                for key in column_names.keys():
                    type=column_names[key]

                    try:
                        conn.execute('ALTER Table Good_Raw_Data Add Column "{column_name}" {datatype}'.format(column_name=key,datatype=type))

                    except:
                        conn.execute('CREATE table Good_Raw_Data ({column_name} {datatype})'.format(column_name=key,datatype=type))

                conn.close()

                file=open('Training_Logs/DatabaseTableCreateLog.txt','a+')
                self.logger.log(file,"Table created successfully")
                file.close()

                file=open('Training_Logs/DatabaseConnectionLog.txt','a+')
                self.logger.log(file,"Database %s Connection Closed Successfully" %DatabaseName )
                file.close()

        except Exception as e:
            file=open('Training_Logs/DatabaseTableCreateLog.txt','a+')
            self.logger.log(file,"Error while creating table %s" %e)
            file.close()
            file=open('Training_Logs/DatabaseConnectionLog.txt','a+')
            self.logger.log(file,"Database %s connection closed successfully" %DatabaseName)
            file.close()
            raise e


    def InsertIntoTableGoodData(self,Database):
        """
        This methods inserts good data into table
        """
        conn=self.DataBaseConnection(Database)
        goodFilePath=self.goodFilePath
        badFilePath=self.badFilePath
        onlyFiles=[f for f in listdir(goodFilePath)]
        log_file=open("Training_Logs/DbInsertLog.txt","a+")

        for file in onlyFiles:
            try:
                with open(goodFilePath+'/'+file,"r") as f:
                    next(f)
                    reader=csv.reader(f,delimiter="\n")
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('INSERT into Good_Raw_Data values ({values})'.format(values=(list_)))
                                self.logger.log(log_file,"%s: File loaded Successfully!!" %file)
                                conn.commit()
                            except Exception as e:
                                raise e
            except Exception as e:

                conn.rollback()
                self.logger.log(log_file,"Error while creating table: %s" %e)
                shutil.move(goodFilePath+'/'+file,badFilePath)
                self.logger.log(log_file, "File moved successfully: %s" % file)
                log_file.close()
                conn.close()
        conn.close()
        self.logger.log(log_file, "InsertIntoTableGoodData Completed!!" )
        log_file.close()

    def SelectingDataFromTableIntoCSV(self,Database):
        """
        This methods exports data into gooddata table as a CSV file
        """
        self.fileFromDB="Training_FileFromDB/"
        self.fileName='InputFile.csv'
        #log_file=open("Training_Logs/ExportToCSV.txt",'a+')
        log_file=open("Training_Logs/NewFile.txt",'a+')
        try:
            conn=self.DataBaseConnection(Database)
            sqlSelect="SELECT * from Good_Raw_Data"
            cursor=conn.cursor()

            cursor.execute(sqlSelect)

            results=cursor.fetchall()

            header=[i[0] for i in cursor.description]
            if not os.path.isdir(self.fileFromDB):
                os.makedirs(self.fileFromDB)

            csvFile=csv.writer(open(self.fileFromDB+self.fileName,'w',newline=""),delimiter=",", lineterminator="\r\n",quoting=csv.QUOTE_ALL,escapechar='\\')

            csvFile.writerow(header)
            csvFile.writerows(results)

            self.logger.log(log_file,"File Exported Successfully")
            log_file.close()
        except Exception as e:
            self.logger.log(log_file,"File Exporting Failed. Error: %s" %e)
            log_file.close()
            raise e




