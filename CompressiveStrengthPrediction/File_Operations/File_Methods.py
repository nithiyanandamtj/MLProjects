import pickle
import os
import shutil

class File_Operation:
    """
    This class is used to save the model after training and load the saved model for prediction
    """
    def __init__(self,file_object,logger_object):
        self.file_object=file_object
        self.logger_object=logger_object
        self.model_directory='Models/'

    def Save_Model(self,model,filename):
        """
        This method is used to save the model file to directory
        """
        self.logger_object.log(self.file_object,'Entered the save model method of file operation class')
        try:
            path=os.path.join(self.model_directory,filename)
            if os.path.isdir(path):
                shutil.rmtree(path)
                os.makedirs(path)
            else:
                os.makedirs(path)
            with open(path+'/'+filename+'.sav','wb') as f:
                pickle.dump(model,f)
            self.logger_object.log(self.file_object,'Model File'+filename+' saved. Exited the save mode method of file operation class')
            return 'Success'
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in save mode method %s'%e)
            self.logger_object.log(self.file_object, 'Model file'+filename+' could not be saved')
            raise e

    def Load_Model(self,filename):
        """This model is used to load the model from memory"""
        self.logger_object.log(self.file_object,'Entered load model method of file operation class')
        try:
            with open(self.model_directory+filename+'/'+filename+'.sav','rb') as f:
                self.logger_object.log(self.file_object,'Model file'+filename+' loaded')
                return pickle.load(f)
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in load model method of file operation class %s'%e)
            raise e

    def Find_Correct_Model_File(self,cluster_number):
        """This method is used to select the correct model based on cluster number"""
        self.logger_object.log(self.file_object,'Entered Find Correct Model File of File operations class')
        try:
            self.cluster_number=cluster_number
            self.folder_name=self.model_directory
            self.list_of_model_files=[]
            self.list_of_files=os.listdir(self.folder_name)
            for self.file in self.list_of_files:
                try:
                    if(self.file.index(str(self.cluster_number))!=-1):
                        self.model_name=self.file
                except:
                    continue
            self.model_name=self.model_name.split('.')[0]
            self.logger_object.log(self.file_object,'Exited the Find correct model file method in file operation class')
            return self.model_name
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in find correct model file method in file operation class %s'%e)
            raise e



