import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer
from sklearn.preprocessing import StandardScaler

class Preprocessor:
    """
    This class is used to clean and transform data before training
    """

    def __init__(self,file_object,logger_object):
         self.file_object=file_object
         self.logger_object=logger_object

    def Remove_Columns(self,data,columns):
        """
        This method removes given column from panda dataframe
        """
        self.logger_object.log(self.file_object,'Entered the remove_columns method of preprocessor class')
        self.data=data
        self.columns=columns
        try:
            self.useful_data=self.data.drop(labels=self.columns,axis=1)
            self.logger_object.log(self.file_object,'Column removal successful')
            return self.useful_data
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in remove_columns method of preprocessor class')
            self.logger_object.log(self.file_object,'Column removal unsuccessful')
            raise e
        finally:
            self.logger_object.log(self.file_object, 'Exited the remove_columns method of preprocessor class')

    def Separate_Label_Feature(self,data,label_column_name):
        """
        This method seprates labels amd features into two different dataframes
        """
        self.logger_object.log(self.file_object,'Entered Separate_Label_Feature method of Preprocessor class')
        try:
            self.X=data.drop(labels=label_column_name,axis=1)
            self.Y=data[label_column_name]
            self.logger_object.log(self.file_object,'Label Seperation Successful')
            return self.X,self.Y
        except Exception as e:
            self.logger_object.log(self.file_object,'Label Seperation Unsuccessful %s'%e)
            raise e
        finally:
            self.logger_object.log(self.file_object,'Exited Separate_Label_Feature method')

    def DropUnnecessaryColumns(self,data,columnamelist):
        """
        This method drops unwanted column as discussed in EDA
        """
        data=data.drop(columnamelist,axis=1)
        return data

    def ReplaceInvalidValuesWithNull(self,data):
        """
        This method repladce invalid values with null
        """
        for column in data.columns:
            count=data[column][data[column]=='?'].count()
            if count!=0:
                data[column]=data[column].replace('?',np.nan)
        return data

    def Is_Null_Present(self,data):
        """
        Checks if there are null values present in pandas dataframe
        """
        self.logger_object.log(self.file_object,'Entered Is_Null_Present of the Preprocessor Class')
        self.null_present=False
        self.cols_with_missing_values=[]
        self.cols=data.columns
        try:
            self.null_counts=data.isna().sum()
            for i in range(len(self.null_counts)):
                if self.null_counts[i]>0:
                        self.null_present=True
                        self.cols_with_missing_values.append(self.cols[i])
            if self.null_present:
                self.dataframe_with_null=pd.DataFrame()
                self.dataframe_with_null['columns']=data.columns
                self.dataframe_with_null['missing values count']=np.asarray(data.isna().sum())
                self.dataframe_with_null.to_csv('preprocessing_data/null_values.csv')
            self.logger_object.log(self.file_object,'Finding missing values is successful. Data written to the null values file')
            return self.null_present,self.cols_with_missing_values
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception in Is_Null_Present method. %s'%e)
            self.logger_object.log(self.file_object,'Finding missing values failed')
            raise e

    def EncodeCategoricalValues(self,data):
        """
        This method is used to encode all categorical values in the training dataset
        """
        self.logger_object.log(self.file_object,'Entered EncodeCategoricalValues method in preprocessor class')
        data['class']=data['class'].map({'p':1,'e':2})
        for column in data.drop(['class'],axis=1).columns:
            data=pd.get_dummies(data,coulmns=[column])
        self.logger_object.log(self.file_object, 'Exited EncodeCategoricalValues method in preprocessor class')
        return data

    def EncodeCategoricalValuesPrediction(self,data):
        self.logger_object.log(self.file_object, 'Entered EncodeCategoricalValuesPrediction method in preprocessor class')
        for column in data.columns:
            data=pd.get_dummies(data,columns=[column])
        self.logger_object.log(self.file_object,
                               'Exited EncodeCategoricalValuesPrediction method in preprocessor class')
        return data

    def StandardScalingData(self,X):
        self.logger_object.log(self.file_object,
                               'Entered StandardScalingData method in preprocessor class')
        scalar=StandardScaler()
        X_Scaled=scalar.fit_transform(X)
        self.logger_object.log(self.file_object,
                               'Exited StandardScalingData method in preprocessor class')
        return X_Scaled

    def LogTransformation(self,X):
        self.logger_object.log(self.file_object,
                               'Entered LogTransformation method in preprocessor class')
        for column in X.columns:
            X[column] += 1
            X[column]=np.log(X[column])
        self.logger_object.log(self.file_object,
                               'Exited LogTransformation method in preprocessor class')
        return X

    def Impute_Missing_Values(self,data):
        """
        This method replaces all missing values with KNN imputer
        """
        self.logger_object.log(self.file_object,'Entered the Impute_Missing_Values method of the preprocessor class')
        self.data=data
        try:
            imputer=KNNImputer(n_neighbors=3,weights='uniform',missing_values=np.nan)
            self.new_array=imputer.fit_transform(self.data)
            self.new_data=pd.DataFrame(data=self.new_array,columns=self.data.columns)
            self.logger_object.log(self.file_object,'Inputing missing values successful')
            return self.new_data
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in Impute_Missing_Values method and Imputing failed %s'%e)
            raise e
        finally:
            self.logger_object.log(self.file_object,'Exited the Impute_Missing_Values method of the preprocessor class')

    def Get_Columns_With_Zero_Std_Deviation(self,data):
        """
        This method is used to find out columns which has a standard deviation of zero
        """
        self.logger_object.log(self.file_object,'Entered Get_Columns_With_Zero_Std_Deviation method in preprocessor class')
        self.columns=data.columns
        self.data_describe=data.describe()
        self.col_to_drop=[]

        try:
            for x in self.columns:
                if(self.data_describe[x]['std']==0):
                    self.col_to_drop.append(x)
            self.logger_object.log(self.file_object,'Column search for zero std is successful')
            return self.col_to_drop
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception in Get_Columns_With_Zero_Std_Deviation method and failed to identify std columns with error %s'%e)
            raise e
        finally:
            self.logger_object.log(self.file_object,'Exited Get_Columns_With_Zero_Std_Deviation method in preprocessor class')
