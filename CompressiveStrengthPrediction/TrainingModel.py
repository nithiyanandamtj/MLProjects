from sklearn.model_selection import train_test_split
from Data_Ingestion import Data_Loader
from Data_Preprocessing import Preprocessing,Clustering
from Best_Model_Finder import Tuner
from File_Operations import File_Methods
from Application_Logging import logger

class TrainModel:

    def __init__(self):
        self.log_writer=logger.App_Logger()
        self.file_object=open("Training_Logs/ModelTrainingLog.txt",'a+')

    def TrainingModel(self):
        self.log_writer.log(self.file_object,'Start of Training')
        try:
            data_getter=Data_Loader.Data_Getter(self.file_object,self.log_writer)
            data=data_getter.Get_Data()


            preprocessor=Preprocessing.Preprocessor(self.file_object,self.log_writer)

            isnull_present,cols_with_missing_values=preprocessor.Is_Null_Present(data)

            if(isnull_present):
                data=preprocessor.Impute_Missing_Values(data)
            X, Y =preprocessor.Separate_Label_Feature(data,label_column_name='Concrete_Compressive_Strength')
            x=preprocessor.LogTransformation(X)

            kMeans=Clustering.KMeansClustering(self.file_object,self.log_writer)
            number_of_clusters=kMeans.Elbow_Plot(X)

            X=kMeans.Create_Clusters(X,number_of_clusters)

            X['Labels']=Y

            list_of_clusters=X['Cluster'].unique()

            for i in list_of_clusters:
                cluster_data=X[X['Cluster']==i]

                cluster_features=cluster_data.drop(['Labels','Cluster'],axis=1)
                cluster_label=cluster_data['Labels']

                x_train,x_test,y_train,y_test=train_test_split(cluster_features,cluster_label,test_size=0.33,random_state=42)

                x_train_scaled=preprocessor.StandardScalingData(x_train)
                x_test_sclaed=preprocessor.StandardScalingData(x_test)

                model_finder=Tuner.Model_Finder(self.file_object,self.log_writer)

                best_model_name,best_model=model_finder.Get_Best_Model(x_train_scaled,y_train,x_test_sclaed,y_test)

                file_op=File_Methods.File_Operation(self.file_object,self.log_writer)
                save_model=file_op.Save_Model(best_model,best_model_name+str(i))

            self.log_writer.log(self.file_object,'Successful end of training')


        except Exception as e:
            self.log_writer.log(self.file_object,'Unsuccessful end of training')
            raise e
        
        finally:
            self.file_object.close()

