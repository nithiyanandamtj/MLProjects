import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from  kneed import  KneeLocator
from File_Operations import File_Methods

class KMeansClustering:
    """This class is used to divide the data into clusters before training"""
    def __init__(self,file_object,logger_object):
        self.file_object=file_object
        self.logger_object=logger_object

    def Elbow_Plot(self,data):
        """This methods saves the plot to decided the optimum number of clusters in the file"""
        self.logger_object.log(self.file_object,'Enterd Elbow plot method of KMeansClustering class')
        wcss=[]
        try:
            for i in range(1,11):
                kmeans=KMeans(n_clusters=i,init='k-means++',random_state=42)
                kmeans.fit(data)
                wcss.append(kmeans.inertia_)
            plt.plot(range(1,11),wcss)
            plt.title('The Elbow Method')
            plt.xlabel('Number of Clusters')
            plt.ylabel('WCSS')
            plt.savefig('Preprocessing_Data/K_Means_Elbow.PNG')
            self.kn=KneeLocator(range(1,11),wcss,curve='convex',direction='decreasing')
            self.logger_object.log(self.file_object,'The optimum number of clusters is'+str(self.kn.knee)+ ' .Exited the elbow method')
            return self.kn.knee
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in Elbow_Plot method inside KmeansClustering class %s'%e)
            raise e

    def Create_Clusters(self,data,number_of_clusters):
        """Create a dataframe consisting of the cluster information"""
        self.logger_object.log(self.file_object,"Entered the create_clusters method of KMeansClusteringClass")
        self.data=data
        try:
            self.kmeans=KMeans(n_clusters=number_of_clusters,init='k-means++',random_state=42)
            self.y_kmeans=self.kmeans.fit_predict(data)

            self.file_op=File_Methods.File_Operation(self.file_object,self.logger_object)
            self.save_model=self.file_op.Save_Model(self.kmeans,'KMeans')
            self.data['Cluster']=self.y_kmeans
            self.logger_object.log(self.file_object,'Successfully created'+str(self.kn.knee)+' clusters. Exited the create_cluster method')
            return self.data
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in Create_Cluster Method in KMeansClustering calss %s'%e)
            raise e