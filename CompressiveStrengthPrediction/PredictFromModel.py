import pandas
from File_Operations import File_Methods
from Data_Preprocessing import Preprocessing
from Data_Ingestion import Data_Loader_Prediction
from Application_Logging import logger
from Prediction_Raw_Data_Validation.PredictionRawDataValidation import Prediction_Data_Validation


class Prediction:
    def __init__(self, path):
        self.file_object = open('Prediction_Logs/Prediction_Log.txt', 'a+')
        self.log_writer = logger.App_Logger()
        self.pred_data_val = Prediction_Data_Validation(path)

    def PredictFromModel(self):
        try:
            self.pred_data_val.DeletePreictionFile()
            self.log_writer.log(self.file_object, 'Start of Prediction')
            data_getter = Data_Loader_Prediction.Data_Getter_Pred(self.file_object, self.log_writer)
            data = data_getter.Get_Data()

            preprocessor = Preprocessing.Preprocessor(self.file_object, self.log_writer)

            is_null_present, cols_with_missing_values = preprocessor.Is_Null_Present(data)

            if (is_null_present):
                data = preprocessor.Impute_Missing_Values(data)

            data = preprocessor.LogTransformation(data)

            data_scaled = pandas.DataFrame(preprocessor.StandardScalingData(data), columns=data.columns)

            file_loader = File_Methods.File_Operation(self.file_object, self.log_writer)
            kMeans = file_loader.Load_Model('KMeans')
            clusters = kMeans.predict(data_scaled)
            data_scaled['clusters'] = clusters
            clusters = data_scaled['clusters'].unique()
            result = []

            for i in clusters:
                cluster_data = data_scaled[data_scaled['clusters'] == i]
                cluster_data = cluster_data.drop(['clusters'], axis=1)
                model_name = file_loader.Find_Correct_Model_File(i)
                model = file_loader.Load_Model(model_name)

                for val in (model.predict(cluster_data.values)):
                    result.append(val)
            result = pandas.DataFrame(result, columns=['Predictions'])
            path = 'Prediction_Output_File/Predictions.csv'
            result.to_csv(path, header=True)
            self.log_writer.log(self.file_object, 'End of prediction')
        except Exception as e:
            self.log_writer.log(self.file_object, 'Error occured while running the prediction %s' % e)
            raise e
        return path
