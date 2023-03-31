from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import GridSearchCV
from sklearn.metrics import r2_score

class Model_Finder:
    """
    This class will be used to find the model with best accuracy and AUC score
    """

    def __init__(self,file_object,logger_object):
        self.file_object=file_object
        self.logger_object=logger_object
        self.Linearreg=LinearRegression()
        self.RandomForestReg=RandomForestRegressor()

    def Get_Best_Params_For_Random_Forest_Regressor(self,train_x,train_y):
        """
        This method is used to get the best parameters to get higher accuracy model for Random Forest Regression
        """
        self.logger_object.log(self.file_object,'Entered the Random Forest method of model finder class')
        try:
            self.param_grid_Random_Forest_Tree ={
                "n_estimators":[10,20,30],
                "max_features":["auto","sqrt","log2"],
                "min_samples_split":[2,4,8],
                "bootstrap":[True,False]
            }

            self.grid=GridSearchCV(self.RandomForestReg,self.param_grid_Random_Forest_Tree,verbose=3,cv=5)
            self.grid.fit(train_x,train_y)

            #Extract the best parameters
            self.n_estimators=self.grid.best_params_['n_estimators']
            self.max_features=self.grid.best_params_['max_features']
            self.min_samples_split=self.grid.best_params_['min_samples_split']
            self.bootstrap=self.grid.best_params_['bootstrap']

            #Create a new model with best parameters
            self.DecisionTreeReg=RandomForestRegressor(n_estimators=self.n_estimators,max_features=self.max_features,min_samples_split=self.min_samples_split,bootstrap=self.bootstrap)

            self.DecisionTreeReg.fit(train_x,train_y)
            self.logger_object.log(self.file_object,'Random Forest Regress Best Params' + str(self.grid.best_params_) + ' Exited Random Forest method from Model finder class')
            return self.DecisionTreeReg
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in Random forest method. %s'%e)
            raise e

    def Get_Best_Params_For_LinearReg(self,train_x,train_y):
        """
         This method is used to get the best parameters to get higher accuracy model for Linear Regression
        """
        self.logger_object.log(self.file_object,'Entered the Linear Regression method of model finder class')
        try:
            self.param_grid_Linear_Regression={
                'fit_intercept':[True,False],
               # 'normalize':[True,False],
                'copy_X':[True,False]
            }
            self.grid=GridSearchCV(self.Linearreg,self.param_grid_Linear_Regression,verbose=3,cv=5)
            self.grid.fit(train_x,train_y)

            #Extract the best parameters
            self.fit_intercept=self.grid.best_params_['fit_intercept']
            #self.normalize=self.grid.best_params_['normalize']
            self.copy_X=self.grid.best_params_['copy_X']

            #Create a new model with best params
            self.linreg=LinearRegression(fit_intercept=self.fit_intercept,copy_X=self.copy_X)

            self.linreg.fit(train_x,train_y)
            self.logger_object.log(self.file_object,'Linear Regression best parameters ' +str(self.grid.best_params_) +' .Exited Linear Regression Method of Model finder class')

            return self.linreg
        except Exception as e:
            self.logger_object.log(self.file_object, 'Exception occured in Linear Regression method. %s' % e)
            raise e

    def Get_Best_Model(self,train_x,train_y,test_x,test_y):
        """This method is used to find the best model between Random Forest and Linear Regression"""
        self.logger_object.log(self.file_object,'Entered Get best model method to find the best model between random forest regression and linear regression')
        try:
            self.Linearreg=self.Get_Best_Params_For_LinearReg(train_x,train_y)
            self.Prediction_LinearRegression=self.Linearreg.predict(test_x)
            self.LinearReg_Error=r2_score(test_y,self.Prediction_LinearRegression)

            self.randomforestreg=self.Get_Best_Params_For_Random_Forest_Regressor(train_x,train_y)
            self.Prediction_RandomForestRegression=self.randomforestreg.predict(test_x)
            self.RandomForestReg_Error=r2_score(test_y,self.Prediction_RandomForestRegression)

            #Comparing both the models to check the best model

            if(self.LinearReg_Error<self.RandomForestReg_Error):
                return 'RandomForestRegressor',self.randomforestreg
            else:
                return  'LinearRegression',self.Linearreg
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception Occured in get best model method of the model finder class %s'%e)
            raise e