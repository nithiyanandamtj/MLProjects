from wsgiref import simple_server
from flask import Flask, request, render_template, Response
import os
from flask_cors import  CORS, cross_origin
import flask_monitoringdashboard as dashboard
from Prediction_Validation_Insertion import Pred_Validation
from TrainingModel import TrainModel
from Training_Validataion_Insertion import Train_Validation
from PredictFromModel import Prediction

os.putenv('LANG','en_US.UTF-8')
os.putenv('LC_ALL','en_US.UTF-8')

#Create a flask app

app=Flask(__name__)

#Object for monitoring the app
dashboard.bind(app)

#Establishing secure connection between server and app
CORS(app)

@app.route("/",methods=['GET'])
@cross_origin()

def home():
    return render_template('index.html')

@app.route('/predict',methods=['POST'])
@cross_origin()


def PredictRouteClient():

    try:
        if request.json is not None:
            path=request.json['filepath']
            pred_val = Pred_Validation(path)
            pred_val.Prediction_Validation()
            pred = Prediction(path)
            path = pred.PredictFromModel()
            return Response('Prediction File Created at %s' % path)
    except ValueError:
        return Response("Error Occured %s"%ValueError)
    except KeyError:
        return Response("Error Occured %s" % KeyError)
    except Exception as e:
        return Response("Error Occured %s"%e)


@app.route('/predict_web',methods=['POST'])
@cross_origin()


def PredictRouteClientWeb():

    try:
        if request.form.get('PredictionPath') is not None:
            path=request.form.get('PredictionPath')
            pred_val = Pred_Validation(path)
            pred_val.Prediction_Validation()
            pred = Prediction(path)
            path = pred.PredictFromModel()
            return Response('Prediction File Created at %s' % path)
    except ValueError:
        return Response("Error Occured %s"%ValueError)
    except KeyError:
        return Response("Error Occured %s" % KeyError)
    except Exception as e:
        return Response("Error Occured %s"%e)




@app.route('/train',methods=['POST'])
@cross_origin()
def TrainRouteClient():

    try:
        if request.json['folderPath'] is not None:
            path=request.json['folderPath']
            train_valobj=Train_Validation(path)
            train_valobj.train_validation()

            trainModelobj=TrainModel()
            trainModelobj.TrainingModel()
    except ValueError:
        return Response('Error Occured %s'%ValueError)
    except KeyError:
        return Response('Error Occured %s'%KeyError)
    except Exception as e:
        return Response("Error Occured %s"%e)
    return Response("Training Successful")

port= int(os.getenv("PORT",5001))
if __name__=="__main__":
    host='0.0.0.0'
    httpd=simple_server.make_server(host,port,app)
    print("Serving on %s %d" %(host,port))
    httpd.serve_forever()




