from pewma import Pewma
from static_control_limits import StaticControlLimits
from dbmanager import DatabaseManager
from flask import Flask, request, jsonify
import pandas as pd
from timeseries import TimeSeries
from utils import Similarity
import inspect

app = Flask(__name__)

database_info = {
    "url": "localhost",
    "port": "27017",
    "database": "Sibyl"
}
label = "analyzer"
db_manager = DatabaseManager(database_info, label)

global pewma
global cl
global ts
cl = StaticControlLimits()
pewma_model = Pewma()
ts = TimeSeries()


@app.route('/static_control_limits', methods=['POST'])
def static_control_limits():
    """ Function used to check if data is above set threshold

        Args:
            data (dict): the raw data

        Returns:
            dict
    """
    try:
        content = request.get_json()
        try:
            data = content["data"]
        except:
            data = content
        result = cl.lambda_handler(data)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route('/configure_pewma', methods=['POST'])
def configure_pewma():
    content = request.get_json()
    # get params from config and assign to variables
    T = content["T"]
    alpha_0 = content["alpha_0"]
    beta = content["beta"]
    threshold = content["threshold"]
    data_cols = [content["data_cols"]]
    key_param = content["key_param"]
    length_limit = content["length_limit"]
    pewma = Pewma(T, alpha_0, beta, threshold, data_cols, key_param, length_limit)


@app.route('/pewma', methods=['POST'])
def pewma():
    """ Function used to compute the probability of an event being anomalous given the moving weighted exp avg and std.
            (returns 1 or 0 based on threshold probability)

            Args:
                data (dict) : received data
                param (dict): parametrization of the function

            Yields:
                pandas.DataFrame
        """
    try:
        content = request.get_json()
        try:
            data = content["data"]
        except:
            data = content
        result = pewma_model.lambda_handler(data)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route('/dynamic_control_limits', methods=['POST'])
def dynamic_control_limits():
    """ Function that computes control limits, training and runtime

        Args:
            data (dict): dataframe containing data to analyze
            param (dict): is a dictionary containing all the parameter used by this method
                ``"remove_outliers_train"``: number used in order to compute percentile on data
                used for training\n
                ``"remove_outliers_run"``: number used in order to compute percentile on data
                used for runtime\n
                ``"training_length"``: minimum number of values to wait before compute control limits\n
                ``"runtime_window"``: minimum number of values to wait before apply the control limits

        Yields:
            pandas.DataFrame

    """
    try:
        content = request.get_json()
        data = content["data"]
        for elm in data:
            data[elm] = [data[elm]]
        data = pd.DataFrame.from_dict(content["data"])
        fname = inspect.stack()[0][3]
        param = content["param"][fname]
        result = pd.DataFrame()
        remove_outliers_train = param["remove_outliers_train"]
        remove_outliers_run = param["remove_outliers_run"]
        training_length = param["training_length"]
        runtime_window = param["runtime_window"]
        param_db = dict()
        param_db["IdMachine"] = data["IdMachine"].unique()[0]
        param_db["IdSig"] = data["IdSig"].unique()[0]
        # define parameter in order to ge control limits from database
        # get control limits if already computed
        df_param = db_manager.get_param(param_db)
        # get data stored in the database
        df_data = db_manager.get_data(param_db)
        # merge old data and new data
        df_data = pd.concat([df_data, data], ignore_index=True, sort=False)
        # get identifier os plit if present
        # check if control limits already computed

        if len(df_param) > 0:
            # check if there is enough data for application of control limits
            # print("len(df_data)="+str(len(df_data))+" runtime_window="+str(runtime_window) )
            if len(df_data) >= runtime_window:
                # sort data by timestamp
                df_data.sort_values(by=["TimeStamp"], inplace=True, ascending=True)
                df_data.reset_index(drop=True, inplace=True)
                result = ts.run_cl_analysis(
                    ts, df_data, df_param, param
                )
                # add in the result the sendout parameters, so we don't need to publish these values
                # db_manager.delete_data(param_db)
        else:
            # check if there is enough data for computation of control limits
            # print("len(df_data)="+str(len(df_data))+" training_length="+str(training_length) )
            if len(df_data) >= training_length:
                # sort data by timestamp
                df_data.sort_values(by=["TimeStamp"], inplace=True, ascending=True)
                df_data.reset_index(drop=True, inplace=True)
                result = ts.calculate_cl(df_data, remove_outliers_train)
                # store control limits
                db_manager.store_param(result, param_db)
                # delete old data
                # db_manager.delete_data(param_db)
                # we want to publish these values out from sibyl
                result["content"] = "cl"
            else:
                db_manager.store_data(data, param_db)
        if len(result) > 0:
            result["IdMachine"] = param_db["IdMachine"]
            result["IdSig"] = param_db["IdSig"]
            result["TimeStamp"] = df_data.tail(1)["TimeStamp"].values[0]
            # result["Source"] = df_data["Source"].unique()[0]
            data_to_send = {}
            print(data_to_send)
            data_to_send["IdSig"] = param_db["IdSig"]
            print(data_to_send)
            data_to_send["IdMachine"] = param_db["IdMachine"]
            print(data_to_send)
            data_to_send["TimeStamp"] = df_data.tail(1)["TimeStamp"].values[0]
            print(data_to_send)
            data_to_send["Value"] = content["data"]["Value"][0]
            print(data_to_send)
            Kind = result["Kind"]
            Value = result["Value"]
            data_to_send["dynamic_control_limits"] = {}
            for k, v in zip(Kind, Value):
                data_to_send["dynamic_control_limits"][k] = v
            print(data_to_send)
            print(jsonify(data_to_send))
        return jsonify(data_to_send)
    except Exception as e:
        return jsonify({"error": str(e)})


@app.route('/cusum', methods=['POST'])
def cusum():
    """ Function used to compute the trend of a signal

        Args:
            data (dict): received data
            param (dict): parametrization of the function:
                ``"remove_outliers"``: number used in order to compute percentile on data\n
                ``"training_length"``: minimum number of values to wait before compute cusum parameters

        Yields:
            pandas.DataFrame
    """
    try:
        content = request.get_json()
        data = content["data"]
        for elm in data:
            data[elm] = [data[elm]]
        data = pd.DataFrame.from_dict(content["data"])
        fname = inspect.stack()[0][3]
        param = content["param"][fname]
        result = pd.DataFrame()
        remove_outliers = param["remove_outliers"]
        training_length = param["training_length"]
        param_db = dict()
        param_db["IdMachine"] = data["IdMachine"].unique()[0]
        # get cusum threshold if alredy computed
        df_param = db_manager.get_param(param_db)
        # get data stored in database
        df_data = db_manager.get_data(param_db)
        # merge old and new data
        df_data = pd.concat([df_data, data], ignore_index=True, sort=False)
        # check if cusum threshodl already computed
        if df_param.shape[0] > 0:
            # sot values by timestamp
            df_data.sort_values(by=["TimeStamp"], inplace=True, ascending=True)
            df_data.reset_index(drop=True, inplace=True)
            result = TimeSeries.run_cusum(df_data, df_param)
            # delete param in order to delete old values for sp and sn
            db_manager.delete_param(param_db)
            db_manager.store_param(result, param_db)
            db_manager.delete_data(param_db)
            # we don't want to publish these values out from sibyl
        else:
            # check if there is enough data for initialization
            if df_data.shape[0] >= training_length:
                # sort values by timestamp
                df_data.sort_values(by=["TimeStamp"], inplace=True, ascending=True)
                df_data.reset_index(drop=True, inplace=True)
                result = TimeSeries.define_cusum(t_s, df_data, remove_outliers)
                # we want to publish these values out from sibyl# we want to publish these values out from sibyl
                result["content"] = "cusum"
                # define sp and sn initialization in order to store in database
                cusum = pd.DataFrame(
                    [["SP", 0], ["SN", 0]], columns=["Kind", "Value"]
                )
                cusum["IdMachine"] = param_db["IdMachine"]
                cusum["Source"] = data["Source"].unique()[0]
                cusum["IdSig"] = data["IdSig"].unique()[0]
                cusum = pd.concat(
                    [cusum, result], axis=0, ignore_index=True, sort=False
                )
                # store sp and sn values
                db_manager.store_param(cusum, param_db)
                db_manager.delete_data(param_db)
            else:
                db_manager.store_data(data, param_db)
        if result.shape[0] > 0:
            result["IdMachine"] = param_db["IdMachine"]
            result["IdSig"] = data["IdSig"].unique()[0]
            result["TimeStamp"] = df_data.tail(1)["TimeStamp"].values[0]
            result["Source"] = data["Source"].unique()[0]
        return jsonify(result.to_json())
    except:
        raise


def time_similarity(data, param):
    """ Function used to compute the similarity between 2 signals

        Args:
            data (dict): received data
            param (dict): parametrization of the function

        Yields:
            pandas.DataFrame
    """
    try:
        content = request.get_json()
        data = pd.read_json(content["data"])
        param = content["param"]
        result = pd.DataFrame()
        param_db = dict()
        param_db["IdMachine"] = data["IdMachine"].unique()[0]
        param_db["IdSig"] = data["IdSig"].unique()[0]
        param_db["IdLayer"] = param["IdLayer"]
        param_db["IdSplit"] = data["IdSplit"].unique()[0]
        # get data stored in the database
        signature = db_manager.get_data(param_db)
        db_manager.delete_data(param_db)
        # check if signature is already present
        if signature.shape[0] > 0:
            runtime = data
            # check if new data are different from the previuos ones
            result = Similarity.time_distance(signature, runtime)
            # we don't want to publish these values out from sibyl
        db_manager.store_data(data, param_db)
        return result.to_json()
    except:
        raise

## for testing and development purposes only - use production server otherwise
# if __name__ == '__main__':
#     app.run(host= '0.0.0.0')
