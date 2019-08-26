from pewma import Pewma
from static_control_limits import StaticControlLimits
from dbmanager import DatabaseManager
import pandas as pd
from timeseries import TimeSeries
from utils import Similarity
import inspect
import pika
import json


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


def entry_point(ch, method, properties, body):
    topic = str(method).split('routing_key=')[-1].split("'")[0]
    print(topic)
    data = json.loads(body.decode("UTF-8"))
    print(data)
    data = transform_properties(data, topic)
    print(data)
    result = static_control_limits(data)
    print(result)
    result = pewma(result)
    print(result)

def transform_properties(payload, topic):
    IdMachine = topic.split(".")[3]

    payload['data'] = {}
    payload["data"]['IdMachine'] = IdMachine
    payload["data"]['IdSig'] = payload["id"]
    payload["data"]['TimeStamp'] = payload["ts"]
    payload["data"]['Value'] = payload["v"]

    payload['param'] = {}

    payload["param"]['static_control_limits'] = {}
    payload["param"]['static_control_limits']['warningLimit'] = 250
    payload["param"]['static_control_limits']['damageLimit'] = 500

    payload["param"]['dynamic_control_limits'] = {}
    payload["param"]['dynamic_control_limits']['remove_outliers_train'] = 75
    payload["param"]['dynamic_control_limits']['remove_outliers_run'] = 75
    payload["param"]['dynamic_control_limits']['training_length'] = 1000
    payload["param"]['dynamic_control_limits']['runtime_window'] = 100
    payload["param"]['dynamic_control_limits']['type_analysis'] = "both"
    payload["param"]['dynamic_control_limits']['w1'] = 3
    payload["param"]['dynamic_control_limits']['w2'] = 3
    payload["param"]['dynamic_control_limits']['w3'] = 1
    payload["param"]['dynamic_control_limits']['ww'] = 1
    payload["param"]['dynamic_control_limits']['wc'] = 3

    payload["param"]['cusum'] = {}
    payload["param"]['cusum']['remove_outliers_train'] = 75
    payload["param"]['cusum']['remove_outliers_run'] = 75

    payload["param"]['dtw'] = {}

    payload["param"]['pewma'] = {}
    payload["param"]['pewma']['remove_outliers_train'] = 75
    payload["param"]['pewma']['remove_outliers_run'] = 75
    payload["param"]['pewma']['training_length'] = 1000
    payload["param"]['pewma']['runtime_window'] = 100
    payload["param"]['pewma']['type_analysis '] = "both"

    payload.pop("ts")
    payload.pop("id")
    payload.pop("v")

    return(payload)


def static_control_limits(content):
    """ Function used to check if data is above set threshold

        Args:
            data (dict): the raw data

        Returns:
            dict
    """
    try:
        data = content["data"]
        fname = inspect.stack()[0][3]
        param = content["param"][fname]
    except:
        data = content
    result = cl.lambda_handler(data, param)
    return result

def configure_pewma(param):
    # get params from config and assign to variables
    T = param["T"]
    alpha_0 = param["alpha_0"]
    beta = param["beta"]
    threshold = param["threshold"]
    data_cols = [param["data_cols"]]
    key_param = param["key_param"]
    length_limit = param["length_limit"]
    pewma = Pewma(T, alpha_0, beta, threshold, data_cols, key_param, length_limit)


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
        data = content["data"]
    except:
        data = content
    result = pewma_model.lambda_handler(data)
    return result
    except Exception as e:
        return {"error": str(e)}

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
            data_to_send["IdSig"] = param_db["IdSig"]
            data_to_send["IdMachine"] = param_db["IdMachine"]
            data_to_send["TimeStamp"] = df_data.tail(1)["TimeStamp"].values[0]
            data_to_send["Value"] = content["data"]["Value"][0]
            Kind = result["Kind"]
            Value = result["Value"]
            data_to_send["dynamic_control_limits"] = {}
            for k, v in zip(Kind, Value):
                data_to_send["dynamic_control_limits"][k] = v
        return data_to_send
    except Exception as e:
        return {"error": str(e)}

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
        data = content["data"]
        for elm in data:
            data[elm] = [data[elm]]
        data = pd.DataFrame.from_dict(content["data"])
        print("------- data -------")
        print(data)
        print("------- data -------")
        fname = inspect.stack()[0][3]
        param = content["param"][fname]
        print("------- param -------")
        print(param)
        print("------- param -------")
        result = pd.DataFrame()
        remove_outliers = param["remove_outliers"]
        training_length = param["training_length"]
        param_db = dict()
        param_db["IdMachine"] = data["IdMachine"].unique()[0]
        print("------- param_db -------")
        print(param_db)
        print("------- param_db -------")
        # get cusum threshold if alredy computed
        df_param = db_manager.get_param(param_db)
        print("------- df_param -------")
        print(df_param)
        print("------- df_param -------")
        # get data stored in database
        df_data = db_manager.get_data(param_db)
        print("------- df_data -------")
        print(df_data)
        print("------- df_data -------")
        # merge old and new data
        df_data = pd.concat([df_data, data], ignore_index=True, sort=False)
        # check if cusum threshold already computed
        if df_param.shape[0] > 0:
            # sot values by timestamp
            df_data.sort_values(by=["TimeStamp"], inplace=True, ascending=True)
            df_data.reset_index(drop=True, inplace=True)
            result = ts.run_cusum(df_data, df_param)
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
                result = ts.define_cusum(t_s, df_data, remove_outliers)
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
        return result.to_json()
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

host = '10.251.0.88'
credentials = pika.PlainCredentials('guest', 'guest')
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=host,
                              credentials=credentials
                              )

)
channel = connection.channel()
channel.exchange_declare(exchange='topic_logs', exchange_type='topic')
result = channel.queue_declare('', exclusive=True)
queue_name = result.method.queue
print(queue_name)
channel.queue_bind(
    exchange='amq.topic',
    queue=queue_name,
    routing_key="comau.lra.#")
channel.basic_consume(
    queue=queue_name,
    on_message_callback=entry_point,
    auto_ack=True)
print(' [*] Waiting for logs. To exit press CTRL+C')
channel.start_consuming()