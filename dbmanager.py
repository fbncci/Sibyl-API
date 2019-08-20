"""
This module contains one of the module of Comau sibyl.
In particular it contains the class DatabaseManager
that contains method to crete connection with different databases
"""
import pandas as pd
from pymongo import MongoClient


class DatabaseManager:
    """
    This class contains all the method that can be used to read configuration.

    Args:
        database_info (dict): dictionary with all informations useful for database connection \n
            ``"url"``: url to use for connection with database\n
            ``"port"``: port to use for connection with database\n
            ``"database"``: name of the database\n
        class_label (str): name of the sibyl module

    Attributes:

        class_label (str) : contains the value of ther argument class_label
        __table_data_name (str): name of table contains the data
        database_info (dict) : contains the informations of the argument database_info
        __engine (pymongo.MongoClient) : client connection to MongoDB
        database (pymongo.Database): is the reference to the database
        db_data (pymongo.Collection): is the reference to the collection that contains the data of the module
        db_param (pymongo.Collection): is the reference to the collection that contains the parameters of the module
    """

    def __init__(self, database_info, class_label):
        self.class_label = class_label
        self.__table_data_name = class_label + "_Data"
        self.database_info = database_info
        self.__engine = None
        self.__create_engine()
        self.clean_data_collection()

    def __create_engine(self):
        """ Function used to create the connection to the database

            Returns:
                None
        """
        self.__engine = MongoClient(
            "mongodb://"
            + self.database_info["url"]
            + ":"
            + self.database_info["port"]
            + "/"
        )
        self.database = self.__engine[self.database_info["database"]]
        self.db_data = self.database[self.class_label + "_Data"]
        self.db_param = self.database[self.class_label + "_Param"]

    def clean_data_collection(self):
        """
            Function used to delete old data for all modules that are not Analyzer

            Returns:
                None
        """
        if "analyzer" not in self.class_label:
            self.db_data.delete_many({})

    def get_param(self, param_db):
        """ Function used to get the parameters for the algorithm

            Args:
                param_db (dict): contains the mongodb query to execute

            Returns:
                pandas.DataFrame: contains the parameter that are useful for the algorithm
        """
        try:
            param = {}
            if self.db_param.count() > 0:
                for param_doc in self.db_param.find(param_db):
                    if len(param) == 0:
                        param = param_doc
                    else:
                        param = pd.concat(
                            [param, pd.DataFrame.from_dict(param_doc)],
                            ignore_index=True,
                            sort=False,
                        )
                if param.shape[0] > 0:
                    param = param.drop(["_id"], axis=1)
            return param
        except:
            raise

    def get_data(self, param_db):
        """ Function used to get the data for a given machine, signal, flow, process and source

            Args:
                param_db (dict): contains the mongodb query to execute

            Returns:
                pandas.DataFrame: contains the requested data
        """
        try:
            data = pd.DataFrame()
            if self.db_data.count() > 0:
                for data_doc in self.db_data.find(param_db):
                    if data.shape[0] == 0:
                        for elm in data_doc:
                            data_doc[elm] = [data_doc[elm]]
                        data = pd.DataFrame.from_dict(data_doc)
                    else:
                        for elm in data_doc:
                            data_doc[elm] = [data_doc[elm]]
                        data = pd.concat(
                            [data, pd.DataFrame(data_doc)],
                            ignore_index=True,
                            sort=False,
                        )
                if data.shape[0] > 0:
                    data = data.drop(["_id"], axis=1)
            return data
        except:
            raise

    def store_data(self, data, param):
        """ Function used to store data for a given machine, signal, flow, process and source

            Args:
                data (pandas.DataFrame): data to store in the database
                param (dict): contains the mongodb query to execute

            Returns:
                None
        """
        try:
            # if "IdSig" in param:
            #     data.pop("IdMachine")
            #     data.pop("IdSig")
            # if "IdMachine" in param:
            #     data.pop("IdMachine")
            # data_to_store = data
            # for key in data_to_store:
            #     data_to_store[key] = data[key]
            # data_to_store.update(param)
            # TODO do a cycle in data so the maximum storage in mongoDB is not reached
            data_to_store = data.to_dict(orient="index")[0]
            print("-----data_to_store-----")
            print(data_to_store)
            print("-----data_to_store-----")
            self.db_data.insert_one(data_to_store)
        except:
            raise

    def store_param(self, data, param):
        """ Function used to store parameters for a given machine, signal, flow, process and source

            Args:
                data (pandas.DataFrame): parameters to store in the database
                param (dict): contains the mongodb query to execute

            Returns:
                None
        """
        try:
            if "IdSig" in param:
                data.pop("IdMachine")
                data.pop("IdSig")
            if "IdMachine" in param:
                data.pop("IdMachine")
            data_to_store = data
            for key in data_to_store:
                data_to_store[key] = param[key]
            self.db_param.insert_one(data_to_store)
        except:
            raise

    def delete_data(self, param):
        """ Function used to delete data for a given machine, signal, flow, process and source

            Args:
                param (dict): contains the mongodb query to execute

            Returns:
                None
        """
        try:
            self.db_data.delete_many(param)
        except:
            raise

    def delete_param(self, param):
        """ Function used to delete data for a given machine, signal, flow, process and source

            Args:
                param (dict): contains the mongodb query to execute

            Returns:
                None
        """
        try:
            self.db_param.delete_many(param)
        except:
            raise
