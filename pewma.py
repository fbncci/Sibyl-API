# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 13:52:04 2019

@author: Jack Press
"""
import math
import copy
import pymongo
import sys
import json


class Pewma:
    def __init__(self, T=30, alpha_0=0.95, beta=0.5, threshold=0.05, data_cols=["Value"], key_param="IdSig",
                 length_limit=30, host="localhost", port="27017", dbName="Sibyl", collectionName="pewma_ifm"):
        '''
        the parameters than need to be changed for a different data set are T, alpha_0, beta, data_cols and key_param
        -T: number of points to consider in initial average
        -alpha_0: base amount of PEWMA which is made up from previous value of PEWMA
        -beta: parameter that controls how much you allow outliers to affect your MA, for standard EWMA set to 0
        -threshold: value below which a point is considered an anomaly, like a probability but not strictly a probability
        -data_cols: columns we want to take PEWMA of
        -key_param: key from event which will become the database key
        '''
        self.T = T
        self.alpha_0 = alpha_0
        self.beta = beta
        self.threshold = threshold
        self.data_cols = data_cols
        self.key_param = key_param
        self.length_limit = length_limit
        self.mongo_setup(host, port, dbName, collectionName)

    def lambda_handler(self, event):
        try:
            query = {"IdMachine": event["IdMachine"],
                     "IdSig": event["IdSig"]}
            response = self.coll.find_one(query)
            if response:
                newRecord = response
                newRecord = self.update_list_of_last_n_points(event, newRecord)
                newRecord = self.generate_pewma(newRecord, event)
            else:
                newRecord = self.initial_record(event)
                self.coll.insert_one(newRecord)  # write new record

            self.coll.replace_one(query, newRecord, True)  # write new record
            try:
                event["pewma"] = {}
                event["pewma"]["STD_Value"] = newRecord["STD_Value"]
                event["pewma"]["P_Value"] = newRecord["P_Value"]
                event["pewma"]["Value_is_Anomaly"] = newRecord["Value_is_Anomaly"]
                if event["Value_is_Anomaly"]:
                    print("Anomaly detected at " +
                          event["TimeStamp"]+" from"+event["IdSig"]+"id "+event["IdMachine"])
                return(event)
            except:
                return event
        except Exception as e:
            event["pewma"] = {"error": str(e)}
            return (event)


    def update_list_of_last_n_points(self, event, current_data):
        '''
        this function updates lists that contain length_limit # of most recent points
        '''
        new_data = current_data
        for col in event:
            if col in self.data_cols:
                append_list = current_data[col]
                append_list.append(float(event[col]))
                if len(append_list) > self.length_limit:
                    append_list = append_list[1:]
                new_data[col] = append_list
            else:
                new_data[col] = event[col]
        return new_data

    def initial_record(self, event):
        '''
        if there is no record for this id then this will generate
        the record which will be the initial record
        '''
        newRecord = copy.deepcopy(event)
        for col in event:
            if col in self.data_cols:
                newRecord[col] = [newRecord[col]]
                newRecord["alpha_" + col] = 0
                newRecord["s1_" + col] = float(event[col])
                newRecord["s2_" + col] = math.pow(float(event[col]), 2)
                newRecord["s1_next_" + col] = newRecord["s1_" + col]
                newRecord["STD_next_" + col] = \
                    math.sqrt(newRecord["s2_" + col] -
                              math.pow(newRecord["s1_" + col], 2))
            else:
                newRecord[col] = newRecord[col]
        return newRecord

    def generate_pewma(self, newRecord, event):
        for col in self.data_cols:
            t = len(newRecord[col])
            newRecord["s1_" + col] = newRecord["s1_next_" + col]
            newRecord["STD_" + col] = newRecord["STD_next_" + col]
            try:
                newRecord["Z_" + col] = (float(event[col]) -
                                         newRecord["s1_" + col]) / newRecord["STD_" + col]
            except ZeroDivisionError:
                newRecord["Z_" + col] = 0
            newRecord["P_" + col] = \
                1 / math.sqrt(2 * math.pi) * \
                math.exp(-math.pow(newRecord["Z_" + col], 2) / 2)
            newRecord["alpha_" + col] = \
                self.calc_alpha(newRecord, t, col)
            newRecord["s1_" + col] = \
                newRecord["alpha_" + col] * newRecord["s1_" + col] + \
                (1 - newRecord["alpha_" + col]) * float(event[col])
            newRecord["s2_" + col] = \
                newRecord["alpha_" + col] * newRecord["s2_" + col] + (1 - newRecord["alpha_" + col]) * math.pow(
                    float(event[col]), 2)
            newRecord["s1_next_" + col] = newRecord["s1_" + col]
            newRecord["STD_next_" + col] = \
                math.sqrt(newRecord["s2_" + col] -
                          math.pow(newRecord["s1_" + col], 2))
            isAnomaly = newRecord["P_" + col] <= self.threshold
            newRecord[col + "_is_Anomaly"] = isAnomaly
        return newRecord

    def calc_alpha(self, newRecord, t, col):
        if t < self.T:
            alpha = 1 - 1.0 / t
        else:
            alpha = (1 - self.beta * newRecord["P_" + col]) * self.alpha_0
        return alpha

    def mongo_setup(self, host="localhost", port="27017", dbName="Sibyl", collectionName="pewma_ifm"):
        mongoClient = pymongo.MongoClient(
            "mongodb://" + host + ":" + port + "/")
        self.db = mongoClient[dbName]
        self.coll = self.db[collectionName]
