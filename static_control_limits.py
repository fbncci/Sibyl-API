# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 13:52:04 2019

@author: Jack Press
"""
import pymongo

class StaticControlLimits:
    def __init__(self, host="localhost", port="27017", dbName="Sibyl", collectionName="control_limits"):
        '''
        add description
        '''
        self.mongo_setup(host, port, dbName, collectionName)

    def lambda_handler(self, event, param=None):
        if param:
            response = param
            self.new_record(event, param)
        else:
            query = {"IdSig": event["IdSig"]}
            response = self.coll.find_one(query)

        result = self.test_data(event, response)

        return result

    def test_data(self, event, response):
        value = float(event["Value"])
        event["static_control_limits"] = {}
        event["static_control_limits"]["warningLimit"] = False
        event["static_control_limits"]["damageLimit"] = False
        if value > response["warningLimit"]:
            event["static_control_limits"] = True
        if value > response["damageLimit"]:
            event["static_control_limits"] = True
        return event

    def new_record(self, event, param):
        event = {"IdSig": event["IdSig"], "static_control_limits": {}}
        event["static_control_limits"]["warningLimit"] = param["warningLimit"]
        event["static_control_limits"]["damageLimit"] = param["damageLimit"]
        self.coll.insert_one(event)  # write new record

    def mongo_setup(self, host="localhost", port="27017", dbName="Sibyl", collectionName="control_limits"):
        mongoClient = pymongo.MongoClient(
            "mongodb://" + host + ":" + port + "/")
        self.db = mongoClient[dbName]
        self.coll = self.db[collectionName]
