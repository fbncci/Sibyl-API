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

    def lambda_handler(self, event):
        query = {"IdSig": event["IdSig"]}
        response = self.coll.find_one(query)
        value = float(event["Value"])
        if response:
            warningLimit = response["warningLimit"]
            damageLimit = response["warningLimit"]
            event["static_control_limits"] = {}
            event["static_control_limits"]["warningLimit"] = False
            event["static_control_limits"]["damageLimit"] = False
            if value > warningLimit:
                event["static_control_limits"] = True
            if value > damageLimit:
                event["static_control_limits"] = True
        else:
            event = query
            event["static_control_limits"] = 250
            event["static_control_limits"] = 500
            self.coll.insert_one(event)  # write new record
        return event
            
    def mongo_setup(self, host="localhost", port="27017", dbName="Sibyl", collectionName="control_limits"):
        mongoClient = pymongo.MongoClient(
            "mongodb://" + host + ":" + port + "/")
        self.db = mongoClient[dbName]
        self.coll = self.db[collectionName]
