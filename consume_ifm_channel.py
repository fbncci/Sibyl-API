# -*- coding: utf-8 -*-
"""
Created on Tue Aug 13 11:58:25 2019

@author: F91064C
"""

import pika
import json

def callback(ch, method, properties, body):
    topic = str(method).split('routing_key=')[-1].split("'")[0]
    print(topic)
    data = json.loads(body.decode("UTF-8"))
    print(data)

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
    on_message_callback=callback, 
    auto_ack=True)
print(' [*] Waiting for logs. To exit press CTRL+C')
channel.start_consuming()