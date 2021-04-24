#!/usr/bin/env python
import pika
import log
import config
import os
import datetime
from pathlib import Path

class RabbitMQ_Connector():
    # https: // habr.com / ru / sandbox / 126064 /
    def __init__(self):

        self.log = log.Logs('RabbitMQ_Connector')
        self.log.rewriteFile()
        self.minloglevel = 10
        self.trace('initialasing...', 0)

        settings = config.read_config('config.ini', 'connect_server')
        if settings:
            self.host = settings['host']
            self.port = int(settings['port'])
            self.user = settings['user']
            self.password = settings['password']
            self.queues = settings['queuesin'].split(',')
            self.exchange = settings['exchangesout']
            self.trace(f'queues {self.queues}', 0)

        else:
            self.trace(f"Can't find options for connection", 0)
            quit()

        settings = config.read_config('config.ini', 'in_parameters')
        if settings:
            self.dir_in = settings['directory']
            if not os.path.exists(self.dir_in):
                os.makedirs(self.dir_in)
            self.error_in = settings['error']
            if not os.path.exists(self.error_in):
                os.makedirs(self.error_in)
        else:
            self.trace(f"Can't find options for incoming", 0)
            quit()

        settings = config.read_config('config.ini', 'out_parameters')
        if settings:
            self.dir_out = settings['directory']
            if not os.path.exists(self.dir_out):
                os.makedirs(self.dir_out)
            self.error_out = settings['error']
            if not os.path.exists(self.error_out):
                os.makedirs(self.error_out)
        else:
            self.trace(f"Can't find options for outcoming", 0)
            quit()

        for i in self.queues:
            path = Path(self.dir_in, i)
            if not os.path.exists(path):
                os.makedirs(path)

        self.connection = None
        self.channel = None


    def trace(self, message, levellog):
        if levellog <= self.minloglevel:
            self.log.trace(message, levellog)

    def get_connection(self):
        self.trace(f"Try connection to server RabbitMQ {self.host}:{self.port} user {self.user} password:{self.password}", 0)
        credentials = pika.PlainCredentials(username=self.user, password=self.password)
        parameters = pika.ConnectionParameters(host=self.host, port=self.port, credentials=credentials)
        try:
            self.connection = pika.BlockingConnection(parameters)
            self.trace(f"OK ", 0)
        except Exception as e:
            self.trace(f"Error try get connection {e} ", 0)
            quit()

    def close_connection(self):
        self.trace(f"Try close connection ", 7)
        if self.connection != None:
            self.connection.close()
            self.trace(f"OK ", 7)
        else:
            self.trace(f"Error try close non-existent connection ", 7)

    def get_channel(self):
        self.trace(f"Try get channel ", 7)
        if self.connection != None:
            self.channel = self.connection.channel()
        else:
            self.trace(f"Error try get channel for non-existent connection ", 7)

    def close_channel(self):
        self.trace(f"Try close channel ", 7)
        if self.channel != None:
            self.channel.close()
        else:
            self.trace(f"Error try close non-existent channel ", 7)

    def get_quantity(self, name_queue):
        result = None
        if self.channel != None:
            try:
                result = self.channel.queue_declare(queue=name_queue, passive=True).method.message_count
            except Exception as e:
                self.trace(f"Error try get queue's {name_queue} quantity", 7)
            finally:
                self.trace(f"OK", 7)
                return result
        else:
            self.trace(f"Error try get queue's {name_queue} quantity for non-existent channel", 7)

    def get_message(self, name_queue, pref, save_body = True, confirm_delivery = True):
        method_frame, properties, body = self.channel.basic_get(name_queue)
        self.trace(f"Get message", 7)
        if save_body:
            filename = properties.headers.get('filename', 'unknown')
            if filename == 'unknown':
                filename += f'{str(datetime.datetime.now().strftime("%d%m%Y%H%M%S"))}_{str(pref)}'
            file = Path('IN', name_queue, filename)
            self.trace(f"Try save message in file {file}", 5)
            with open(file,'w') as f:
                f.write(body.decode())
            self.trace(f"OK", 5)
            if confirm_delivery:
                self.channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        else:
            if confirm_delivery:
                self.channel.basic_ack(delivery_tag = method_frame.delivery_tag)
        return method_frame, properties, body

    def send_message(self, routingkey, body, file):
        properties = pika.BasicProperties(headers={"filename": file})
        self.channel.basic_publish(exchange=self.exchange,
                                               routing_key=routingkey,
                                               body=body,
                                               properties=properties)
        self.trace(f'send: {body} rout:{routingkey} filename: {file} OK', 10)
        return True


if __name__ == '__main__':
    rabb = RabbitMQ_Connector()
    rabb.get_connection()
    rabb.get_channel()
    for i in rabb.queues:
        k = rabb.get_quantity(i)
        print(f'{i} {k}')
        for j in range(k):
            method_frame, properties, body = rabb.get_message(i, j)
            print(f'{j} {method_frame} {properties}')
            print(f'{body}')
            print('***********************************')
    rabb.close_channel()
    rabb.close_connection()
