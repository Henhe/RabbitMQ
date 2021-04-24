import log
import os
import rabbitMQ_client
import config
from pathlib import Path
import json

class Sender():
    def __init__(self):
        self.log = log.Logs('Sender')
        self.log.rewriteFile()
        self.minloglevel = 10
        self.trace('initialasing RabbitMQ_Connector...', 0)

        self.rabb_client = rabbitMQ_client.RabbitMQ_Connector()
        self.dir_out = self.rabb_client.dir_out
        # self.error_out = self.rabb_client.error_out

    def trace(self, message, levellog):
        if levellog <= self.minloglevel:
            self.log.trace(message, levellog)

    def read_directory(self):
        self.trace(f"Try find json files in directory {self.dir_out}", 0)
        files = os.listdir(self.dir_out)
        jsonfiles = list(filter(lambda x: x.endswith('.json'), files))
        self.trace(f"OK", 10)
        return jsonfiles

    def read_routingkey_file(self, filename):
        self.trace(f'Read routing key from file {filename}',10)
        file_ = Path(self.dir_out, filename)
        dict = {}
        with open(file_,'r') as f:
            dict = json.load(f)
        self.trace('OK', 10)
        return dict.get('routingkey', None), dict

    def run(self):
        self.trace(f'Connnect to RabbitMQ, channel', 2)
        self.rabb_client.get_connection()
        self.rabb_client.get_channel()
        self.trace(f'OK', 2)

        files = self.read_directory()

        if len(files) > 0:
            for file in files:
                self.trace(f'Work on {file}',10)
                routingkey, message = self.read_routingkey_file(file)
                path_from = Path(self.dir_out, file)
                if routingkey != None:
                    res = self.rabb_client.send_message(routingkey, json.dumps(message), file)
                    if res:
                        os.remove(path_from)
                    self.trace(f'{file} OK',10)
                else:
                    path_to = Path(self.dir_out,'ERROR', file)
                    os.replace(path_from, path_to)
                    self.trace(f"Routing key doesn't found for file {file}", 3)

        self.trace(f'Disconnnect to RabbitMQ, close channel', 2)
        self.rabb_client.close_channel()
        self.rabb_client.close_connection()
        self.trace(f'OK', 2)

if __name__ == '__main__':
    sender = Sender()
    # print(sender.read_directory())
    sender.run()