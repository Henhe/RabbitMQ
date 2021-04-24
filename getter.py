import log
import rabbitMQ_client

class Getter():
    def __init__(self):
        self.log = log.Logs('Getter')
        self.log.rewriteFile()
        self.minloglevel = 10

        self.trace('initialasing RabbitMQ_Connector...', 0)
        self.rabb_client = rabbitMQ_client.RabbitMQ_Connector()
        self.queues = self.rabb_client.queues

    def trace(self, message, levellog):
        if levellog <= self.minloglevel:
            self.log.trace(message, levellog)

    def run(self):
        self.trace(f'Connnect to RabbitMQ, channel', 2)
        self.rabb_client.get_connection()
        self.rabb_client.get_channel()
        self.trace(f'OK', 2)

        for i in self.rabb_client.queues:
            k = self.rabb_client.get_quantity(i)
            for j in range(k):
                method_frame, properties, body = self.rabb_client.get_message(i, j)

        self.trace(f'Disconnnect to RabbitMQ, close channel', 2)
        self.rabb_client.close_channel()
        self.rabb_client.close_connection()
        self.trace(f'OK', 2)

if __name__ == '__main__':
    getter = Getter()
    # print(sender.read_directory())
    getter.run()