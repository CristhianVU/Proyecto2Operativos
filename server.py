import queue
from concurrent import futures
import grpc
import time
import threading
import logging
import service_pb2
import service_pb2_grpc

# Configuración del logger
logger = logging.getLogger('MessageBroker')
logger.setLevel(logging.INFO)

# Crear un manejador para escribir en un archivo de log
file_handler = logging.FileHandler('server.log')
file_handler.setLevel(logging.INFO)

# Crear un manejador para escribir en la consola
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Formato del log
formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%d/%m/%Y:%H:%M:%S')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Agregar los manejadores al logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

class MessageBrokerServicer(service_pb2_grpc.MessageBrokerServicer):
    def __init__(self):
        self.queues = {}
        self.subscribers = {}
        self.lock = threading.Lock()

    def Publish(self, request, context):
        with self.lock:
            topic = request.topic
            message = request.message
            if topic not in self.queues:
                self.queues[topic] = queue.Queue(maxsize=5)
            try:
                self.queues[topic].put(message)
                if topic in self.subscribers:
                    for subscriber in self.subscribers[topic]:
                        subscriber.send_message(message)
                return service_pb2.PublishReply(status="Message published")
            except queue.Full:
                return service_pb2.PublishReply(status="Queue is full")

    def Subscribe(self, request, context):
        topic = request.topic
        with self.lock:
            if topic not in self.queues:
                self.queues[topic] = queue.Queue(maxsize=5)
            if topic not in self.subscribers:
                self.subscribers[topic] = []
            subscriber = Subscriber(context)
            self.subscribers[topic].append(subscriber)
        threading.Thread(target=self._send_messages_to_subscriber, args=(topic, subscriber)).start()
        return subscriber.get_response_stream()

    def _send_messages_to_subscriber(self, topic, subscriber):
        while True:
            message = self.queues[topic].get()
            subscriber.send_message(message)


class Subscriber:
    def __init__(self, context):
        self.context = context
        self.messages = []
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)

    def send_message(self, message):
        with self.condition:
            self.messages.append(message)
            self.condition.notify()

    def get_response_stream(self):
        while True:
            with self.condition:
                while not self.messages and self.context.is_active():
                    self.condition.wait()
                if not self.context.is_active():
                    break
                message = self.messages.pop(0)
                yield service_pb2.SubscribeReply(message=message)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service_pb2_grpc.add_MessageBrokerServicer_to_server(MessageBrokerServicer(), server)
    port = 50051
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logger.info(f"Servidor iniciado y escuchando en el puerto {port}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
