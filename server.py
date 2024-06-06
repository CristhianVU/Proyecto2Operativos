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
                self.queues[topic].put(message, block=False)
                if topic in self.subscribers:
                    for subscriber in self.subscribers[topic]:
                        subscriber.put(message)
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
            subscriber_queue = queue.Queue(maxsize=5)
            self.subscribers[topic].append(subscriber_queue)
        while True:
            try:
                message = subscriber_queue.get(block=True)
                yield service_pb2.SubscribeReply(message=message)
            except queue.Empty:
                continue

class Subscriber:
    def __init__(self, context):
        self.context = context
        self.messages = []
        self.lock = threading.Lock()

    def send_message(self, message):
        with self.lock:
            self.messages.append(message)

    def get_response_stream(self, semaphore):
        while True:
            if self.context.is_active():
                semaphore.acquire()
                with self.lock:
                    if self.messages:
                        yield service_pb2.SubscribeReply(message=self.messages.pop(0))
            else:
                break

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
