from concurrent import futures
import grpc
import time
import threading
import logging
import service_pb2
import service_pb2_grpc
from collections import defaultdict
from queue import Queue

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
        self.subscribers = defaultdict(dict)
        self.message_queues = defaultdict(lambda: Queue(maxsize=5))  # Colas de mensajes por tema
        self.lock = threading.Lock()
        self.semaphore = threading.Semaphore(0)

    def Publish(self, request, context):
        with self.lock:
            topic = request.topic
            message = request.message
            if topic in self.subscribers:
                for client_id, subscribers in self.subscribers[topic].items():
                    for subscriber in subscribers:
                        subscriber.send_message(message)
                        self.semaphore.release()
            else:
                # Si no hay suscriptores, guardar el mensaje en la cola correspondiente
                if not self.message_queues[topic].full():
                    self.message_queues[topic].put(message)
                    logger.info(f"Mensaje almacenado en cola para el tema {topic}")
                else:
                    logger.warning(f"Cola llena para el tema {topic}. Mensaje descartado.")
        return service_pb2.PublishReply(status="Message published")

    def Subscribe(self, request, context):
        topic = request.topic
        client_id = context.peer()  # Usar el contexto del cliente como ID único
        with self.lock:
            if topic not in self.subscribers:
                self.subscribers[topic] = {}
            if client_id not in self.subscribers[topic]:
                self.subscribers[topic][client_id] = []
            subscriber = Subscriber(context)
            self.subscribers[topic][client_id].append(subscriber)
            logger.info(f"Nuevo cliente suscrito al tema {topic}")
            # Consumir mensajes de la cola si hay mensajes pendientes
            for message in list(self.message_queues[topic].queue):
                subscriber.send_message(message)
                self.semaphore.release()
                logger.info(f"Mensaje enviado a nuevo suscriptor en el tema {topic}")
        return subscriber.get_response_stream(self.semaphore)

    def Unsubscribe(self, request, context):
        topic = request.topic
        client_id = context.peer()  # Usar el contexto del cliente como ID único
        with self.lock:
            if topic in self.subscribers and client_id in self.subscribers[topic]:
                self.subscribers[topic][client_id] = [
                    s for s in self.subscribers[topic][client_id] if s.context != context
                ]
                if not self.subscribers[topic][client_id]:
                    del self.subscribers[topic][client_id]  # Eliminar cliente si no tiene suscripciones
                if not self.subscribers[topic]:
                    del self.subscribers[topic]  # Eliminar el tema si no tiene suscriptores
                logger.info(f"Cliente desuscrito del tema {topic}")
        return service_pb2.UnsubscribeReply(status="Unsubscribed from topic")

class Subscriber:
    def __init__(self, context):
        self.context = context
        self.messages = []
        self.lock = threading.Lock()

    def send_message(self, message):
        with self.lock:
            self.messages.append(message)

    def get_response_stream(self, semaphore, view_old_messages=False):
        while True:
            if self.context.is_active():
                semaphore.acquire()
                with self.lock:
                    if not self.messages and view_old_messages:
                        semaphore.release()
                        break
                    elif self.messages:
                        yield service_pb2.SubscribeReply(message=self.messages.pop(0))
                        semaphore.release()
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
