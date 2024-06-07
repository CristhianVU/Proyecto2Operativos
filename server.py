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
        self.subscribers = {}
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
            logger.info(f"Mensaje enviado al tema {topic}")
        return service_pb2.PublishReply(status="Mensaje publicado!")

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
        return service_pb2.UnsubscribeReply(status="Te has desincrito! ")

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
