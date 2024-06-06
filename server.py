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
        self.message_queues = {}
        self.subscribers = {}
        self.lock = threading.Lock()
        self.cond = threading.Condition(self.lock)

    def Publish(self, request, context):
        with self.lock:
            topic = request.topic
            message = request.message
            if topic not in self.message_queues:
                self.message_queues[topic] = queue.Queue(maxsize=5)
            try:
                self.message_queues[topic].put_nowait((message, []))  # Guardar una lista de suscriptores que ya han recibido el mensaje
                logger.info(f"Mensaje enviado al tema {topic}")
                if topic in self.subscribers:
                    for subscriber in self.subscribers[topic]:
                        subscriber.send_message(message)
                return service_pb2.PublishReply(status="Message published")
            except queue.Full:
                logger.info(f"La cola del tema {topic} está llena, no se puede publicar el mensaje")
                return service_pb2.PublishReply(status="Cola llena, el mensaje no puede ser publicado")
            finally:
                self.cond.notify_all()  # Notificar a todos los suscriptores de que se ha publicado un nuevo mensaje

    def Subscribe(self, request, context):
        topic = request.topic
        with self.lock:
            if topic not in self.message_queues:
                self.message_queues[topic] = queue.Queue(maxsize=5)
            if topic not in self.subscribers:
                self.subscribers[topic] = []
            subscriber = Subscriber(context)
            self.subscribers[topic].append(subscriber)
            logger.info(f"Nuevo cliente suscrito al tema {topic}")

            # Enviar todos los mensajes que ya están en la cola
            while not self.message_queues[topic].empty():
                try:
                    message, receivers = self.message_queues[topic].queue[0]  # No extraer el mensaje de la cola
                    if subscriber not in receivers:
                        subscriber.send_message(message)
                        receivers.append(subscriber)
                    if set(receivers) == set(self.subscribers[
                                                 topic]):  # Si todos los suscriptores han recibido el mensaje, eliminarlo de la cola
                        self.message_queues[topic].get_nowait()
                except queue.Empty:
                    continue

            # Esperar hasta que se publique un nuevo mensaje
            while context.is_active():
                self.cond.wait()  # Bloquear hasta que se publique un nuevo mensaje
                if not self.message_queues[topic].empty():
                    try:
                        message, receivers = self.message_queues[topic].queue[0]  # No extraer el mensaje de la cola
                        if subscriber not in receivers:
                            subscriber.send_message(message)
                            receivers.append(subscriber)
                        if set(receivers) == set(self.subscribers[
                                                     topic]):  # Si todos los suscriptores han recibido el mensaje, eliminarlo de la cola
                            self.message_queues[topic].get_nowait()
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

    def get_response_stream(self):
        while True:
            if self.context.is_active():
                with self.lock:
                    while self.messages:
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
