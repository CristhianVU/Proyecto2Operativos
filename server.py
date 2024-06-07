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
            logger.info(f"Mensaje publicado en {topic}: {message}")
            if topic not in self.queues:
                self.queues[topic] = queue.Queue(maxsize=3)
            try:
                self.queues[topic].put_nowait(message)
                if topic in self.subscribers:
                    for subscriber in self.subscribers[topic]:
                        subscriber.send_message(message)
                return service_pb2.PublishReply(status="Message published")
            except queue.Full:
                logger.warning(f"La cola para {topic} está llena. No se puede publicar el mensaje.")
                return service_pb2.PublishReply(status="Queue is full")

    def Subscribe(self, request, context):
        topic = request.topic
        with self.lock:
            if topic not in self.queues:
                self.queues[topic] = queue.Queue(maxsize=3)
            if topic not in self.subscribers:
                self.subscribers[topic] = []
            subscriber = Subscriber(context)
            self.subscribers[topic].append(subscriber)
        threading.Thread(target=self._send_messages_to_subscriber, args=(topic, subscriber)).start()
        logger.info(f"Cliente suscrito a {topic} en el hilo {threading.get_ident()}")
        return subscriber.get_response_stream()

    def _send_messages_to_subscriber(self, topic, subscriber):
        while True:
            try:
                message = self.queues[topic].get()
                logger.info(f"Mensaje enviado a suscriptor de {topic}: {message}")
                subscriber.send_message(message)
            except Exception as e:
                logger.error(f"Error enviando mensaje a suscriptor de {topic}: {e}")
                break


class Subscriber:
    def __init__(self, context):
        self.context = context
        self.messages = []
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.semaphore = threading.Semaphore(1)  # Inicializar el semáforo a 1
        self.sent_messages = set()  # Conjunto para mantener registro de mensajes enviados

    def send_message(self, message):
        self.semaphore.acquire()  # Decrementar el semáforo, bloquear si el semáforo es 0
        with self.condition:
            if message not in self.sent_messages:  # Verificar si el mensaje ya se ha enviado
                self.messages.append(message)
                self.sent_messages.add(message)  # Agregar el mensaje al registro de mensajes enviados
                logger.info(f"Mensaje enviado: {message}")  # Agregar registro
                self.condition.notify()
        self.semaphore.release()  # Liberar el semáforo fuera del bloque if

    def get_response_stream(self):
        while True:
            with self.condition:
                while not self.messages and self.context.is_active():
                    self.condition.wait()
                if not self.messages and not self.context.is_active():
                    break
                message = self.messages.pop(0)
                logger.info(f"Mensaje consumido en el hilo {threading.get_ident()}: {message}")
                self.semaphore.release()  # Incrementar el semáforo
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
