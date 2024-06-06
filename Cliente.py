import grpc
import threading
import service_pb2
import service_pb2_grpc

def publish_message(stub, topic, message):
    response = stub.Publish(service_pb2.PublishRequest(topic=topic, message=message))
    print(f"Estado de publicación: {response.status}")

def subscribe_to_topic(stub, topic):
    responses = stub.Subscribe(service_pb2.SubscribeRequest(topic=topic))
    for response in responses:
        print(f"Mensaje recibido en {topic}: {response.message}")

def main():
    channel = grpc.insecure_channel('localhost:50051')
    stub = service_pb2_grpc.MessageBrokerStub(channel)

    topics = ["topic1", "topic2", "topic3"]

    while True:
        print("\nMenú principal:")
        print("1. Suscribirse a un tema")
        print("2. Publicar un mensaje")
        print("3. Cancelar la suscripción a un tema")
        print("4. Salir")

        choice = input("Seleccione una opción (1/2/3/4): ")

        if choice == '1':
            print("Temas disponibles para suscripción:")
            for i, topic in enumerate(topics):
                print(f"{i + 1}. {topic}")
            selected_topics = input("Ingrese los números de los temas a los que desea suscribirse (separados por comas): ")
            selected_topics = selected_topics.split(',')
            selected_topics = [topics[int(i) - 1] for i in selected_topics]
            for topic in selected_topics:
                t = threading.Thread(target=subscribe_to_topic, args=(stub, topic))
                t.daemon = True
                t.start()
        elif choice == '2':
            topic = input("Ingrese el tema al que desea publicar: ")
            if topic not in topics:
                print("Tema no válido. Inténtalo de nuevo.")
                continue
            message = input("Ingrese el mensaje que desea publicar: ")
            publish_message(stub, topic, message)
        elif choice == '3':
            # Implementar la lógica para cancelar la suscripción a un tema
            pass
        elif choice == '4':
            print("Saliendo del programa. ¡Hasta luego!")
            break
        else:
            print("Opción no válida. Inténtalo de nuevo.")

if __name__ == '__main__':
    main()
