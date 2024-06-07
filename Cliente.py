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

    topics = ["tema1", "tema2", "tema3"]

    while True:
        print("\nMenú principal:")
        print("1. Suscribirse a un tema")
        print("2. Publicar un mensaje")
        print("3. Salir")

        opcion = input("Seleccione una opción (1/2/3): ")

        if opcion == '1':
            print("Temas disponibles para suscripción:")
            for i, topic in enumerate(topics):
                print(f"{i + 1}. {topic}")
            while True:
                selected_topics = input(
                    "Ingrese los números de los temas a los que desea suscribirse (separados por comas): ")
                selected_topics = selected_topics.split(',')
                try:
                    selected_topics = [int(i) for i in selected_topics]
                    if all(1 <= i <= 3 for i in selected_topics):
                        break
                    else:
                        print("Error: Todos los números deben estar entre 1 y 3. Intente de nuevo.")
                except ValueError:
                    print("Error: Por favor, ingrese números válidos. Intente de nuevo.")

            selected_topics = [topics[i - 1] for i in selected_topics]

            for topic in selected_topics:
                t = threading.Thread(target=subscribe_to_topic, args=(stub, topic))
                t.daemon = True
                t.start()
                print(f"Te has suscrito a {topic}!")
        elif opcion == '2':
            topic = input("Ingrese el tema al que desea publicar (tema1/tema2/tema3): ")
            if topic not in topics:
                print("Tema no válido. Inténtalo de nuevo.")
                continue
            message = input("Ingrese el mensaje que desea publicar: ")
            publish_message(stub, topic, message)
        elif opcion == '3':
            print("Saliendo del programa. ¡Hasta luego!")
            break
        else:
            print("Opción no válida. Inténtalo de nuevo.")

if __name__ == '__main__':
    main()
