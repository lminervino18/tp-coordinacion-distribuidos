import os
import signal
import socket
import logging
import multiprocessing

import message_handler
from common import middleware, message_protocol

SERVER_HOST = os.environ["SERVER_HOST"]
SERVER_PORT = int(os.environ["SERVER_PORT"])

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]


def _safe_shutdown_socket(sock):
    if sock is None:
        return

    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass


def _safe_close_socket(sock):
    if sock is None:
        return

    try:
        sock.close()
    except OSError:
        pass


def handle_client_request(client_socket, message_handler_instance):
    output_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, OUTPUT_QUEUE)

    try:
        while True:
            message = message_protocol.external.recv_msg(client_socket)

            if message[0] == message_protocol.external.MsgType.FRUIT_RECORD:
                serialized_message = message_handler_instance.serialize_data_message(
                    message[1]
                )
                output_queue.send(serialized_message)
                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )

            if message[0] == message_protocol.external.MsgType.END_OF_RECODS:
                serialized_message = message_handler_instance.serialize_eof_message(
                    message[1]
                )
                output_queue.send(serialized_message)
                message_protocol.external.send_msg(
                    client_socket, message_protocol.external.MsgType.ACK
                )
                return
    except socket.error:
        logging.error("The connection with the server was lost")
    except Exception as exc:
        logging.error(exc)
    finally:
        try:
            output_queue.close()
        except Exception as exc:
            logging.error(exc)


def handle_client_response(client_list):
    input_queue = middleware.MessageMiddlewareQueueRabbitMQ(MOM_HOST, INPUT_QUEUE)

    def _consume_result(message, ack, nack):
        matched_client_index = None
        matched_client_socket = None

        try:
            for index, [message_handler_instance, client_socket] in enumerate(client_list):
                deserialized_message = (
                    message_handler_instance.deserialize_result_message(message)
                )

                if not deserialized_message:
                    continue

                message_protocol.external.send_msg(
                    client_socket,
                    message_protocol.external.MsgType.FRUIT_TOP,
                    deserialized_message,
                )
                message_protocol.external.recv_msg(client_socket)

                matched_client_index = index
                matched_client_socket = client_socket
                break

            if matched_client_index is None:
                logging.warning("No client handler could process this message")
                nack()
                return

            client_list.pop(matched_client_index)
            ack()
        except socket.error:
            logging.error("The connection with the server was lost")
            if matched_client_index is not None:
                try:
                    client_list.pop(matched_client_index)
                except Exception:
                    pass
            ack()
        except Exception as exc:
            logging.error(exc)
            nack()
            input_queue.stop_consuming()
        finally:
            if matched_client_socket is not None:
                _safe_shutdown_socket(matched_client_socket)
                _safe_close_socket(matched_client_socket)

    try:
        input_queue.start_consuming(_consume_result)
    finally:
        try:
            input_queue.close()
        except Exception as exc:
            logging.error(exc)


def handle_sigterm(server_socket, client_list, sigterm_received):
    _safe_shutdown_socket(server_socket)
    _safe_close_socket(server_socket)

    for [_, client_socket] in client_list:
        _safe_shutdown_socket(client_socket)
        _safe_close_socket(client_socket)

    sigterm_received.value = 1


def main():
    logging.basicConfig(level=logging.INFO)

    with multiprocessing.Manager() as manager:
        client_list = manager.list()
        sigterm_received = manager.Value("c_short", 0)

        process_count = os.cpu_count() or 1

        with multiprocessing.Pool(processes=process_count) as processes_pool:
            processes_pool.apply_async(handle_client_response, (client_list,))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                logging.info("Listening to connections")
                server_socket.bind((SERVER_HOST, SERVER_PORT))
                server_socket.listen()
                signal.signal(
                    signal.SIGTERM,
                    lambda signum, frame: handle_sigterm(
                        server_socket, client_list, sigterm_received
                    ),
                )

                while True:
                    try:
                        client_socket, _ = server_socket.accept()

                        logging.info("A new client has connected")
                        message_handler_instance = message_handler.MessageHandler()
                        client_list.append([message_handler_instance, client_socket])
                        processes_pool.apply_async(
                            handle_client_request,
                            (client_socket, message_handler_instance),
                        )
                    except socket.error:
                        if sigterm_received.value == 0:
                            logging.error("The connection with the client was lost")
                            return 1
                        return 0
                    except Exception as exc:
                        logging.error(exc)
                        return 2

    return 0


if __name__ == "__main__":
    main()