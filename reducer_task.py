import reducer
import socket
import pickle
import comm_pb2
import logging


class ReducerTask:

    def __init__(self, server_ip, server_port, reduce_task, output_dict):
        self.data = None
        self.inverted_index_output = None
        self.word_count_output = None
        self.map_task = reduce_task
        self.output_dict = output_dict
        self.server_ip = server_ip
        self.server_port = server_port
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def get_data(self):
        try:
            message_info = self.client_socket.recv(16)
            info = comm_pb2.Info()
            info.ParseFromString(message_info)

            message_header = self.client_socket.recv(22)
            header = comm_pb2.Header()
            header.ParseFromString(message_header)
            data_len = (header.header.split(":")[0])
            reducer_data = b''
            while len(reducer_data) < int(data_len):
                to_read = int(data_len) - len(reducer_data)
                data = self.client_socket.recv(
                    4096 if to_read > 4096 else to_read
                )
                reducer_data += data
            self.data = pickle.loads(reducer_data)
        except Exception as e:
            logging.error("Reducer received Exception", str(e))
            raise e

    def reduce_data(self):
        if self.map_task == "word_count":
            try:
                logging.info("WC Reduce data")
                reducer_task = reducer.Reducer()
                self.word_count_output = reducer_task.word_count_reduce(self.data)

                pkl_key_value_store = pickle.dumps(self.word_count_output)
                header = str('')
                header += str(len(pkl_key_value_store))
                header += ':'
                while len(header) != 20:
                    header += '9'

                message_info = comm_pb2.Info()
                message_header = comm_pb2.Header()

                message_info.send_info = "Sending header"
                message_header.header = header

                self.client_socket.send(message_info.SerializeToString())
                self.client_socket.send(message_header.SerializeToString())
                self.client_socket.sendall(pkl_key_value_store)
            except Exception as e:
                logging.error("WC reduce data received Exception", str(e))
                raise e
        else:
            try:
                logging.info("ININDEX Reduce data")
                mapper_task = reducer.Reducer()
                self.inverted_index_output = mapper_task.inverted_index_reduce(
                    self.data
                )

                pkl_key_value_store = pickle.dumps(self.inverted_index_output)
                header = str('')
                header += str(len(pkl_key_value_store))
                header += ':'
                while len(header) != 20:
                    header += '9'

                message_info = comm_pb2.Info()
                message_header = comm_pb2.Header()

                message_info.send_info = "Sending header"
                message_header.header = header

                self.client_socket.send(message_info.SerializeToString())
                self.client_socket.send(message_header.SerializeToString())
                self.client_socket.sendall(pkl_key_value_store)
            except Exception as e:
                logging.error("ININDEX reduce data received Exception", str(e))
                raise e

    def terminate(self):
        self.client_socket.close()

