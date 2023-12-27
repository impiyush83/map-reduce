import mapper
import socket
import pickle
import comm_pb2
import logging


class MapperTask:
    word_count_output = None
    server_ip = None
    server_port = None
    word_count_data = None
    inverted_index_data = None
    client_socket = None

    def __init__(self, server_ip, server_port, number_of_reducers, map_task):
        logging.info("Mapper Task Init")
        self.data = None
        self.output_inverted_index_dict = None
        self.server_ip = server_ip
        self.server_port = server_port
        self.map_task = map_task
        self.output_dict = {}
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.number_of_reducers = number_of_reducers
        for i in range(self.number_of_reducers):
            self.output_dict[i] = []

    @staticmethod
    def inverted_index_mapper(file_content_dict):
        try:
            logging.info("Mapper Task Inverted Index Mapper")
            inverted_index_dict = {}
            for item in file_content_dict.items():
                filename = item[0]
                text = item[1]
                for word in text.split(" "):
                    if word not in inverted_index_dict:
                        inverted_index_dict[word + "-" + filename] = 1
                    else:
                        inverted_index_dict[word + "-" + filename] += 1
            return inverted_index_dict
        except Exception as e:
            logging.info("Error in Mapper Task Inverted Index Mapper")
            logging.error(str(e))
            raise e

    def get_data(self):
        if self.map_task == "word_count":
            try:
                logging.info("Mapper Task Word Count Get Data")
                message_info = self.client_socket.recv(16)
                info = comm_pb2.Info()
                info.ParseFromString(message_info)

                message_header = self.client_socket.recv(22)
                header = comm_pb2.Header()
                header.ParseFromString(message_header)
                data_len = (header.header.split(":")[0])

                mapper_data = b''
                while len(mapper_data) < int(data_len):
                    to_read = int(data_len) - len(mapper_data)
                    data = self.client_socket.recv(
                        4096 if to_read > 4096 else to_read
                    )
                    mapper_data += data
                message_text_chunk = comm_pb2.MapperTextChunk()
                message_text_chunk.ParseFromString(mapper_data)
                self.word_count_data = message_text_chunk.text_chunk
            except Exception as e:
                logging.info("Error Mapper Task Word Count Get Data")
                logging.error(str(e))
                raise e
        else:
            try:
                logging.info("Mapper Task Inverted Index Get Data")
                message_info = self.client_socket.recv(16)
                info = comm_pb2.Info()
                info.ParseFromString(message_info)

                message_header = self.client_socket.recv(22)
                header = comm_pb2.Header()
                header.ParseFromString(message_header)

                data_len = (header.header.split(":")[0])
                mapper_data = b''
                while len(mapper_data) < int(data_len):
                    to_read = int(data_len) - len(mapper_data)
                    data = self.client_socket.recv(
                        4096 if to_read > 4096 else to_read
                    )
                    mapper_data += data
                self.inverted_index_data = pickle.loads(mapper_data)
            except Exception as e:
                logging.info("Error Mapper Task Inverted Index Get Data")
                logging.error(str(e))
                raise e

    def process_data(self):
        try:

            mapper_task = mapper.Mapper()
            if self.map_task == "word_count":
                logging.info("Mapper Task Process Data for Word Count")
                self.word_count_output = mapper_task.word_count_mapper(
                    self.word_count_data
                )
                self.distribute_to_reducers_word_count()
            else:
                logging.info("Mapper Task Process Data for Inv Index")
                self.output_inverted_index_dict = mapper_task.inverted_index_mapper(
                    self.inverted_index_data
                )
                self.distribute_to_reducers_inverted_index()
        except Exception as e:
            logging.error(str(e))
            raise e

    def distribute_to_reducers_word_count(self):
        try:
            logging.info("Distribute to reducers word count in mapper task")
            for items in self.word_count_output.items():
                word = items[0]
                ascii_sum = 0
                for i in range(len(word)):
                    ascii_sum += ord(word[i])
                key = ascii_sum % self.number_of_reducers
                self.output_dict[key].append(items)

            pkl_dict = pickle.dumps(self.output_dict)

            header = str('')
            header += str(len(pkl_dict))
            header += ':'
            while len(header) != 20:
                header += '9'

            message_info = comm_pb2.Info()
            message_header = comm_pb2.Header()
            message_info.send_info = "Sending header"
            message_header.header = header

            self.client_socket.send(message_info.SerializeToString())
            self.client_socket.send(message_header.SerializeToString())
            self.client_socket.sendall(pkl_dict)
        except Exception as e:
            logging.info(
                "Error in Distribute to reducers word count in mapper task")
            logging.error(str(e))
            raise e

    def distribute_to_reducers_inverted_index(self):
        try:
            logging.info("Distribute to reducers Inverted Index in mapper task")
            for items in self.output_inverted_index_dict.items():
                word = items[0]
                ascii_sum = 0
                for i in range(len(word)):
                    ascii_sum += ord(word[i])
                if ascii_sum % self.number_of_reducers not in self.output_dict:
                    self.output_dict[ascii_sum % self.number_of_reducers] = [
                        (items[0], items[1])
                    ]
                else:
                    self.output_dict[
                        ascii_sum % self.number_of_reducers
                    ].extend([(items[0], items[1])])

            pkl_dict = pickle.dumps(self.output_dict)

            header = str('')
            header += str(len(pkl_dict))
            header += ':'

            while len(header) != 20:
                header += '9'

            message_info = comm_pb2.Info()
            message_header = comm_pb2.Header()
            message_info.send_info = "Sending header"
            message_header.header = header

            self.client_socket.send(message_info.SerializeToString())
            self.client_socket.send(message_header.SerializeToString())
            self.client_socket.sendall(pkl_dict)
        except Exception as e:
            logging.info(
                "Error in Distribute to reducers Inverted Index in mapper task")
            logging.error(str(e))
            raise e

    def terminate(self):
        self.client_socket.close()

