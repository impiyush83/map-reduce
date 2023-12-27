import uuid
import socket
from flask import Flask, request
from _thread import *
import pickle
import mapper_task
import reducer_task
import concurrent.futures
import os
import comm_pb2
import logging
import requests

app = Flask(__name__)


class MapReduce:
    def __init__(self):
        self.cluster_id = uuid.uuid4()
        self.count_mapper = 0
        self.mapper_port = int(os.getenv('MAPPER_PORT')) or 8002
        self.reducer_port = int(os.getenv('REDUCER_PORT')) or 8003
        self.count_reducer = 0
        self.mapper_sock = None
        self.reducer_sock = None
        self.mappers_completed = 0
        self.reducers_completed = 0
        self.word_count_dict = {}
        self.inverted_index_dict = {}
        self.reducer_input = {}
        self.reducer_output_dict = {}
        self.final_output = {}
        self.connection_count = 0
        self.shuffle_finished = False
        self.mappers_started = 0
        self.reducers_started = 0

    def init_cluster(self, num_mappers, num_reducers):
        self.count_mapper = num_mappers
        self.count_reducer = num_reducers
        self.mapper_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.reducer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.mapper_sock.bind(
            (socket.gethostbyname(socket.gethostname()), self.mapper_port)
        )
        self.reducer_sock.bind((
            socket.gethostbyname(socket.gethostname()), self.reducer_port)
        )
        self.mapper_sock.listen()
        self.reducer_sock.listen()

    def threaded_mapper(self, everything):
        try:
            logging.info("Threaded mapper")
            mapper = mapper_task.MapperTask(
                str(everything[0]),
                int(everything[1]),
                int(everything[2]),
                everything[3]
            )
            mapper.client_socket.connect((mapper.server_ip, mapper.server_port))
            mapper.client_socket.send("Connected".encode('utf-8'))
            mapper.get_data()
            mapper.process_data()
            mapper.terminate()
            mapper.client_socket.close()
            self.connection_count -= 1
        except Exception as e:
            logging.error("Error in Threaded mapper")
            logging.error(str(e))
            raise e

    def threaded_reducer(self, everything):
        try:
            logging.info("Threaded reducer")
            reducer = reducer_task.ReducerTask(
                str(everything[0]),
                int(everything[1]),
                everything[3],
                self.reducer_input
            )
            reducer.client_socket.connect(
                (reducer.server_ip, reducer.server_port)
            )
            reducer.client_socket.send("Connected".encode('utf-8'))
            reducer.get_data()
            reducer.reduce_data()
            reducer.terminate()
            self.connection_count -= 1
            reducer.client_socket.close()
        except Exception as e:
            logging.error("Error in Threaded reducer")
            logging.error(str(e))
            raise e

    def threaded_server_mapper_word_count(self, conn, thread_id, text_chunk):
        try:
            logging.info("Threaded Server Mapper WC")
            text_chunk = (" ".join(x for x in text_chunk))
            message_text_chunk = comm_pb2.MapperTextChunk()
            message_text_chunk.text_chunk = text_chunk
            pkl_text_chunk = message_text_chunk.SerializeToString()
            header = str('')
            header += str(len(pkl_text_chunk))
            header += ':'

            while len(header) != 20:
                header += '9'
            message_info = comm_pb2.Info()
            message_header = comm_pb2.Header()

            message_info.send_info = "Sending header"
            message_header.header = header

            conn.send(message_info.SerializeToString())
            conn.send(message_header.SerializeToString())

            conn.sendall(pkl_text_chunk)

            message_info = conn.recv(16)
            info = comm_pb2.Info()
            info.ParseFromString(message_info)

            message_header = conn.recv(22)
            header = comm_pb2.Header()
            header.ParseFromString(message_header)
            data_len = (header.header.split(":")[0])
            mapper_data = b""

            while len(mapper_data) < int(data_len):
                to_read = int(data_len) - len(mapper_data)
                mapper_data += conn.recv(

                    4096 if to_read > 4096 else to_read
                )

            mapper_out = pickle.loads(mapper_data)
            self.word_count_dict[thread_id] = mapper_out

            for key in mapper_out.keys():
                with open(f'word_count/mapper/mapper_thread_{thread_id}_reducer_{key}.txt', 'w+') as file:
                    lines = []
                    for item in mapper_out[key]:
                        lines.append(str(item[0]) + ' ' + str(item[1]) + '\n')
                    file.writelines(lines)
            self.mappers_completed += 1
            conn.close()
        except Exception as e:
            logging.error("Error in Threaded server mapper WC")
            logging.error(str(e))
            raise e

    def threaded_server_mapper_inverted_index(
            self, conn, thread_id, inverted_index_dict):
        try:
            logging.info("Threaded Server Mapper INVERTED INDEX")
            pkl_key_value_store = pickle.dumps(inverted_index_dict)
            header = str('')
            header += str(len(pkl_key_value_store))
            header += ':'

            while len(header) != 20:
                header += '9'
            message_info = comm_pb2.Info()
            message_header = comm_pb2.Header()

            message_info.send_info = "Sending header"
            message_header.header = header

            conn.send(message_info.SerializeToString())
            conn.send(message_header.SerializeToString())
            conn.send(pkl_key_value_store)
            message_info = conn.recv(16)
            info = comm_pb2.Info()
            info.ParseFromString(message_info)

            message_header = conn.recv(22)
            header = comm_pb2.Header()
            header.ParseFromString(message_header)
            data_len = (header.header.split(":")[0])
            mapper_data = b""

            while len(mapper_data) < int(data_len):
                to_read = int(data_len) - len(mapper_data)

                mapper_data += conn.recv(

                    4096 if to_read > 4096 else to_read
                )
            mapper_out = pickle.loads(mapper_data)
            self.word_count_dict[thread_id] = mapper_out
            self.mappers_completed += 1
            conn.close()
        except Exception as e:
            logging.error("Error in Threaded server mapper INVERTED INDEX")
            logging.error(str(e))
            raise e

    def threaded_server_reducer_word_count(self, conn, thread_id, key_value_data):
        try:
            logging.info("Threaded server reducer WC")
            with open(f'word_count/reducer_input/{thread_id}', 'w+') as file:
                lines = []
                for item in key_value_data:
                    lines.append(str(item[0]) + ' ' + str(item[1]) + '\n')
                file.writelines(lines)
            pkl_key_value_store = pickle.dumps(key_value_data)
            header = str('')
            header += str(len(pkl_key_value_store))
            header += ':'
            while len(header) != 20:
                header += '9'

            message_info = comm_pb2.Info()
            message_header = comm_pb2.Header()

            message_info.send_info = "Sending header"
            message_header.header = header

            conn.send(message_info.SerializeToString())
            conn.send(message_header.SerializeToString())

            conn.sendall(pkl_key_value_store)

            message_info = conn.recv(16)
            info = comm_pb2.Info()
            info.ParseFromString(message_info)

            message_header = conn.recv(22)
            header = comm_pb2.Header()
            header.ParseFromString(message_header)
            data_len = (header.header.split(":")[0])
            reducer_data = b""
            while len(reducer_data) < int(data_len):
                to_read = int(data_len) - len(reducer_data)
                reducer_data += conn.recv(
                    4096 if to_read > 4096 else to_read
                )
            data = pickle.loads(reducer_data)
            reducer_out = data

            self.reducer_output_dict[thread_id] = reducer_out
            self.reducers_completed += 1
            conn.close()
        except Exception as e:
            logging.error("Error in Threaded server reducer WC")
            logging.error(str(e))
            raise e

    def threaded_server_reducer_inverted_index(
            self, conn, thread_id, key_value_data):
        try:
            logging.info("Threaded server reducer INVERTED INDEX")
            pkl_key_value_store = pickle.dumps(key_value_data)
            header = str('')
            header += str(len(pkl_key_value_store))
            header += ':'
            while len(header) != 20:
                header += '9'

            message_info = comm_pb2.Info()
            message_header = comm_pb2.Header()

            message_info.send_info = "Sending header"
            message_header.header = header

            conn.send(message_info.SerializeToString())
            conn.send(message_header.SerializeToString())

            conn.sendall(pkl_key_value_store)

            message_info = conn.recv(16)
            info = comm_pb2.Info()
            info.ParseFromString(message_info)

            message_header = conn.recv(22)
            header = comm_pb2.Header()
            header.ParseFromString(message_header)

            data_len = (header.header.split(":")[0])

            reducer_data = b""
            while len(reducer_data) < int(data_len):
                to_read = int(data_len) - len(reducer_data)
                reducer_data += conn.recv(
                    4096 if to_read > 4096 else to_read
                )
            data = pickle.loads(reducer_data)
            reducer_out = data
            self.reducer_output_dict[thread_id] = reducer_out
            self.reducers_completed += 1
            conn.close()
        except Exception as e:
            logging.error("Error in Threaded server reducer INVERTED INDEX")
            logging.error(str(e))
            raise e

    def threaded_server_word_count(self, parts_queue, output_location):
        try:
            self.connection_count = 0

            while self.mappers_started != self.count_mapper:
                if self.mappers_completed == self.count_mapper:
                    break
                conn, address = self.mapper_sock.accept()
                self.mappers_started += 1
                print(conn.recv(9))
                self.connection_count += 1
                start_new_thread(self.threaded_server_mapper_word_count, (conn, uuid.uuid4(), [
                    parts_queue[self.connection_count - 1],
                ],))

            while not self.shuffle_finished:
                continue

            reducer_count = self.count_reducer
            while self.reducers_started != self.count_reducer:
                if self.reducers_completed == self.count_reducer:
                    break

                conn, address = self.reducer_sock.accept()
                self.reducers_started += 1
                reducer_count -= 1
                print(conn.recv(9))
                start_new_thread(
                    self.threaded_server_reducer_word_count,
                    (
                        conn,
                        uuid.uuid4(),
                        self.reducer_input[reducer_count],
                    )
                )
            while self.reducers_completed != self.count_reducer:
                continue
            self.combine_reducer_output_word_count()
            print(self.final_output)
            with open(output_location, 'w+') as file:
                lines = []
                for item in self.final_output.items():
                    lines.append(str(item[0]) + ' ' + str(item[1]) + '\n')
                file.writelines(lines)
            print("Hurray! Word Count Completed")
            return
        except Exception as e:
            logging.error("Error in Threaded server WC")
            logging.error(str(e))
            raise e

    def threaded_server_inverted_index(self, inverted_index_dict, output_location):
        try:
            self.connection_count = 0
            while self.mappers_started != self.count_mapper:
                if self.mappers_completed == self.count_mapper:
                    break
                conn, address = self.mapper_sock.accept()
                self.mappers_started += 1
                print(conn.recv(9))

                self.connection_count += 1

                if inverted_index_dict.get(self.mappers_started - 1):
                    start_new_thread(
                        self.threaded_server_mapper_inverted_index,
                        (
                            conn,
                            uuid.uuid4(),
                            inverted_index_dict[self.mappers_started - 1],
                        )
                    )

            while not self.shuffle_finished:
                continue

            reducer_count = self.count_reducer
            while self.reducers_started != self.count_reducer:
                if self.reducers_completed == self.count_reducer:
                    break

                conn, address = self.reducer_sock.accept()
                self.reducers_started += 1
                reducer_count -= 1
                print(conn.recv(9))
                start_new_thread(
                    self.threaded_server_reducer_inverted_index,
                    (
                        conn,
                        uuid.uuid4(),
                        self.reducer_input[reducer_count],
                    )
                )
            while self.reducers_completed != self.count_reducer:
                continue
            self.combine_reducer_output_inverted_index()
            print(self.final_output)
            with open(output_location, 'w+') as file:
                lines = []
                for item in self.final_output.items():
                    lines.append(str(item[0]) + ' ' + str(item[1]) + '\n')
                file.writelines(lines)
            print("Hurray! Inverted Index Completed")
            return
        except Exception as e:
            logging.error("Error in Threaded server INVERTED INDEX")
            logging.error(str(e))
            raise e

    def shuffle_sort_word_count(self):
        self.reducer_input = {}
        for item in self.word_count_dict.items():
            for text in item[1].items():
                if text[0] not in self.reducer_input:
                    self.reducer_input[text[0]] = text[1]
                else:
                    self.reducer_input[text[0]].extend(text[1])

    def shuffle_sort_inverted_index(self):
        self.reducer_input = {}
        for item in self.word_count_dict.items():
            for text in item[1].items():
                if text[0] not in self.reducer_input:
                    self.reducer_input[text[0]] = text[1]
                else:
                    self.reducer_input[text[0]].extend(text[1])

    def combine_reducer_output_word_count(self):
        for key in self.reducer_output_dict.keys():
            for item in self.reducer_output_dict[key].items():
                self.final_output[item[0]] = item[1]

    def combine_reducer_output_inverted_index(self):
        for key in self.reducer_output_dict.keys():
            for item in self.reducer_output_dict[key].items():
                self.final_output[item[0]] = item[1]

    def run_mapred(self, input_data, map_fn, reduce_fn, output_location):

        def split(a, n):
            k, m = divmod(len(a), n)
            return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i
                    in range(n))

        if map_fn == 'word_count' and reduce_fn == 'word_count':
            with open(input_data, encoding="utf-8") as input_file:
                equal_parts = split(input_file.read(), self.count_mapper)

            part_queue = []
            for part in equal_parts:
                part_queue.append(part)

            start_new_thread(self.threaded_server_word_count, (
                part_queue, output_location,
            ))

            ps_futures = None
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.count_mapper
            ) as tpe:

                for i in range(self.count_mapper):
                    ps_futures = tpe.submit(
                        self.threaded_mapper,
                        [
                            socket.gethostbyname(socket.gethostname()),
                            self.mapper_port,
                            self.count_reducer,
                            map_fn,
                        ],
                    )

            while self.mappers_completed != self.count_mapper:
                continue

            if self.mappers_completed == self.count_mapper:
                self.shuffle_sort_word_count()
                self.shuffle_finished = True

            for item in self.reducer_input.items():
                with open(f'word_count/shuffle_sort/reducer_{item[0]}.txt', 'w+') as file:
                    lines = []
                    for block in self.reducer_input[item[0]]:
                        lines.append(str(block[0]) + ' ' + str(block[1]) + '\n')
                    file.writelines(lines)

            ps_futures = None
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.count_reducer
            ) as tpe:
                for i in range(self.count_reducer):
                    ps_futures = tpe.submit(
                        self.threaded_reducer,
                        [
                            socket.gethostbyname(socket.gethostname()),
                            self.reducer_port,
                            self.count_reducer,
                            map_fn,
                        ],
                    )
            print("WC COMPLETED")
        elif map_fn == "inverted_index" and reduce_fn == "inverted_index":
            inverted_filenames = []
            for (_, __, filenames) in os.walk(input_data):
                inverted_filenames.extend(filenames)

            inverted_index_dict = {}

            def clean_text(text):
                clean_text = ""
                text = text.lower()
                words = text.split(" ")
                for i in range(len(words)):
                    if words[i].isalnum() and i == 0:
                        clean_text = words[i]
                    elif words[i].isalnum() and len(words):
                        clean_text = clean_text + " " + words[i]
                return clean_text

            for filename in inverted_filenames:
                with open(input_data + '/' + filename, 'r') as file:
                    inverted_index_dict[filename] = file.read()
                    with open('clean_books/' + filename, 'w+') as file1:
                        file1.write(clean_text(inverted_index_dict[filename]))

            inverted_index_mapper_input = {}

            file_count = 0
            for filename in inverted_index_dict.keys():
                if not inverted_index_mapper_input.get(file_count % self.count_mapper):
                    inverted_index_mapper_input[file_count % self.count_mapper] = {
                        filename: inverted_index_dict[filename]
                    }
                else:
                    inverted_index_mapper_input[file_count % self.count_mapper][filename] = inverted_index_dict[
                        filename]
                file_count += 1

            start_new_thread(self.threaded_server_inverted_index, (inverted_index_mapper_input, output_location,))

            ps_futures = None
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.count_mapper
            ) as tpe:

                for i in range(self.count_mapper):
                    ps_futures = tpe.submit(
                        self.threaded_mapper,
                        [
                            socket.gethostbyname(socket.gethostname()),
                            self.mapper_port,
                            self.count_reducer,
                            map_fn,
                        ],
                    )
            while self.mappers_completed != self.count_mapper:
                continue

            if self.mappers_completed == self.count_mapper:
                self.shuffle_sort_inverted_index()
                self.shuffle_finished = True

            ps_futures = None
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.count_reducer
            ) as tpe:
                for i in range(self.count_reducer):
                    ps_futures = tpe.submit(
                        self.threaded_reducer,
                        [
                            socket.gethostbyname(socket.gethostname()),
                            self.reducer_port,
                            self.count_reducer,
                            map_fn,
                        ],
                    )
            print("INVERTED INDEX COMPLETED")
        else:
            raise Exception("Invalid Operation")




mr = MapReduce()
@app.route('/init')
def init_cluster():
    try:
        params = request.args
        mr.init_cluster(
            int(params.get("count_mapper")),
            int(params.get("count_reducer"))
        )
        return {"message": "Initiated Cluster"}, 200
    except requests.exceptions.HTTPError as err:
        return err.response.text, err.response.status_code


@app.route('/mapreduce')
def run_mapred():
    try:
        params = request.args
        mr.run_mapred(
            params.get("file_path"),
            params.get("map_func_type"),
            params.get("reduce_func_type"),
            params.get("output_file_path"))
        return {"message": "Processed Map Reduce"}, 200
    except requests.exceptions.HTTPError as err:
        return err.response.text, err.response.status_code


if __name__ == '__main__':
    app.run(host='localhost', port=8000, debug=True)
