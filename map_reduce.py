import requests
import logging
import master


class MapReduceLibrary:
    def __init__(self):
        self.initiated = False
        self.mr = master.MapReduce()

    def init_cluster(
            self,
            count_mapper,
            count_reducer
    ):
        """
        Initiates Map Reduce Cluster
        :param count_mapper: integer
        :param count_reducer: integer
        :return: string
        """
        try:
            self.initiated = True
            response = requests.get(
                'http://localhost:8000//init',
                params={
                    "count_mapper": count_mapper,
                    "count_reducer": count_reducer
                },
                headers={'content-type': 'application/json'}
            )
            return response
        except Exception as e:
            logging.error(str(e))
            raise e

    def run_mapred(
            self,
            file_path,
            map_func_type,
            reduce_func_type,
            output_file_path
    ):
        """
        Starts map reduce
        :param file_path: string
        :param map_func_type: string (word_count, inverted_index)
        :param reduce_func_type: string (word_count, inverted_index)
        :param output_file_path: string
        :return: string
        """

        try:
            if not self.initiated:
                raise Exception("Init cluster not called first")

            if map_func_type != reduce_func_type:
                raise Exception("Map and Reduce function type should be same")

            if map_func_type == reduce_func_type == 'word_count' or\
                    map_func_type == reduce_func_type == 'inverted_index':
                response = requests.get(
                    'http://localhost:8000/mapreduce',
                    params={
                        "file_path": file_path,
                        "map_func_type": map_func_type,
                        "reduce_func_type": reduce_func_type,
                        "output_file_path": output_file_path
                    },
                    headers={
                        'content-type': 'application/json'
                    }
                )
                return response
            else:
                return "Unknown Map Reduce Type", 400
        except Exception as e:
            logging.error(str(e))
            raise e











