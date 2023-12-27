import logging


class Reducer:

    def __init__(self):
        self.output_dict = {}

    def word_count_reduce(self, key_value_pairs):
        try:
            logging.info("Word count reducer")
            for item in key_value_pairs:
                if item[0] not in self.output_dict:
                    self.output_dict[item[0]] = item[1]
                else:
                    self.output_dict[item[0]] += item[1]
            return self.output_dict
        except Exception as e:
            logging.error("Error in Word count reducer")
            logging.error(str(e))
            raise e

    def inverted_index_reduce(self, key_value_pairs_list):
        try:
            logging.info("Inverted Index reducer")
            inverted_index_dict = {}
            for item in key_value_pairs_list:

                word_file = item[0]
                value = item[1]

                if word_file not in inverted_index_dict:
                    inverted_index_dict[word_file] = value
                else:
                    inverted_index_dict[word_file] += value
            for pair in inverted_index_dict.items():
                key = pair[0]
                value = pair[1]
                word = key.split("-")[0]
                doc1 = key.split("-")[1]
                if word not in self.output_dict:
                    self.output_dict[word] = doc1 + ":" + str(value)
                else:
                    docs = self.output_dict[word]
                    doc1 = docs + "," + doc1 + ":" + str(value)
                    self.output_dict[word] = doc1

            return self.output_dict
        except Exception as e:
            logging.error("Error in Inverted Index reducer")
            logging.error(str(e))
            raise e
