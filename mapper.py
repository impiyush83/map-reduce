import logging


class Mapper:

    text = None

    def __init__(self):
        self.word_count_dict = {}
        self.inverted_index_dict = {}

    def word_count_mapper(self, text):
        try:
            logging.info("Word Count mapper")
            text = str(text).lower()
            clean_text = self.clean_text(text)
            for word in clean_text.split():
                if word not in self.word_count_dict:
                    self.word_count_dict[word] = 1
                else:
                    self.word_count_dict[word] += 1
            return self.word_count_dict
        except Exception as e:
            logging.error(str(e))
            raise e

    def inverted_index_mapper(self, file_content_dict):
        try:
            logging.info("Inverted Index mapper")
            self.inverted_index_dict = {}
            for item in file_content_dict.items():
                filename = item[0]
                text = item[1]
                text = str(text).lower()
                clean_text = self.clean_text(text)

                for word in clean_text.split(" "):
                    key = word + "-" + filename
                    if key not in self.inverted_index_dict:
                        self.inverted_index_dict[key] = 1
                    else:
                        self.inverted_index_dict[key] += 1
            return self.inverted_index_dict
        except Exception as e:
            logging.error(str(e))
            raise e

    @staticmethod
    def clean_text(text):
        try:
            logging.info("Cleaning Mapper Text")
            clean_text = ""
            words = text.split(" ")
            for i in range(len(words)):
                if words[i].isalnum() and i == 0:
                    clean_text = words[i]
                elif words[i].isalnum() and len(words):
                    clean_text = clean_text + " " + words[i]
            return clean_text
        except Exception as e:
            logging.error(str(e))
            raise e
