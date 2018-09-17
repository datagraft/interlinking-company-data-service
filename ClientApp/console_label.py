import pickle
import simplejson as json
from io import BytesIO


class ConsoleLabel:
    training_file_name = "training_file.json"

    def __init__(self, uncertain_pairs_stream):
        self.labeled_examples = {'distinct': [], 'match': []}

        buff = BytesIO(uncertain_pairs_stream)

        self.uncertain_pairs = pickle.load(buff)

    def get_uncertain_pair(self):

        record_pair = self.uncertain_pairs.pop()

        self.print_pair(record_pair)

        return record_pair

    def print_pair(self, record_pair):

        for pair in record_pair:
            for example in pair:
                for field in example:
                    line = "%s : %s" % (field, example[field])
                    print(line)
                print("\n")

        n_match = len(self.labeled_examples['match'])
        n_distinct = len(self.labeled_examples['distinct'])
        print("{0}/10 positive, {1}/10 negative".format(n_match, n_distinct))

        print('Do these records refer to the same thing?')
        print('(y)es / (n)o / (u)nsure / (f)inished')

    def label_record_pair(self, label, record_pair):

        if label == 'y':
            self.labeled_examples['match'].append(record_pair.pop())
        elif label == 'n':
            self.labeled_examples['distinct'].append(record_pair.pop())
        elif label == 'u':
            record_pair.pop()
        elif label == 'f':
            print('Finished labeling')
            self.__create_uncertain_pairs_file()

    def to_json(self, python_object):

        if isinstance(python_object, tuple):
            python_object = {'__class__': 'tuple',
                             '__value__': list(python_object)}
        else:
            raise TypeError(repr(python_object) + ' is not JSON serializable')

        return python_object

    def __create_uncertain_pairs_file(self):

        with open(self.training_file_name, "w") as fjson:
            json.dump(
                self.labeled_examples,
                fjson,
                default=self.to_json,
                tuple_as_array=False
            )
