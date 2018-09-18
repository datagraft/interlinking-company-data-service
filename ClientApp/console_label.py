import pickle
import simplejson as json
from io import BytesIO


class ConsoleLabel:
    training_file_name = "training_file.json"

    def __init__(self, uncertain_pairs_stream):
        """
            Constructor
        """
        self.labeled_examples = {'distinct': [], 'match': []}

        buff = BytesIO(uncertain_pairs_stream)

        self.uncertain_pairs = pickle.load(buff)

    def get_uncertain_pair(self):
        """
            This function returns an uncertain pair from a list of uncertain pairs.

            :return: a list object containing two dictionaries which represents the examples considered by library
                    to be uncertain to label. The library cannot figure out if these examples are the same or not, so it cannot
                    give a certain label and therefore ask the user to give one.
        """

        record_pair = self.uncertain_pairs.pop()

        self.print_pair(record_pair)

        return record_pair

    def print_pair(self, record_pair):
        """
            This function prints the uncertain pair in a pretty way.

            :param record_pair:  a tuple object containing two dictionaries which represents the examples considered
                                by library to be uncertain to label.
        """
        
        for pair_element in record_pair:
            for field in pair_element:
                line = "%s : %s" % (field, pair_element[field])
                print(line)
            print("\n")

        n_match = len(self.labeled_examples['match'])
        n_distinct = len(self.labeled_examples['distinct'])
        print("{0}/10 positive, {1}/10 negative".format(n_match, n_distinct))

        print('Do these records refer to the same thing?')
        print('(y)es / (n)o / (u)nsure / (f)inished\n')

    def label_record_pair(self, label, record_pair):
        """
            This function adds to an instance variable a record pair depending on its label. If the label is 'y'
            then the record pair it will be added to "match" category, if the label is 'n' to "distinct" category,
            if the label is 'u' then we ignore it and if the label is 'f' the labeling part is finished and a new
            training file is created.

            :param label: string object which can contain only the 'y', 'n', 'u', 'f' values.
            :param record_pair: a list object containing two dictionaries which represents the examples considered
                               by library to be uncertain to label.
        """

        if label == 'y':
            self.labeled_examples['match'].append(record_pair)
        elif label == 'n':
            self.labeled_examples['distinct'].append(record_pair)
        elif label == 'u':
            record_pair = ()
        elif label == 'f':
            print('Finished labeling')
            self.__create_uncertain_pairs_file()

    def to_json(self, python_object):
        """
            This function presents how to create the json file. This json file should have a specific
            template in our case.

            :param python_object: an object which will be written in the json file
            :return: the object which was written in the json file
        """

        if isinstance(python_object, tuple):
            python_object = {'__class__': 'tuple',
                             '__value__': list(python_object)}
        else:
            raise TypeError(repr(python_object) + ' is not JSON serializable')

        return python_object

    def __create_uncertain_pairs_file(self):
        """
            This function creates a json training file using the instance variable "labeled_examples".
        """

        with open(self.training_file_name, "w") as fjson:
            json.dump(
                self.labeled_examples,
                fjson,
                default=self.to_json,
                tuple_as_array=False
            )
