import utilities
import simplejson as json
import pandas as pd
import pickle

from werkzeug.utils import secure_filename


class Backbone:
    # this is how the configuration file given by the user must be named,
    # otherwise, this class won't know/find the configuration file
    configuration_file_name = 'configuration_file_bs.json'

    # this is how the configuration file that is used by dedupe must be named,
    # otherwise, dedupe won't know/find its configuration file
    configuration_file_name_for_dedupe = 'configuration_file_dedupe.json'

    # in case there is no second file given in the configuration file, we will have to create a temporary
    # one containing the second dataset for dedupe
    tmp_file_2_name = 'tmp_input_file_2.csv'

    # jupyter notebook file name, which contains the Dedupe algorithm
    dedupe_jupyter_nb_name = 'dedupe_interlinking_data.ipynb'

    # if the user creates the training file, that file will be named like it's written on the next line
    training_file_name_created_by_client = 'training_file.json'

    def __init__(self):
        self.__set_data_from_config_file()
        self.__set_input_file_1_name()
        self.__set_input_file_2_name()
        self.__set_training_file_name()
        self.__set_settings_file_name()
        self.__set_last_cluster_id_in_db()
        self.__create_dedupe_configuration_file()
        self.__set_jupyter_notebook_data()
        self.__set_output_file_1_name()
        self.__set_output_file_2_name()

    def __set_jupyter_notebook_data(self):
        """
        Reads the JSON file where the Dedupe algorithm is located, and stores it in an instance variable
        """
        with open(self.dedupe_jupyter_nb_name, 'r') as content_file:
            self.jupyter_notebook_data = json.loads(content_file.read())

    def __set_data_from_config_file(self):
        """
        Reads the data from the configuration file, and stores it in an instance variable
        """
        with open(self.configuration_file_name, 'r') as config_file:
            self.data_from_config_file = json.load(config_file)

    def __set_input_file_1_name(self):
        """
        Set the name of the 1st input file that will be passed to Dedupe. 
        'secure_filename' function is used to avoid malicious cases, i.e.,
        when the file name given by the user is '../../../../file'
        """
        self.input_file_1 = secure_filename(self.data_from_config_file['input_file_1'])

    def __set_input_file_2_name(self):
        """
        Set the name of the 2nd input dataset that will be passed to Dedupe: 
        if the name is not correctly given in the configuration file, i.e., 
        it is null or it's not a '.csv' file, the second dataset the will be given 
        to Dedupe will be a temporary file, that will contain rows extracted by 
        the given 'jurisdiction', from the tables that are currently in the database
        """
        if self.data_from_config_file.get('input_file_2') and \
                self.data_from_config_file.get('input_file_2').find(".csv") == len(
            self.data_from_config_file.get('input_file_2')) - 4:

            self.input_file_2 = secure_filename(self.data_from_config_file.get('input_file_2'))
        else:
            self.input_file_2 = self.tmp_file_2_name

    def __set_training_file_name(self):
        """
        Set the training file name that will be passed to Dedupe. If the user created a
        training file on the client side, the name of that file will be the one
        stored in the static variable 'training_file_name_created_by_client'. Otherwise,
        the name will be the one specified in the configuration file
        """
        if self.data_from_config_file['training']['create_training_file_by_client']:
            self.training_file_name = self.training_file_name_created_by_client
        else:
            self.training_file_name = secure_filename(self.data_from_config_file['training']['training_file'])

    def __set_settings_file_name(self):
        """
        Set the name of the settings file if it is given by the user. Otherwise, set it to 'None'
        """
        if self.data_from_config_file['training']['settings_file']:
            self.settings_file_name = secure_filename(self.data_from_config_file['training']['settings_file'])
        else:
            self.settings_file_name = None

    def __set_output_file_1_name(self):
        """
        Set the name of the first output file, i.e., the one that is created from the first input dataset
        The names of the output files are just the name of the input files having 'output_' as prefix
        """
        self.output_file_1 = "output_" + self.input_file_1

    def __set_output_file_2_name(self):
        """
        Set the name of the second output file.
        If the user didn't give a name for the 2nd input file, then the 2nd output file name will be
        the concatenation of 'output_' with the name of the temporary file that will be created
        """
        self.output_file_2 = "output_" + self.data_from_config_file[
            'input_file_2'] if not self.is_tmp_file_used() else "output_" + self.tmp_file_2_name

    def __set_last_cluster_id_in_db(self):
        """
        Set the last/maximum cluster_id that we currently have in the 'backbone_index' table.
        We need to pass this value to Dedupe, because when the Dedupe algorithm will create new
        clusters, their ids should continue from the last one that is in the 'backbone_index' table
        """
        self.last_cluster_id_in_db = utilities.get_maximum_cluster_id_from_backbone_index_table(
            self.data_from_config_file['database_config'])

    def is_tmp_file_used(self):
        """
        Function which returns a Boolean value whose is False if the user has provided a second
        input dataset, and is True otherwise (because a temporary file with rows extracted by
        'jurisidiction was created) 
        """
        return self.input_file_2 == self.tmp_file_2_name

    def __create_dedupe_configuration_file(self):
        """
        This function creates the configuration file that the Dedupe algorithm will use.
        It will copy the initial configuration file provided by the user, and will modify 
        some of its parts or add other fileds, to correspond to what Dedupe expects
        """
        # copy the initial configuration file
        self.config_data_for_dedupe = self.data_from_config_file.copy()

        # delete configuration parameters that Dedupe's configuration file won't need
        self.config_data_for_dedupe.pop('provider_1_name', None)
        self.config_data_for_dedupe.pop('provider_2_name', None)
        self.config_data_for_dedupe.pop('jurisdiction', None)

        # modify the existing fields/add new ones
        self.config_data_for_dedupe['input_file_1'] = self.input_file_1
        self.config_data_for_dedupe['input_file_2'] = self.input_file_2
        self.config_data_for_dedupe['training']['training_file'] = self.training_file_name
        self.config_data_for_dedupe['training']['settings_file'] = self.settings_file_name
        self.config_data_for_dedupe['last_cluster_id'] = self.last_cluster_id_in_db

        print(self.config_data_for_dedupe)
        
        # write Dedupe's configuration file that we've made to a JSON file 
        with open(self.configuration_file_name_for_dedupe, "w") as config_file_for_dedupe:
            json.dump(self.config_data_for_dedupe, config_file_for_dedupe)

    def extract_data_from_db_and_create_second_input_dataset(self):
        """
        This function queries all the tables from the database that store datasets
        from providers and extracts all the rows that have the given 'jurisdiction'.
        Then, it merges all the extracted rows into a single table, keeping only the
        fields that are common across the resulted rows. After this table is created
        all of its content is written in a csv file
        """

        info_db = self.data_from_config_file['database_config']

        # get the names of all the tables that contain datasets from different providers   
        table_names = utilities.get_all_table_names_from_schema(info_db, 'public')

        # list of dataframes; each dataframe is composed of all the rows that are extracted 
        # from a table with the SELECT query (they are extracted based on their jurisdiction)
        list_of_dataframes_resulted_from_select_query = []
        for table_name in table_names:
            df = utilities.extract_rows_by_jurisdiction_from_table_and_return_as_df(info_db, table_name, repr(
                self.data_from_config_file['jurisdiction']))

            list_of_dataframes_resulted_from_select_query.append(df)

        column_names_of_each_dataframe = []
        for df in list_of_dataframes_resulted_from_select_query:
            column_names_of_each_dataframe.append(set(df.columns.values))

        # we'll keep only the common columns of the rows previously extracted from the tables
        common_columns_across_dataframes = set.intersection(*column_names_of_each_dataframe)

        # new list of dataframes, where each dataframe was reduced to only having the common columns
        list_of_dataframes_resulted_from_select_query_with_common_fields = []
        for df in list_of_dataframes_resulted_from_select_query:
            df = df[list(common_columns_across_dataframes)]
            list_of_dataframes_resulted_from_select_query_with_common_fields.append(df)

        # merge/concatenate all dataframes that we have so far into a big one 
        big_df = pd.concat(list_of_dataframes_resulted_from_select_query_with_common_fields, axis=0).reset_index(
            drop=True)

        # shuffle (rearrange in random order) the examples from the dataframe
        big_df = big_df.sample(frac=1).reset_index(drop=True)

        # Keep only one example from a cluster. This is neccessary because our Dedupe
        # algorithm makes one-to-one matches between the two input files, i.e., the clusters
        # that Dedupe creates contain maximum 2 elements, where one element is from input file 1
        # and the other is from input file 2. If we would have two or more examples from the same
        # cluster in this file, at most one would get matched and the remaining ones will be put
        # in their own clusters and later on inserted into the database AGAIN.
        big_df.drop_duplicates(subset=['cluster_id'], inplace=True)

        # rename the 'cluster_id' column to 'cluster_id_from_db', so that
        # when Dedupe creates the output file and adds  the columns 'cluster_id'
        # and 'link_score', there would not be columns that have the same name
        big_df.rename(columns={'cluster_id': 'cluster_id_from_db', 'link_score': 'link_score_from_db'}, inplace=True)

        # write this big table to a csv file that dedupe will use as the 2nd input file
        big_df.to_csv(self.tmp_file_2_name, index=False)

    def execute_jupyter_notebook_cells(self, idx_first_cell, idx_last_cell=None):
        """
        This function executes cells of the Jupyter notebook where the Dedupe algorithm is.
        It starts executing the cell at index 'idx_first_cell' and the last executed cell
        is the one BEFORE the cell at index 'idx_last_cell'. Bear in mind that the first
        cell has index 0. If the user doesn't specify a value for 'idx_last_cell', then
        all the cells starting from 'idx_first_cell' are executed.

        :param idx_first_cell: int data type
        :param idx_last_cell: int data type
        """
        if idx_last_cell is None:
            last_cell_to_execute = len(self.jupyter_notebook_data["cells"])
        else:
            last_cell_to_execute = idx_last_cell
        # execute each cell from the notebook
        for idx_cell in range(idx_first_cell, last_cell_to_execute):
            # get all (L)ines (O)f (C)ode from the current cell, merge them all 
            # into a long LOC and execute it
            # N.B.: every LOC is terminated with a new line character '\n' --> 
            # concatenating them won't produce an error
            result = self.jupyter_notebook_data["cells"][idx_cell]["source"]
            exec(''.join(result), locals())

    def search_field_in_db_by_value_and_return_serialized_result(self, field, value):
        """
        This function calls the 'search_field_in_db_by_value' from the 'utilities' module and
        returns the serialized version of the dictionary returned by the function.

        :param  field: string object containing the field name
        :param  value: string object that contains the value that we want to have/find in that field
        """
        return pickle.dumps(
            utilities.search_field_in_db_by_value(self.data_from_config_file['database_config'], field, value))
