import utilities
import simplejson as json
import pandas as pd

from werkzeug.utils import secure_filename


class Backbone:
    configuration_file_name = 'configuration_file_bs.json'
    configuration_file_name_for_dedupe = 'configuration_file_dedupe.json'

    # in case there is no second file given in the configuration file, we will have to create a temporary
    # one containing the second dataset for dedupe
    tmp_file_2_name = 'tmp_input_file_2.csv'

    # jupyter notebook file name, which contains the Dedupe algorithm
    dedupe_jupyter_nb_name = 'dedupe_linking_data.ipynb'

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

        # the names of the output files are just the name of the input files having 'output_' as prefix
        self.__set_output_file_1_name()
        self.__set_output_file_2_name()

    def __set_jupyter_notebook_data(self):
        # read the JSON file where the Dedupe algorithm is located
        with open(self.dedupe_jupyter_nb_name, 'r') as content_file:
            self.jupyter_notebook_data = json.loads(content_file.read())

    def __set_data_from_config_file(self):
        # read the data from the configuration file
        with open(self.configuration_file_name, 'r') as config_file:
            self.data_from_config_file = json.load(config_file)

    def __set_input_file_1_name(self):
        self.input_file_1 = secure_filename(self.data_from_config_file['input_file_1'])

    def __set_input_file_2_name(self):
        # second input dataset filename: if it is not given in the configuration file
        # we'll create a temporary file containing rows from the tables from the
        # database, that will be extracted by jurisdiction
        if self.data_from_config_file.get('input_file_2') and \
                self.data_from_config_file.get('input_file_2').find(".csv") == len(
            self.data_from_config_file.get('input_file_2')) - 4:

            self.input_file_2 = secure_filename(self.data_from_config_file.get('input_file_2'))
        else:
            self.input_file_2 = self.tmp_file_2_name

    def __set_training_file_name(self):
        if self.data_from_config_file['create_training_file_by_client']:
            self.training_file_name = self.training_file_name_created_by_client
        else:
            self.training_file_name = secure_filename(self.data_from_config_file['training_file'])

    def __set_settings_file_name(self):
        if self.data_from_config_file['settings_file']:
            self.settings_file_name = secure_filename(self.data_from_config_file['settings_file'])
        else:
            self.settings_file_name = None

    def __set_output_file_1_name(self):
        self.output_file_1 = "output_" + self.input_file_1

    def __set_output_file_2_name(self):
        # if the user didn't give the 2nd input file, then the 2nd output file name will be None
        self.output_file_2 = "output_" + self.data_from_config_file[
            'input_file_2'] if not self.is_tmp_file_used() else "output_" + self.tmp_file_2_name

    def __set_last_cluster_id_in_db(self):
        self.last_cluster_id_in_db = utilities.get_maximum_cluster_id_from_backbone_index_table(
            self.data_from_config_file['database_config'])

    def is_tmp_file_used(self):
        return self.input_file_2 == self.tmp_file_2_name

    def __create_dedupe_configuration_file(self):
        # create the dedupe configuration file; we'll copy the initial configuration file and modify it to 
        # correspond to what Dedupe expects
        self.config_data_for_dedupe = self.data_from_config_file.copy()

        # delete configuration parameters that Dedupe's configuration file won't need
        self.config_data_for_dedupe.pop('provider_1_name', None)
        self.config_data_for_dedupe.pop('provider_2_name', None)
        self.config_data_for_dedupe.pop('jurisdiction', None)

        self.config_data_for_dedupe['input_file_1'] = self.input_file_1
        self.config_data_for_dedupe['training_file'] = self.training_file_name
        self.config_data_for_dedupe['settings_file'] = self.settings_file_name

        if self.is_tmp_file_used():
            # write in Dedupe's configuration file the name of the file containing the second dataset
            self.config_data_for_dedupe['input_file_2'] = self.tmp_file_2_name

        self.config_data_for_dedupe['last_cluster_id'] = self.last_cluster_id_in_db
        # write Dedupe's configuration file that we've made so far to a JSON file 
        with open(self.configuration_file_name_for_dedupe, "w") as config_file_for_dedupe:
            json.dump(self.config_data_for_dedupe, config_file_for_dedupe)

    def extract_data_from_db_and_create_second_input_dataset(self):
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

        # leave only one example from a cluster
        big_df.drop_duplicates(subset=['cluster_id'], inplace=True)

        # rename the cluster_id column to 'cluster_id_from_db', so that
        # there will not be two 'cluster_id' columns in the output file
        big_df.rename(columns={'cluster_id': 'cluster_id_from_db', 'link_score': 'link_score_from_db'}, inplace=True)

        # write this big table to a csv file that dedupe will use as the 2nd input file
        big_df.to_csv(self.tmp_file_2_name, index=False)

    def execute_jupyter_notebook_cells(self, idx_first_cell, idx_last_cell=None):
        if idx_last_cell is None:
            last_cell_to_execute = len(self.jupyter_notebook_data["cells"])
        else:
            last_cell_to_execute = idx_last_cell
        # execute each cell from the notebook
        for idx_cell in range(idx_first_cell, last_cell_to_execute):
            # get all (L)ines (O)f (C)ode from the current cell, merge them all 
            # into a long LOC and execute it
            # N.B.: every LOC is terminated with a new line character '\n' --> 
            # merging them won't produce an error
            result = self.jupyter_notebook_data["cells"][idx_cell]["source"]
            exec(''.join(result))