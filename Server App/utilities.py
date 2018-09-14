import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np


def get_maximum_cluster_id_from_backbone_index_table(info_db):
    """
    This function returns the last/maximum known idx (cluster_id) that is in the
    backbone_index table

    Input: 'info_db' - dictionary containing the database parameters needed
                       for creating a connection; the dictionary is the one
                       given in the configuration file
    """
    # create database connection
    db_connection = create_database_connection(info_db)
    db_cursor = db_connection.cursor()

    # get the latest(maximum) cluster_id from the backbone_index table
    # we need this to let Dedupe know from what number to start counting the cluster
    db_cursor.execute("SELECT MAX(idx) FROM backbone_index")

    # fetchall() returns a list of dictionaries containing all the rows received 
    # from the SELECT query since we only get one row, we will look for the 
    # last/maximum cluster_id into the dictionary at row 0, having the key 'max'
    cursor_result_max_query = db_cursor.fetchall()
    last_cluster_id = cursor_result_max_query[0]['max'] if cursor_result_max_query[0]['max'] else 0

    # close the database connection 
    db_cursor.close()
    db_connection.close()

    return last_cluster_id


def extract_rows_by_jurisdiction_from_table_and_return_as_df(info_db, table_name, field_value):
    """
    This function extracts all the rows from 'table_name', where the field 'jurisdiction' is equal to
    the 'field_value' parameter and returns the extracted rows as a pandas dataframe. 
    Since the 'field_value' parameter in this case is a string, it should be passed to the function
    as a string representation, i.e., if the field value is "uk" it should be given as "'uk'",
    because we need the single quotes when creting the SELECT query.
    One can do that with 'repr' python function

    Input: 'info_db' - dictionary containing the database parameters needed
                       for creating a connection; the dictionary is the one
                       given in the configuration file
           'table__name' - string object containing the table name from
                                where the query will extract rows
           'field_value' - string representation object that specifies on which jurisdiction 
                            the query is based on. It is used in the WHERE clause and that's why
                            it needs the string to be enclosed in quotes.
                            E.g.: SELECT id FROM table_name WHERE jurisdiction = field_value
                            and field_value = repr("uk")
    """
    # create database connection
    db_connection = create_database_connection(info_db)
    db_cursor = db_connection.cursor()

    select_statement = sql_statement_for_selecting_companies_by_a_given_field(
        "*",
        'jurisdiction',
        field_value,
        table_name)
    db_cursor.execute(select_statement)

    resulted_dict = db_cursor.fetchall()

    # close the database connection 
    db_cursor.close()
    db_connection.close()

    return pd.DataFrame(resulted_dict, dtype='object')


def get_all_table_names_from_schema(info_db, table_schema_name):
    """
    This function returns all the names of tables from a given schema name
    The schema name is usually 'public'
    The table names that are returned are the one that start
    with 'bi_', because these are tables from the (b)ackbone (i)ndex project

    Input: 'info_db' - dictionary containing the database parameters needed
                       for creating a connection; the dictionary is the one
                       given in the configuration file
           'table_schema_name' - string object containing the schema name
                                where the query will look for table names
    """
    # create database connection
    db_connection = create_database_connection(info_db)
    db_cursor = db_connection.cursor()

    # Get the table names on which to run the SELECT query
    # Usually, for us, 'table_schema_name' will be 'public'
    db_cursor.execute(
        "SELECT table_name from information_schema.tables where table_schema = " + repr(table_schema_name.split()[0]))

    table_names = []
    for row in db_cursor:
        # we do the if statement because all the tables that contain the datasets from providers have
        # at the beginning of their names 'bi_' which stands for (b)ackbone (i)ndex
        if row['table_name'].find('bi_') == 0:
            table_names.append(row['table_name'])

    # close the database connection 
    db_cursor.close()
    db_connection.close()

    return table_names


def sql_statement_for_creating_new_table_with_fk_on_cluster_id(column_names_and_datatypes, table_name):
    """
    This function returns a string that contains a SQL statement
    The SQL statement is a CREATE statement, that creates a new table and enforces a foreign key
    constraint on the 'cluster_id' column which references the primary key (idx) from the backbone_index table
    The new table's name will be the value given in the 'table_name' parameter
    and the column names and their datatypes will be the ones from the 'column_names_and_datatypes'

    Input: 'column_names_and_dataypes' - dictionary having as keys the column names and
                                        the values are the column datatypes
           'table_name' - string containing the name of the new table
    """
    create_table_sql_statement = "CREATE TABLE " + table_name.split()[0] + " (company_id SERIAL PRIMARY KEY"

    for k, v in column_names_and_datatypes.items():
        create_table_sql_statement += ","
        create_table_sql_statement += k
        create_table_sql_statement += " "
        create_table_sql_statement += v

    return create_table_sql_statement + """, CONSTRAINT cluster_id FOREIGN KEY (cluster_id)
            REFERENCES public.backbone_index (idx) MATCH SIMPLE
            ON UPDATE NO ACTION
            ON DELETE NO ACTION)"""


def sql_statement_for_copying_values_from_file(columns, table_name):
    """
    This function returns a string that contains a SQL statement
    The SQL statement is a COPY statement, that will copy values from csv file
    and insert them into a table

    Input: 'columns' - list of string objects, where the strings are names of columns
           'table_name' - string containing the name of the table where values will
                         be inserted (copied)
    """
    sql_copy_statement = "COPY " + table_name.split()[0] + " ("

    sql_copy_statement += ','.join(columns)

    return sql_copy_statement + ") FROM STDIN CSV HEADER"


def get_names_of_columns_that_are_numeric_but_start_with_0(csv_file_name):
    """
    This function returns a list containing all the column names in the csv file,
    whose values are numeric (only digits), but have some values that start with 0 (zero).
    
    E.g.: postCode column has postcodes that start with zero (0273), but pandas will read
    this column as a float and eliminate the starting 0s. We don't want this behaviour, so
    we will force pandas to read the column as 'object' (string), but first we need to
    find that postCode column has these types of values, that start with 0s

    Input: 'csv_file_name' - the name of the csv file in which we search for
                            columns that are numeric, but have values that start with 0

    """
    # read all the columns of the csv file as being string (object)
    df = pd.read_csv(csv_file_name, dtype=object)

    # get the names of the columns
    column_names = df.columns.values

    columns_starting_with_0_and_are_numeric = []
    for column_name in column_names:
        # we use a RegEx which states that the string should start with 0 should have one or more extra digits 
        if (df[column_name].str.contains('^0[0-9]+$', regex=True)).sum() != 0:
            columns_starting_with_0_and_are_numeric.append(column_name)

    return columns_starting_with_0_and_are_numeric


def get_columns_and_their_datatypes(df):
    """
    This function receives as input a pandas dataframe and will return
    a dictionary having as keys the names of columns in the dataframe
    and the values will be the corresponding datatypes that work
    for SQL

    Input: 'df' - pandas dataframe 
    """
    column_datatypes = {}

    for k, v in dict(df.dtypes).items():
        if v == 'int64':
            column_datatypes[k] = 'INT'
        elif v == 'float64':
            column_datatypes[k] = 'FLOAT'
        else:
            column_datatypes[k] = 'VARCHAR(500)'

    return column_datatypes


def get_statements_for_creating_table_with_fk_and_for_copying_data_into_table_from_csv(dataset_file_name, table_name):
    """
    This function uses returns a tuple made of 2 SQL statements: one is for creating a new table (which will have a 
    FK constraint on the 'cluster_id' column and references the 'idx' column from backbone_index table) and the other
    SQL statement is for copying values from a csv file into a table

    Input: 'dataset_file_name' - name of the csv file which contains the dataset and also has the 'cluster_id' column
           'table_name' - name of the table where the dataset will be inserted
    """
    # get the column names of numeric columns that have values starting with digit 0
    numeric_col_names_that_start_with_0 = get_names_of_columns_that_are_numeric_but_start_with_0(dataset_file_name)

    # put all these column names in a dictionary, where the keys are the column names and
    # their values will be 'object'
    data_types = {}
    for column in numeric_col_names_that_start_with_0:
        data_types[column] = object

    # read the csv and force the numeric columns that have values starting with 0 to be read as object (string)
    result_df = pd.read_csv(dataset_file_name, dtype=data_types)

    # get a dictionary where the keys are column names and their values are the SQL corresponding datatypes
    column_names_and_datatypes = get_columns_and_their_datatypes(result_df)

    # get the SQL statement for creating a new table with the given fields and which has a foreign key on
    # 'cluster_id' column
    create_table_stmt = sql_statement_for_creating_new_table_with_fk_on_cluster_id(column_names_and_datatypes,
                                                                                   table_name)

    # get the SQL statement for copying values from a csv file into a table
    copy_into_table_stmt = sql_statement_for_copying_values_from_file(result_df.columns, table_name)

    return create_table_stmt, copy_into_table_stmt


def create_database_connection(info_db):
    """
    This function creates a database connection, sets the autocommit to 'True' and returns the created connection

    Input: 'info_db' - dictionary containing the database parameters needed
                       for creating a connection; the dictionary is the one
                       given in the configuration file
    """

    connection = psycopg2.connect(
        database=info_db['database_name'],
        user=info_db['username'],
        password=info_db['password'],
        host=info_db['host'],
        port=info_db['port'],
        cursor_factory=psycopg2.extras.RealDictCursor
    )

    connection.autocommit = True

    return connection


def sql_statement_for_selecting_companies_by_a_given_field(columns, field, value, table_name):
    """
    This function returns a string which contains a SQL query.
    The query selects the given columns from the given table, where
    a field matches a certain value

    Input: 'columns' - list of strings, where each string is the name of a column;
                        it can also be just a string made of the asterisk character '*',
                        which means that we want all the columns to be returned
           'table_name' - string containing the name of the table that will be querried
           'field' - string containing the name of the field on which we focus the querry
           'value' - string containing the value of the field on which we do the querry 
    """
    if columns == "*":
        columns_statement = columns
    else:
        columns_statement = ','.join(columns)

    select_sql_statement = "SELECT " + columns_statement + " FROM " + table_name.split()[
        0] + " WHERE " + field + "=" + value

    return select_sql_statement


def insert_new_cluster_ids_into_backbone_index_table(info_db, output_file_1, output_file_2, last_cluster_id):
    """
    This function will insert the new cluster_ids that were created by Dedupe into the backbone_index table

    Input: 'info_db' - dictionary containing the database parameters needed
                       for creating a connection; the dictionary is the one
                       given in the configuration file
            'output_file_1' - name of the 1st csv output file that resulted from dedupe
            'output_file_2' - name of the 2nd csv output file that resulted from dedupe;
                            if this csv file contains examples that were previously extracted
                            from the database, this parameter will be 'None'
            'last_cluster_id' - the last known cluster_id (the maximum one that currently exists
                                in the backbone_index table)
            
    """
    df_output_1 = pd.read_csv(output_file_1)

    # creating an empty set
    cluster_ids = set()

    if output_file_2:
        df_output_2 = pd.read_csv(output_file_2)

        # get all the cluster_ids and keep only 1 value of each one
        cluster_ids = set(pd.concat([df_output_1["cluster_id"], df_output_2["cluster_id"]], axis=0))

        # keep only the new cluster_ids, i.e., the ones that are greater than the last known cluster_id
        cluster_ids = [c_id for c_id in cluster_ids if c_id > last_cluster_id]
    else:
        cluster_ids = list(df_output_1[df_output_1['cluster_id'] > last_cluster_id].cluster_id)

    db_connection = create_database_connection(info_db)
    db_cursor = db_connection.cursor()

    cluster_ids_as_list_of_tuples = [tuple([x]) for x in cluster_ids]
    db_cursor.executemany("INSERT INTO backbone_index VALUES (%s)", cluster_ids_as_list_of_tuples)

    db_cursor.close()
    db_connection.close()


def create_table_and_insert_dataset_resulted_from_dedupe(info_db, provider_name, file_name):
    """
    This function creates a new table (which will have a FK constraint on the 'cluster_id' column 
    and references the 'idx' column from backbone_index table) and inserts the values from
    the given csv file into the created table. The table's name will be composed of
    the prefix 'bi_' (which stands for (b)ackbone (i)ndex) and the name of the provider (the name
    of the company that gave us the dataset)
    The dataset will be made of the dataset given by the provider + 2 extra columns: cluster_id and
    link_score

    Input:  'info_db' - dictionary containing the database parameters needed
                       for creating a connection; the dictionary is the one
                       given in the configuration file
            'provider_name' - string containing the name of the company that
                            gave us the dataset
            'file_name' - the name of the csv file where the dataset is stored
    """
    db_connection = create_database_connection(info_db)
    db_cursor = db_connection.cursor()

    provider_table_name = 'bi_' + provider_name

    create_stmt, copy_stmt = get_statements_for_creating_table_with_fk_and_for_copying_data_into_table_from_csv(
        file_name, provider_table_name)

    db_cursor.execute("DROP TABLE IF EXISTS " + provider_table_name)
    db_cursor.execute(create_stmt)

    # copy the rows from the csv file into the table in the database
    with open(file_name, 'r') as f:
        db_cursor.copy_expert(copy_stmt, f)

    db_cursor.close()
    db_connection.close()


def update_cluster_ids_of_output_file_1(output_file_1, output_file_2, input_file_2):
    """
    This function updates the cluster_ids given by Dedupe to the first input dataset,
    in the case when the user only gave 1 new dataset as input and the other dataset
    was extracted from thew database. In this case, the examples extracted from the
    database already have a cluster_id (backbone_index). After Dedupe is run, it
    creates 2 output files which are exactly like the input ones, just that
    they have 2 additional fields: cluster_id and link_score, and these cluster_ids
    are unique compared to the ones that already exist in the database. So, to the
    clusters that contain examples from the second dataset (the one that was extracted
    from the DB), since they already had cluster_ids assigned to them, we want to
    reassign those old cluster_ids. And then, for the examples that are in their
    own cluster, we want to update their cluster_ids, so that when we put then
    new cluster_ids in the backbone_index table, they will be consecutive,
    and no value is skipped.

    E.g.: if the first four examples from output file 1 have the cluster_ids
    100,101,102,103 and the first two examples are clusters that also have an
    example from the database with cluster_ids 14 and 55, and the last two
    examples are individual clusters the output will be: the first two
    examples will have cluster_ids 14 and 55 and the last two examples
    will have cluster_ids 100 and 101

    The function iterates over the rows of the 1st output file (after they
    were sorted by cluster_id - this way the first 'n' cluster_ids belong
    to clusters that also have an example from the database and the next
    clusters are individual clusters) --> We change the cluster_ids of
    the first 'n' clusters and then we update the cluster_ids of the
    remaining individual clusters

    Input - output_file_1 - string containing the name of the output file
                            belonging to the first input dataset
            output_file_2 - string containing the name of the output file
                            belonging to the second input dataset
            input_file_1 - string containing the name of the input file,
                            whose examples were extracted from the database
    """
    # read the csv files into pandas dataframes, where all cells are treated as
    # objects (strings)
    df_output_1 = pd.read_csv(output_file_1, dtype=object)
    df_output_2 = pd.read_csv(output_file_2, dtype=object)
    df_input_2 = pd.read_csv(input_file_2, dtype=object)

    # convert the 'cluster_id' column datatype to 'int64' to all dataframes
    df_output_1['cluster_id'] = df_output_1.cluster_id.astype('int64')
    df_output_2['cluster_id'] = df_output_2.cluster_id.astype('int64')
    df_input_2['cluster_id_from_db'] = df_input_2.cluster_id_from_db.astype('int64')

    # sort by 'cluster_id' the 1st dataframe's rows
    df_output_1 = df_output_1.sort_values(by=['cluster_id']).reset_index(drop=True)

    # number of clusters found by Dedupe (clusters that contain 2 examples - 1 from the 1st dataset
    # and 1 from the 2nd input dataset)
    nr_of_clusters_found = len(set(df_output_1['cluster_id']).intersection(set(df_output_2['cluster_id'])))
    nr_of_changed_cluster_ids = 0

    # give to all the existing clusters (of 2 elements) the cluster_id belonging to the example 
    # that was extracted already from the database
    for i in range(0, nr_of_clusters_found):
        cluster_id_in_output_1 = df_output_1.iloc[i]['cluster_id']

        row_index_of_cluster_in_output_2 = np.where(df_output_2['cluster_id'] == cluster_id_in_output_1)[0][0]

        cluster_id_in_input_2 = df_input_2.iloc[row_index_of_cluster_in_output_2]['cluster_id_from_db']

        df_output_1.at[i, 'cluster_id'] = cluster_id_in_input_2
        nr_of_changed_cluster_ids += 1

    # update the cluster_id of all individual clusters
    for i in range(nr_of_clusters_found, len(df_output_1)):
        df_output_1.at[i, 'cluster_id'] = df_output_1.iloc[i]['cluster_id'] - nr_of_changed_cluster_ids

    df_output_1.to_csv(output_file_1, index=False)