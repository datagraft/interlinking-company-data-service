import os

import utilities

from flask import Flask, flash, request, redirect, send_from_directory
from werkzeug.utils import secure_filename

from backbone import Backbone

app = Flask(__name__)
 

@app.route('/run_algorithm', methods=['POST'])
def run_algorithm():
    """
    This function represents the main algorithm of the service. It assumes all the
    neccessary files were uploaded by the user.
    The execution flow is the next one:
    1) Create Backbone object, which will create the configuration file for Dedupe
    2) If the user has not provided the 2nd input dataset, then a temporary file,
       that will contain rows extracted by 'jurisdiction' from the database, 
       will be created using the Backbone object
    3) Execute all the cells in the jupyter notebook
    4) If a temporary file was created update the given cluster_ids (read more about
       this in a comment below)
    5) Insert the new cluster_ids into the 'backbone_index' table
    6) Create table(s) in the database and insert the dataset(s) resulted from the
       Dedupe algorithm.
    7) Remove all the files that were used in the process, except for the configuration
       file provided by the user. We do not remove this file, because if the user
       would like to see some results, that are stored in the database, it will need
       to provide again the configuration file (since the system needs the database
       configuration data). So, we leave it there for convenience
    """

    # Backbone object that will do all the work
    backbone = Backbone()

    # run the backbone script file
    if backbone.is_tmp_file_used():
        backbone.extract_data_from_db_and_create_second_input_dataset()

    # execute all the cells in the Jupyter notebook
    backbone.execute_jupyter_notebook_cells(idx_first_cell=0)

    # if the 2nd dataset contained rows from the database, those examples from the 2nd dataset
    # already had assigned a cluster_id (backbone index), but when Dedupe created new clusters,
    # which were made of 1 example from the 1st dataset and one from the 2nd dataset,
    # it gave that cluster a unique cluster_id (one that did not exist in the backbone_index table).
    # So, we have to update the cluster_ids of those clusters with the cluster_ids that the examples 
    # from the 2nd dataset originally had.
    if backbone.is_tmp_file_used():
        utilities.update_cluster_ids_of_output_file_1(
            backbone.output_file_1,
            backbone.output_file_2,
            backbone.input_file_2
        )

    # insert the new cluster_ids created by Dedupe into the backbone_index table
    if backbone.is_tmp_file_used():
        utilities.insert_new_cluster_ids_into_backbone_index_table(
            backbone.data_from_config_file['database_config'],
            backbone.output_file_1,
            output_file_2=None,
            last_cluster_id=backbone.last_cluster_id_in_db)
    else:
        utilities.insert_new_cluster_ids_into_backbone_index_table(
            backbone.data_from_config_file['database_config'],
            backbone.output_file_1,
            backbone.output_file_2,
            backbone.last_cluster_id_in_db)

    # create a new table having FK on cluster_id (referencing the PK 'idx' of the backbone_index table) 
    # and insert the resulted dataset from Dedupe in the table
    # the resulted dataset is formed from the input dataset + 2 new columns: 'cluster_id' and 'link_score'
    utilities.create_table_and_insert_dataset_resulted_from_dedupe(
        backbone.data_from_config_file['database_config'],
        backbone.data_from_config_file['provider_1_name'],
        backbone.output_file_1)

    # if we were provided with a 2nd input dataset, insert it in the DB also
    if not backbone.is_tmp_file_used():
        utilities.create_table_and_insert_dataset_resulted_from_dedupe(
            backbone.data_from_config_file['database_config'],
            backbone.data_from_config_file['provider_2_name'],
            backbone.output_file_2)

    os.remove(backbone.input_file_1)
    os.remove(backbone.input_file_2)
    os.remove(backbone.training_file_name)
    os.remove(backbone.output_file_1)
    os.remove(backbone.output_file_2)
    os.remove(backbone.configuration_file_name_for_dedupe)

    if backbone.settings_file_name:
        os.remove(backbone.settings_file_name)
    else:
        os.remove("settings_file")

    return "Algorithm ran successfully"


@app.route('/create_uncertain_pairs_file', methods=['POST'])
def create_uncertain_pairs_file():
    """
    This function is called if the user wants to create a training file on the
    client side of the application. It assumes all the neccessary files were
    previously uploaded by the user. If neccessary, it creates the 2nd input 
    dataset and then executes the first 20 jupyter notebook cells. It executes
    the first 20 cells, because those cells are needed to create the uncertain
    pairs file. This file will contain pairs of examples (from the two input datasets)
    that Dedupe is unsure about.

    In the first 20 cells things like module imports, reading the configuration file, 
    reading the input datasets and creating the uncertain pairs file (if it is
    specified in the configuration file that the user wants to create the training file)
    are done.
    """

    backbone = Backbone()

    if backbone.is_tmp_file_used():
        backbone.extract_data_from_db_and_create_second_input_dataset()

    backbone.execute_jupyter_notebook_cells(idx_first_cell=0, idx_last_cell=21)

    return "Uncertain pairs file created successfully"


@app.route('/search/company/legal_name/<legal_name>', methods=['GET'])
def search_by_legal_name(legal_name):
    """
    This GET request function queries the database for companies that contain
    the given name. For more info look into the 'utilities' module at the
    'search_field_in_db_by_value' function.

    :param legal_name: string object containing the name of the company we
                       search for
    """
    
    backbone = Backbone()

    return backbone.search_field_in_db_by_value_and_return_serialized_result("legal_name", legal_name)


@app.route('/search/company/thoroughfare/<thoroughfare>', methods=['GET'])
def search_by_thoroughfare(thoroughfare):
    """
    This GET request function queries the database for companies that contain
    the given name. For more info look into the 'utilities' module at the
    'search_field_in_db_by_value' function.

    :param thoroughfare: string object containing the address where companies 
                         have their offices
    """
    backbone = Backbone()

    return backbone.search_field_in_db_by_value_and_return_serialized_result("thoroughfare", thoroughfare)


@app.route('/upload', methods=['GET', 'POST'])
def upload_files():
    """
    This function is used for uploading files from the client into the server.
    It can handle POST requests that have one or more files and stores them
    in the current working directory, after their names have passed through the
    'secure_filename' function.

    This function can also handle GET requests. In case this url is called
    from the browser, this function returns html code that the user can use
    to browse for a file, select it and upload it into the server.
    """

    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)

        files = request.files.getlist('file')

        for file in files:
            # if user does not select file, browser also
            # submit an empty part without filename
            if file.filename == '':
                flash('No selected file')
                return redirect(request.url)
            if file:
                filename = secure_filename(file.filename)
                file.save(filename)

        if len(files) > 1:
            return "All the files were uploaded successfully!"
        return "File uploaded successfully!"

    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      <input type=file name=file>
      <input type=submit value=Upload>
    </form>
    '''


@app.route('/files/<filename>')
def uploaded_file(filename):
    """
    This function returns the specified file stored on the server (if the
    file exists)

    :param filename: string object that contains the name of the file to be
                     downloaded
    """
    return send_from_directory(os.getcwd(), secure_filename(filename))


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0')
