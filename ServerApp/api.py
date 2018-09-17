import os

import utilities

from flask import Flask, flash, request, redirect, send_from_directory
from werkzeug.utils import secure_filename

from Backbone import Backbone

app = Flask(__name__)

UPLOAD_FOLDER = os.getcwd()
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


@app.route('/run_algorithm', methods=['POST'])
def run_algorithm():
    # object that will do all the work
    backbone = Backbone()

    # run the backbone script file
    if backbone.is_tmp_file_used():
        backbone.extract_data_from_db_and_create_second_input_dataset()

    backbone.execute_jupyter_notebook_cells(idx_first_cell=0)

    # if the 2nd dataset contained rows from the database, those examples from the 2nd dataset
    # already have assigned a cluster_id (backbone index), but when Dedupe creates new clusters,
    # which are basically a part of 1 example from the 1st dataset and one from the 2nd dataset,
    # it gives that cluster a unique cluster_id (one that does not exist in the backbone_index table).
    # So, we have to give to those clusters the old cluster_ids, that the examples from the 2nd dataset
    # originally had.
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

    # create a new table having FK on cluster_id (referencing the PK of the backbone_index table) 
    # and insert the resulted dataset from Dedupe in the table
    # the resulted dataset is formed by the input dataset + 2 new columns: cluster_id and link_score
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

    if backbone.settings_file_name:
        os.remove(backbone.settings_file_name)
    else:
        os.remove("settings_file")

    return "Algorithm ran successfully"


@app.route('/create_uncertain_pairs_file', methods=['POST'])
def create_uncertain_pairs_file():
    # object that will do all the work
    backbone = Backbone()

    if backbone.is_tmp_file_used():
        backbone.extract_data_from_db_and_create_second_input_dataset()

    backbone.execute_jupyter_notebook_cells(idx_first_cell=0, idx_last_cell=20)


@app.route('/search/company/legal_name/<legal_name>', methods=['GET'])
def search_by_legal_name(legal_name):
    # object that will do all the work
    backbone = Backbone()

    return backbone.search_field_in_db_by_value_and_return_serialized_result("legal_name", legal_name)


@app.route('/search/company/thoroughfare/<thoroughfare>', methods=['GET'])
def search_by_thoroughfare(thoroughfare):
    # object that will do all the work
    backbone = Backbone()

    return backbone.search_field_in_db_by_value_and_return_serialized_result("thoroughfare", thoroughfare)


@app.route('/upload', methods=['GET', 'POST'])
def upload_files():
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
                file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))

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
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)


if __name__ == '__main__':
    app.run(debug=False, host='0.0.0.0')
