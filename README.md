# The Backbone Application: Instance Matching for Company Data using Dedupe

## Description

The Backbone Application is a client-server application, based on a machine learning algorithm, that matches instances from different datasets and stores them in a database. 

### Instance matching algorithm

The instance matching algorithm is used to match data about companies. It receives a configuration file and two input datasets (.csv files), each containing data about different companies, and tries to find and create links between two entities that refer to the same company. The 'links' are represented by clusters, i.e., if the algorithm matched two companies, they will be put in the same cluster. The output is represent by two .csv files, which are composed of the data that was in the input files and two new columns: 'cluster_id' (the id of the cluster the company was assigned to) and 'link_score' (score representing how similar that company is with the others that were assigned to the same cluster). 

Notes: 
* companies that do not match with other companies from the other dataset are assigned to their own cluster (a 1 element cluster)
* the algorithm does a one-to-one match between the two datasets, i.e., it matches at most two companies from the two datasets; if there are two entities in the same dataset that refer to the same company, they will for sure not end up in the same cluster

### Server Application

The server side is a RESTful application that:
* can receive all the neccessary input files that the matching algorithm needs
* if the user only sent one dataset with company data, the server can extract company entities from the database (based on the jurisdiction the user specifies in the configuration file) and create the second dataset that the algorithm needs
* can insert into the database the results of the matching algorithm
* can run the matching algorithm when all the neccessary input files were provided
* can search by name or address in the database for companies

### Client Application

The client side is a desktop application where the user can:
* select and upload the input files that the algorithm needs
* create a training file for the algorithm (the file is automatically sent to the server after it was created)
* start the algorithm (after all the neccessary files were uploaded)
* search for companies, by their names or addresses, in the database


## Getting Started

### Server Application

Run the *api.py* script in an IDE that supports Python or from a terminal like in the example below:

```
python3 api.py
```

### Client Application

Run the *ClientApp.py* script in an IDE that supports Python or from a terminal like in the example below

```
python3 ClientApp.py
```

### Optional: Instance matching algorithm

To run only the instance matching algorithm, one needs to have Jupyter Notebook installed and open the *dedupe_interlinking_data.ipynb* file, that can be found in the ServerApp folder, with Jupyter.

## Prerequisites

* Jupyter notebook (needed only if the matching algorithm is to be run individually) - [intallation guide](https://jupyter.readthedocs.io/en/latest/install.html)
* Python 3 - [installation guide](https://wiki.python.org/moin/BeginnersGuide/Download)
* pandas (if Jupyter notebook and Anaconda are not installed) - [installation guide](https://pandas.pydata.org/pandas-docs/stable/install.html)
* numPy (if Jupyter notebook and Anaconda are not installed) - [installation guide](https://docs.scipy.org/doc/numpy/user/install.html)
* dedupe - Dedupe's GitHub page can be found [here](https://github.com/dedupeio/dedupe)
```
pip install dedupe
```
* unidecode (used in the instance matching algorithm for preprocessing data)
```
pip install unidecode
```
* simplejson (e.g.: used in the instance matching algorithm to read the JSON configuration file) 
```
pip install simplejson
```
* flask - [installation guide](http://flask.pocoo.org/docs/0.12/installation/)
* requests
  * [official installation guide](http://docs.python-requests.org/en/master/user/install/)
  * [stackoverflow installation guide](https://stackoverflow.com/questions/30362600/how-to-install-requests-module-in-python-3-4-instead-of-2-7)
* Database:
  * PostgreSQL database - [installation guide](https://wiki.postgresql.org/wiki/Detailed_installation_guides)
  * psycopg - [installation guide](http://initd.org/psycopg/docs/install.html)
