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

Run the *api.py* script from a terminal

```
python3 api.py
```

### Client Application

Run the *ClientApp.py* script from a terminal

```
python3 ClientApp.py
```

### Optional: Instance matching algorithm

To run only the instance matching algorithm, one needs to have Jupyter Notebook installed and open the *dedupe_interlinking_data.ipynb* file, that can be found in the ServerApp folder, with Jupyter.

### Prerequisites

What things you need to install the software and how to install them

```
Give examples
```

### Installing

A step by step series of examples that tell you how to get a development env running

Say what the step will be

```
Give the example
```

And repeat

```
until finished
```

End with an example of getting some data out of the system or using it for a little demo
