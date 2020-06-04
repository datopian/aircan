# 💨🥫 AirCan

Load data into [CKAN DataStore](https://docs.ckan.org/en/2.8/maintaining/datastore.html) using Airflow as the runner. Replacement for DataPusher and Xloader.

Clean separation of components do you can reuse what you want (e.g. don't use Airflow but use your own runner)

Examples

* Convert a CSV file to JSON (simple case)
* Load local CSV to CKAN DataStore
* Auto Load file uploaded to CKAN into CKAN DataStore
  * Configure CKAN to automatically load

## Get Started

Create your project directory

Install Python >= 3.xx 😄 (and make a virtualenv)

```
git clone https://github.com/datopian/aircan
```


### Setup Airflow

https://airflow.apache.org/docs/stable/installation.html

```
export AIRFLOW_HOME=~/airflow
pip install apache-airflow
airflow initdb
```

Start the server and visit your airflow admin UI

```
airflow webserver -p 8080
```

### Example 1: CSV to JSON

Create the DAG for loading

```
cp aircan/examples/xxx.py ~/airflow/dags/
```

Run this DAG:

* Select this [screenshot]
* Configure it with a path to ../your/aircan/examples/example1.csv
* Run it ...

Check the output

* Locate the output on disk at `~/aircan-example-1.json`


### Local file to CKAN DataStore

We'll load a local csv into CKAN DataStore instance.

### Preliminaries: Setup your CKAN instance

We'll assume you have:

* a local CKAN setup and running at https://localhost:5000
* datastore enabled
* CKAN API key that is authorized to write with this exported as env variable:
  ```
  export CKAN_API_KEY=XXXX
  ```

Then create a dataset called `aircan-example` using this script:

```
cd aircan
pip install -r requirements-example.txt
python examples/setup-ckan.py --api-key
```

#### Doing the load

Create the DAG for loading

```
cp aircan/examples/ckan-datastore-from-local.py ~/airflow/dags/
```

Run it:

* Select the DAG [screenshot]
* Configure it with a path to ../your/aircan/examples/example1.csv
* Run it ... [screenshot]

Check the output

* Visit http://localhost:5000/dataset/aircan-example/ and see the resource named XXX. It will have data in its datastore now! 😄 💨

### Autoload ...

* Setup CKAN - see previous
  * Also install this extension in your ckan instance: ckanext-aircan-connector TODO instructions
  * Configure ckan with location of your airflow instance and the dag id (`aircan-load-csv`)

#### Setup DAG

Create the DAG for loading

```
cp aircan/examples/ckan-datastore-from-remote.py ~/airflow/dags/
```

#### Run it

Run this script which uploads a CSV file to your ckan instance and will trigger a load to the datastore.

```
cd aircan
pip install -r requirements-example.txt
python examples/ckan-upload-csv.py
```


### Get Started on Google Cloud Composer

* Sign up for an account
* Create a bucket for storing results and log data (automatic IIRC)
* Upload this library code ...
* Set up the DAG ...
* Configure with this file online https://raw.githubusercontent.com/datopian/aircan/examples/...
* Run it ...

