# AirCan

AirCan is an open source cloud-based system powered by AirFlow for creating, manipulating and running data workflows (aka data pipelines). It is focused on integration with CKAN and the DataHub but can also be used standalone.

It can be used to create any kind of data pipeline and out of the box it provides functionality such as:

* (Metadata) harvesting
* Data loading (e.g. to [CKAN DataStore][])
* Data validation (via goodtables)
* Data conversion (e.g. CSV => JSON)
* And more ...

In design, it is a lightweight set of patterns and services built on top of widely adopted open source products such as AirFlow and Frictionless.

For CKAN, this an evolution of DataPusher and Xloader (some core code is similar but it is much improved with the runner now being AirFlow).

[CKAN DataStore]: https://docs.ckan.org/en/2.8/maintaining/datastore.html


## Features

* Runner via AirFlow
* Workflow pattern and tooling: pattern/library for creating workflows out of tasks
* Library of tasks
* Workflow templates
* API for status, errors and logging
* Code samples for integration and deployment on cloud providers e.g. Google Cloud Composer
* Integrations: CKAN, DataHub
* Documentation


## Overview

AirCan consists of 2 parts

* The Runner: this runs the pipelines and is provided by Apache AirFlow.
* Pipelines/DAGs: a set of standard AirFlow DAGs designed for common data processing operations such as loading data into Data API storage. These are in this repo in the `aircan` directory. They get loaded into your AirFlow instance.

In addition, if you want to use it with CKAN you will want:

* CKAN extension: https://github.com/datopian/ckanext-aircan. This hooks key actions from CKAN into AirCan and provides an API to run the flows (DAGs).

<img src="https://docs.google.com/drawings/d/e/2PACX-1vRkwxYVcEmK3H8lz07WXcH7j_hhoIVDmVBQbzRzC4LhAgNUwtkNDuxB_IZp-hpbBhF_uWUvCVCQNN54/pub?w=831&amp;h=797">

How it works in e.g. CKAN:

* Resource update on CKAN (via UI or API)
* ckanext-aircan calls `aircan_submit(dag_id, run_id, config)`
* Which calls AirFlow instanceto run the DAG XXX (configurable)
* DAG runs on AirFlow [Ex: data load to datastore]
  * Goes to storage and gets the file
  * Loads into CKAN DataStore over the DataStore API
* Can then call `aircan_status`: which calls AirFlow for "is it running" + Stackdriver for logs

## Installation

* Set up AirFlow. For production we recommend using Google Cloud Composer or another hosted AirFlow instance. For trialling and development you can setup your own local AirFlow instance.
  * [Local setup instructions &raquo;](#airflow-local)
  * [Cloud setup instrucions &raquo;](#airflow-cloud)
* Install the DAGs you want into that AirFlow instance. We recommend you start with the AirCan DAGs provided in this repo and then add your own as necessary.
  * Instructions for adding AirCan DAGs are part of the AirFlow setup instructions below
* [Optional for CKAN] Setup `ckanext-aircan` on your CKAN instance. See the installation instructions in https://github.com/datopian/ckanext-aircan


### AirFlow Local

Install and setup **Airflow** (https://airflow.apache.org/docs/stable/installation.html):

```bash
export AIRFLOW_HOME=~/airflow
pip install apache-airflow
airflow initdb
```
  
_Note_: On recent versions of Python (3.7+), you may face the following error when executing `airflow initdb`:

```bash
ModuleNotFoundError: No module named 'typing_extensions'
```

This can be solved with `pip install typing_extensions`.c

* Then, start the server and visit your Airflow admin UI:

  ```bash
  airflow webserver -p 8080
  ```

By default, the server will be accessible at `http://localhost:8080/` as shown in the output of the terminal where you ran the previous command.

### AirFlow Local: Setting up AirCan DAGs

1. Open your `airflow.cfg` file (usually located at `~/airflow/airflow.cfg`) and point your DAG folder to AirCan:
    ```bash
      dags_folder = /your/path/to/aircan
      auth_backend = airflow.api.auth.backend.basic_auth
      dag_run_conf_overrides_params = True
      ...other configs
    ```
    ***Note:** do not point `dags_folder` to `/your/path/to/aircan/aircan/dags`. It must be pointing to the outer `aircan` folder*

2. Verify that Airflow finds the DAGs of Aircan by running `airflow list_dags`. The output should list:
    ```bash
    -------------------------------------------------------------------
    DAGS
    -------------------------------------------------------------------
    ckan_api_load_multiple_steps
    ...other DAGs...
    ```
3. Make sure you have these environment variables properly set up:
    ```bash
    export LC_ALL=en_US.UTF-8
    export LANG=en_US.UTF-8
    export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
    ```
4. Run the Airflow webserver (in case you have skipped the previous example): `airflow webserver`
5. Run the Airflow scheduler: `airflow scheduler`. Make sure the environment variables from (3) are set up.
6. Access the Airflow UI (`http://localhost:8080/`). You should see the DAG `ckan_api_load_single_step` listed.
7. Activate the DAG by hitting on the **Off** button on the interface (It will switch the button state to  **On**).


### AirFlow Cloud

We recommend Google Cloud Composer but there are plenty of other options. Documentation here is for Google Cloud Composer.

1. Sign up for an account at https://cloud.google.com/composer. Create or select an existing project at Google Cloud Platform. For this example, we use one called `aircan-test-project`.

2. Create an environment at Google Cloud Composer, either by command line or by UI. Make sure you select **Python 3** when creating the project. Here, we create an environment named `aircan-airflow`.
![Google Cloud Composer environment configuration](docs/resources/images/composer.png)

    After creating your environment, it should appear in your environment list:
    ![Google Cloud Composer environment configuration](docs/resources/images/composer2.png)

3. Override the configuration for `dag_run_conf_overrides_params`:
![Google Cloud Composer environment configuration](docs/resources/images/composer_airflow_config.png)

#### AirFlow Cloud: Adding the AirCan DAGs

1. Access the designated DAGs folder (which will be a bucket). Upload the contents of `local/path/to/aircan/aircan` to the bucket:
![Google Cloud Composer DAGs folder configuration](docs/resources/images/bucket1.png)

    The contents of the subfolder `aircan` must be:
    ![Google Cloud Composer DAGs folder configuration](docs/resources/images/bucket2.png)

2. Enter the subdirectory `dags` and delete the `__init__.py` file on this folder. It conflicts with Google Cloud Composer configurations.

3. [For CKAN related DAGs] Similarly to what we did on Example 2, access your Airflow instance (created by Google Cloud Composer) and add `CKAN_SITE_URL` and `CKAN_SYSADMIN_API_KEY` as Variables. Now the DAGs must appear on the UI interface.

<!-- TODO: fix this previous reference to Example 2 -->

#### Setting up Logging

Note: If you are using GCP, make sure to enable the following services for your Composer Project:

* Cloud Logging API
* Stackdriver Monitoring API

Also, make sure your service account key (which you can creating by accessing the IAM panel -> Service accounts) must have permissions to read logs and objects from buckets. Enable the following options for the service account (assuming you'll have a setup with StackDriver and Google Cloud Composer):

* Composer Administrator
* Environment and Storage Object Administrator
* Logging Admin
* Logs Bucket Writer
* Private Logs Viewer


---


## Getting Started

### Example: CSV to JSON

In this example we'll run an AirCan example to convert a CSV to JSON. The example assumes AirFlow running locally.

Add the DAG to the default directory for Airflow to recognize it:

```bash
mkdir ~/airflow/dags/
cp examples/aircan-example-1.csv ~/airflow/
cp examples/csv_to_json.py ~/airflow/dags/
```

To see this DAG appear in the Airflow admin UI, you may need to restart the server or launch the scheduler to update the list of DAGs (this may take about a minute or two to update, then refresh the page on the Airflow admin UI):

```bash
airflow scheduler
```

Run this DAG:

* Enable the dag in the [admin UI](http://localhost:8080/) with this toggle to make it run with the scheduler:
  
  ![aircan_enable_example_dag_in_scheduler](docs/resources/images/aircan_enable_example_dag_in_scheduler.png)
* "Trigger" the DAG with this button:

  ![aircan_trigger_example_dag_to_run](docs/resources/images/aircan_trigger_example_dag_to_run.png)

* After a moment, check the output. You should see a successful run for this DAG:

  ![aircan_output_example_dag](docs/resources/images/aircan_output_example_dag.png)

* Locate the output on disk at `~/airflow/aircan-example-1.json`

### Load a CSV to CKAN DataStore with local AirFlow

This assumes:

* You have a local AirFlow
* You have add the AirCan DAGs
* a local CKAN instace properly setup and running at http://localhost:5000 (or any other known endpoint of your choice);
* a dataset (for example, `my-dataset`) 
* a resouce file to send to CKAN

Now we can manually run the DAG. On your terminal, run:

```bash
airflow trigger_dag ckan_api_load_multiple_steps \
 --conf='{ "resource": {
            "path": "path/to/my.csv", 
            "format": "CSV",
            "ckan_resource_id": "res-id-123",
            "schema": {
                "fields": [
                    {
                        "name": "Field_Name",
                        "type": "number",
                        "format": "default"
                    }
                ]
            } 
        },
        "ckan_config": {
            "api_key": "API_KEY",
            "site_url": "URL",
        }
      }'
```

Replace the necessary parameters accordingly.

---

### ckan_api_load_gcp DAG: Using Google Cloud Composer

Let's assume you have a resource on `https://demo.ckan.org/` with `my-res-id-123` as its resource_id. We also assume you have, in the root of your DAG bucket on Google Cloud platform, two files: One CSV file with the resource you want to upload, named `r3.csv`, with two columns, `field1` and `field2`. The other file you must have in the root of your your bucket is `r4.json`, an empty JSON file.
![Google Cloud Composer DAGs folder configuration](docs/resources/images/setup1.png)

Since our DAGs expect parameters, you'll have to trigger them via CLI. For example, to trigger `api_ckan_load_single_node`, run (from your terminal):

```bash
gcloud composer environments run aircan-airflow \
     --location us-east1 \
     trigger_dag -- ckan_api_load_single_step \
      --conf='{ "resource": {
            "path": "path/to/my.csv", 
            "format": "CSV",
            "ckan_resource_id": "res-id-123",
            "schema": {
                "fields": [
                    {
                        "name": "Field_Name",
                        "type": "number",
                        "format": "default"
                    }
                ]
            } 
        },
        "ckan_config": {
            "api_key": "API_KEY",
            "site_url": "URL",
        }
       }'
```

Check Google Cloud logs (tip: filter them by your DAG ID, for example, `ckan_api_load_single_step`). It should updload the data of your `.csv` file to `demo.ckan` successfully.

---

### ckan_api_import_to_bq DAG: Using Google Cloud Composer

While running `test_ckan_import_to_bq` you have to provide appropriate cloud credentials for operation with BigQuery.
You can get your credentials from here: (https://cloud.google.com/docs/authentication/getting-started).
Please save your credentials as `google.json` and include in `/tests` directory.



---
### ckan_datastore_loader DAG: Auto Load file uploaded to CKAN into CKAN DataStore
Configure CKAN to automatically load.
  * Setup CKAN - Follow this instruction to setup CKAN instance locally [https://tech.datopian.com/ckan/getting-started.html#booting-ckan](https://tech.datopian.com/ckan/getting-started.html#booting-ckan).
  * Also install this extension in your ckan instance: `ckanext-aircan`.
  * Configure ckan with location of your airflow instance and the DAG id (`ckan_datastore_loader `).
  * Access your local Airflow Connections panel at http://localhost:8080/admin/connection/. Create a new connection named `ckan_postgres` with your datastore information.
  * Add `APPEND_OR_UPDATE_DATA=True` airflow env variable to append data in exising datastore table if already exsit. 
  * Add `LOAD_WITH_POSTGRES_COPY=True` airflow env variable to load with postgres copy loader. By default it loads with datastore API. 
  * Add `DATASTORE_CHUNK_INSERT_ROWS` airflow evn variable to configure number of records to send a request to datastore.

---
## Running Tests

To run all the tests, do:

> make test

You can specify the path to single test by using:

> make test TESTS_DIR=tests/test_file.py
> e.g make test TESTS_DIR=tests/test_hybrid_load.py
