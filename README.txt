SETTING UP THE GCP ENVIRONMENT
------------------------------

# Project ID and bucket name should be globally unique
# Location should be available for both Storage and BigQuery
export PROJECTID=ny-taxi-rides-hg
export PROJECTNAME="NY Taxi Rides"
export SUBSCRIPTION=taxirides
export TESTTOPIC=test
export TESTSUBSCRIPTION=test
export DFLOCATION=us-central1
export BUCKETNAME=ny-taxi-rides-hg
export BQLOCATION=us-east1
export BQDATASET=taxirides
export SERVICEACCOUNT=taxirides
export KEYFILE=taxirides-gcp-keys.json

# Create project. 
gcloud projects create $PROJECTID --name=$PROJECTNAME
gcloud config set project $PROJECTID

# Linking the new project to a billing account
# can't be performed from the shell, but only from the GCP console:
# https://console.cloud.google.com/billing/projects

# Configure service account and download key file
gcloud iam service-accounts create $SERVICEACCOUNT
gcloud projects add-iam-policy-binding $PROJECTID \
    --member "serviceAccount:$SERVICEACCOUNT@$PROJECTID.iam.gserviceaccount.com" \
    --role "roles/owner"
gcloud iam service-accounts keys create $KEYFILE \
    --iam-account $SERVICEACCOUNT@$PROJECTID.iam.gserviceaccount.com
cloudshell download ./$KEYFILE 

# Setting up DataFlow service. Enabling API and
# creating Storage bucket for cloud DataFlow operation
gcloud services enable dataflow.googleapis.com
gsutil mb -c STANDARD -l $DFLOCATION -b on gs://$BUCKETNAME/

# Setting up subscription for the streaming data
gcloud services enable pubsub.googleapis.com 
gcloud pubsub subscriptions create projects/$PROJECTID/subscriptions/$SUBSCRIPTION \
    --topic=projects/pubsub-public-data/topics/taxirides-realtime \
    --ack-deadline=60 \
    --message-retention-duration=6h
# Setting up test topic and subscription for manual testing
gcloud pubsub topics create projects/$PROJECTID/topics/$TESTTOPIC
gcloud pubsub subscriptions create projects/$PROJECTID/subscriptions/$TESTSUBSCRIPTION \
    --topic=projects/$PROJECTID/topics/$TESTTOPIC \
    --ack-deadline=60 \
    --message-retention-duration=6h

# Creating BigQuery dataset and tables
bq mk -d --data_location=$BQLOCATION $BQDATASET
bq mk --table $BQDATASET.filtered_records \
    ride_id:STRING,timestamp:TIMESTAMP,ride_status:STRING,passenger_count:INT64,processed:TIMESTAMP
bq mk --table $BQDATASET.passenger_counts \
    sum_passengers:INT64,window_start:TIMESTAMP,window_end:TIMESTAMP
bq mk --table $BQDATASET.status_counts \
    ride_status:STRING,count:INT64,window_start:TIMESTAMP,window_end:TIMESTAMP

SETTING UP PYTHON VIRTUAL ENVIRONMENT
-------------------------------------

systeminfo | findstr /B /C:"OS Name" /C:"OS Version"
:: OS Name:                   Microsoft Windows 10 Pro
:: OS Version:                10.0.18362 N/A Build 18362
python --version
:: Python 3.7.5
python -m venv nytaxirides
nytaxirides\Scripts\activate
pip list
:: Package    Version
:: ---------- -------
:: pip        19.2.3
:: setuptools 41.2.0
pip --version
:: pip 19.2.3 from c:\python\nytaxirides\lib\site-packages\pip (python 3.7)
python -m pip install --upgrade pip
:: ...
:: Successfully installed pip-19.3.1
pip install apache-beam[gcp]
pip list
:: Package                  Version
:: ------------------------ ----------
:: apache-beam              2.17.0
:: google-api-core          1.16.0
:: google-apitools          0.5.28
:: google-auth              1.10.1
:: google-cloud-bigquery    1.17.1
:: google-cloud-bigtable    1.0.0
:: google-cloud-core        1.2.0
:: google-cloud-datastore   1.7.4
:: google-cloud-pubsub      1.0.2
:: google-resumable-media   0.4.1
:: googleapis-common-protos 1.51.0
:: ...

PROJECT FOLDER STRUCTURE
------------------------

.\nytaxirides\__init__.py
.\nytaxirides\pipeline.py
.\nytaxirides\taxi.py
.\nytaxirides\transform.py
.\.gitignore
.\LICENSE
.\README.txt
.\setup.py
.\taximain.bat
.\taximain.py

RUN THE PROGRAM ON WINDOWS
--------------------------

This BAT file activates the Python virtual environment and runs
the application with a rich set of command line arguments:

.\taximain.bat

For a comprehensive description of all the command line parameters
check the taxi.py module.

CHECK FOR DUPLICATIONS (BIGQUERY)
---------------------------------

#standardSQL
SELECT COUNT(DISTINCT ride_id) AS distinct_recno, COUNT(ride_id) AS all_recno
FROM taxirides.filtered_records
WHERE processed < CURRENT_TIMESTAMP AND ride_status = 'dropoff';

OTHER TABLE CHECK QUERIES
-------------------------

SELECT * 
FROM taxirides.passenger_counts 
ORDER BY window_end DESC
LIMIT 100;

SELECT * 
FROM taxirides.status_counts  
ORDER BY window_end DESC
LIMIT 100;

SELECT * 
FROM taxirides.filtered_records 
ORDER BY processed DESC
LIMIT 100;