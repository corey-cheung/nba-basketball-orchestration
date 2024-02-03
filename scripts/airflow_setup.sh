#!/bin/bash

airflow db migrate

airflow users create \
    --username $AIRFLOW_USER \
    --password $AIRFLOW_PW  \
    --firstname $FIRSTNAME \
    --lastname $LASTNAME \
    --role Admin \
    --email $EMAIL

# airflow webserver --port 8080

# airflow scheduler
