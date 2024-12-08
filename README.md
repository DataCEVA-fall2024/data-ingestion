# DataCEVA CS 620 Capstone 


## Overview
For our DataCEVA 620 Capstone project we ingest real estate data that regularly updates from redfin. From this ingested data we push it through a Kafka pipeline to BigQuery which the data is then moves through multiple processing stages(see image below) before finally being used in Tableau for dashboard visualizations

![Data Diagram](https://github.com/DataCEVA-fall2024/data-ingestion/blob/add_neighborhood_data/Data%20Diagram.png?raw=true)

## Github stuff

### Data Directory
Contains code for Kafka pipeline with custom consumer and producer additionally contains data download injestion stuff

To manually get neighborhood data use -n flag and for weekly data use -w flag with download_csv_convert_avro.py, same goes for producer send_to_kafka.py

### Scripts Directory
Holds scripts to be run on a virtual machine or some other device to facilate data upload, further instructions on setup to be found on the [ReadME](https://github.com/DataCEVA-fall2024/data-ingestion/blob/add_neighborhood_data/scripts/README.md) in Scripts directory. Best used alongside cronjobs as well as some systemd services


## Important non-Github stuff

### BIG QUERY
- Need Big Query setup to use this application/data pipeline, both mentors and instructors should have access to our account setup by DataCEVA if you wanted to set this up yourself, would need to setup Big Query instance.

### VIRTUAL MACHINES
- Worth setting up virtual machine on google cloud if already using big query, machine requirements at least 4 GB of ram and a decent amount of storage (~30+ GB)

## How to setup/run yourself
Create Database inside of bigquery according to the data diagram/schema and may need sql scripts between zones for cleaning, make sure to modify scripts to align with your zone names for data ingestion.
Setup google credentials on your machine and modify the credentials path (ideally stored in ENV but didn't get to that)

See Scripts README as mentioned above to setup infrastructure automation but otherwise setup venv, use requirements.txt to install requirements, then run your injestion scripts: Warning this may take a while...

## Misc.

### What's next
- Additonal data sources maybe setup system to get information from MLS or government listings
- API to access data
- Ensure and adapt our solution as our clients needs may change or evolve over time

### For instructors & dataCEVA
Should be able to find data and sql queries for data processing between zones inside the big query instance, additionally for VM automation the app lives in /opt/dataCEVA/...
Cronjobs are on the debiche user which isn't best practice but was easiest in to get setup in this case.

Can find systemd in /etc/systemd/system/... as showed in scripts directory README.

![Bufo Hearts](https://github.com/DataCEVA-fall2024/data-ingestion/blob/add_neighborhood_data/bufo-hearts.png)
