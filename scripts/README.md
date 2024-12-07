# SCRIPTS

## Description 

The scripts directory holds helper scripts that are currently to be used during deployment on Google VM.


#### Usage
- run_ingestion.sh
    - Helper script to run ingestion script, activate venv and have logging and error tracking





##### Other useful things
- If data is failing to be injested you may need to reauth with google which can be done with the following command:
  - gcloud auth application-default login --no-launch-browser

###### Log Rotate

1. For the log directory it makes sense to implement log rotation to 
manage log files effectively. Follow the steps below for setup

> Create a logrotate conf file using your favorite tool (my preferred is vim)
sudo vim /etc/logrotate.d/dataCEVA_data

> Setup Configuration

```/opt/dataCEVA/data-ingestion/weekly_data/logs/run_*.log {
    daily
    rotate 7
    compress
    missingok
    notifempty
    create 0640 debiche debiche
}
```
> to test you can run `sudo logrotate -d /etc/logrotate.d/dataCEVA_data` (-d runs logrotate in debug mode without making changes)

###### Kafka systemd stuff
(This lets us start up kafka services on system start up, so we can then startup consumer to run in background sending any new data it receives to bigquery)

1. kafka.service
> located in /etc/systemd/system/kafka.service

```
[Unit]
Requires=zookeeper.service
After=zookeeper.service

[Service]
Type=simple
User=debiche
ExecStart=/bin/sh -c '/opt/dataCEVA/data-ingestion/kafka/bin/kafka-server-start.sh /opt/dataCEVA/data-ingestion/kafka/config/server.properties > /opt/dataCEVA/data-ingestion/kafka/kafka.log 2>&1'
ExecStop=/opt/dataCEVA/data-ingestion/kafka/bin/kafka-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
```

2. zookeeper.service
> located in /etc/systemd/system/zookeeper.service

```
[Unit]
Requires=network.target remote-fs.target
After=network.target remote-fs.target

[Service]
Type=simple
User=debiche
ExecStart=/opt/dataCEVA/data-ingestion/kafka/bin/zookeeper-server-start.sh /opt/dataCEVA/data-ingestion/kafka/config/zookeeper.properties
ExecStop=/opt/dataCEVA/data-ingestion/kafka/bin/zookeeper-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target

```

3. Our customer Consumer to run on startup as well
> located in /etc/systemd/system/my-kafka-consumer.service

```
[Unit]
Description=Big Query Consumer
Wants=network-online.target
After=network-online.target

[Service]
User=debiche
Group=debiche
WorkingDirectory=/opt/dataCEVA/data-ingestion/data
Environment="PATH=/opt/dataCEVA/data-ingestion/data/venv/bin:/usr/local/bin:/usr/bin:/bin"
ExecStart=/opt/dataCEVA/data-ingestion/data/venv/bin/python /opt/dataCEVA/data-ingestion/data/send_to_big_query.py
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```
