# SCRIPTS

## Description 

The scripts directory holds helper scripts that are currently to be used during deployment on Google VM.


#### Usage
- run_ingestion.sh
    - Helper script to run ingestion script, activate venv and have logging and error tracking








##### Other useful things

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

