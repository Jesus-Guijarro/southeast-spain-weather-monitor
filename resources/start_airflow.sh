#!/bin/bash

# Open a new terminal and start Airflow Webserver
gnome-terminal -- bash -c "airflow webserver --port 8080; exec bash"

# Wait for a few seconds to ensure Zookeeper server is fully started
sleep 5

# Open a new terminal and start Airflow scheduler
gnome-terminal -- bash -c "airflow scheduler; exec bash"
