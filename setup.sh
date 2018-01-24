#!/usr/bin/env bash

GEOLIFE_DIR=/home/doug/Data/Geolife
GEOLIFE_ZIP="Geolife Trajectories 1.3.zip"


function install_development_dependencies {
	sudo apt install -y python3-pip python3-dev build-essential
	sudo -H pip3 install --upgrade pip
	sudo -H pip3 install virtualenv
}


function install_database {
	sudo apt install -y postgresql postgresql-server-dev-all
	sudo -H -u postgres bash -c 'echo "CREATE DATABASE geolife;" | psql'
	sudo -H -u postgres bash -c "echo CREATE USER geolife WITH PASSWORD \'nope\'\; | psql"
	sudo -H -u postgres bash -c 'echo "GRANT USAGE ON SCHEMA public TO geolife;" | psql geolife'
	sudo -H -u postgres bash -c 'echo "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO geolife;" | psql geolife'
}


function download_geolife {
	mkdir --parents "${GEOLIFE_DIR}"
	pushd           "${GEOLIFE_DIR}"
	wget https://download.microsoft.com/download/F/4/8/F4894AA5-FDBC-481E-9285-D5F8C4C4F039/Geolife%20Trajectories%201.3.zip
	unzip "${GEOLIFE_ZIP}"
	popd
}


function setup_python_dependencies {
	virtualenv -p python3 venv
	source venv/bin/activate
	pip install -r requirements.txt
}

sudo apt update && sudo apt upgrade -y

# Install and set up initial dev dependencies
which pip3 >/dev/null || install_development_dependencies

# Install and set up PostgreSQL if not already existing on system
which psql >/dev/null || install_database

# Download the GeoLife dataset
if [[ ! -e "${GEOLIFE_DIR}/${GEOLIFE_ZIP}" ]]
then
	download_geolife
fi

if [[ ! -d 'venv' ]]
then
	setup_python_dependencies
fi
