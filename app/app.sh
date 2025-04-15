#!/bin/bash
# Start ssh server and services
service ssh restart
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
# Sourcing environment
source .venv/bin/activate

# Install all required packages
pip install -r requirements.txt  

# Packaging environment
venv-pack -o .venv.tar.gz

# Collecting data
bash prepare_data.sh

# Start indexer
bash index.sh

# Start searcher
bash search.sh "cat and dog"