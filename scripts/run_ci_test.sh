#!/bin/bash

# run requirement.txt
pip install -r requirements.txt

# Change directory to the data folder
cd data/

# Run all test cases using pytest
pytest Test*.py