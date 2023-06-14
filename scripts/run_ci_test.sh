#!/bin/bash

# run requirement.txt
pip install -r main/requirements.txt

# Change directory to the data folder
cd main/data/

# Run all test cases using pytest
pytest Test*.py