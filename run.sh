#!/bin/bash

# Install dependencies
pip install -r requirements.txt

# Run the Apache Beam pipeline
python main.py


# Run the unit test
python test_main.py