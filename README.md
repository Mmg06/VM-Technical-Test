# Apache Beam Data Pipeline

## Project Description
This project implements an Apache Beam data pipeline that processes transaction data. The pipeline reads the input data, filters transactions with a `transaction_amount` greater than 20, excludes transactions made before 2010, and sums the total amount by date. The results are output into a compressed CSV file.

The project is designed to work with Google Cloud Storage as the data source and can run locally using the DirectRunner.

## Table of Contents
- [Project Description](#project-description)
- [Setup Instructions](#setup-instructions)
- [Running the Pipeline](#running-the-pipeline)
- [Running the Unit Tests](#running-the-unit-tests)
- [Repository Structure](#repository-structure)

## Setup Instructions

### 1. Clone the repository
Clone the repository to your local machine:
```bash
git clone https://github.com/Mmg06/VM-Technical-Test.git
cd VM-Technical-Test
# VM-Technical-Test
VM Data Engineer Tech test

### 2. Set up a virtual environment (optional but recommended)


On Linux/macOS:
python3 -m venv venv
source venv/bin/activate

On Windows:
python -m venv venv
venv\Scripts\activate

### 3. Install the required dependencies

pip install -r requirements.txt


