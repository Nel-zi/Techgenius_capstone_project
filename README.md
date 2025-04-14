### Trustpilot Sentiment Analysis Pipeline

This project implements an end-to-end data pipeline designed to scrape customer reviews from Trustpilot, transform and clean the data, perform sentiment analysis, load the processed results into Azure Synapse, and archive them on Azure Blob Storage. The entire workflow, from data extraction to data ingestion in Azure Synapse and archival in Azure Blob Storage is orchestrated using Apache Airflow.
Note: The pipeline runs on WSL due to Airflow's configuration.


### Table of Contents
>>> Overview

>>> Architecture

>>> Features

>>> Installation

>>> Configuration

>>> Usage

>>> Project Structure

>>> Contributing

>>> License


### Overview
The pipeline performs the following key steps:

>>> Extraction: A Python web scraper retrieves the latest reviews from Trustpilot.


>>> Raw Data Storage: Extracted data is stored in a PostgreSQL database as raw backup.


>>> Data Transformation: The raw data is cleaned (removing duplicates, stopwords, formatting text) and enriched. This stage also includes performing sentiment analysis using NLP libraries.


>>> Data Loading: The cleaned and enriched data is: Loaded into an analytical layer ( Azure synapse) for further processing. Archived as CSV files in Azure Blob Storage for historical reference.


>>> Orchestration: The entire process is automated and scheduled via Apache Airflow.


This solution ensures real-time insights into customer sentiment, enables scalability, and maintains data integrity by preserving raw data alongside transformed data.


### Architecture
Below is a high-level overview of the architecture:
                  ┌─────────────────────────────┐
                   │    Trustpilot Reviews       │
                   └────────────┬────────────────┘
                                │
                                ▼
                   ┌─────────────────────────────┐
                   │ Python Scraper (Beautifulsoup)    │
                   └────────────┬────────────────┘
                                │
                                ▼
                  ┌──────────────────────────────┐
                  │ PostgreSQL (Raw Data Layer)  │
                  └────────────┬─────────────────┘
                                │
                                ▼
           ┌────────────────────────────────────────┐
           │ Airflow Orchestrated ETL/ELT Pipeline  │
           │ - Data Cleaning & Transformation       │
           │ - Sentiment Analysis                   │
           └────────────┬───────────────────────────┘
                                │
                                ▼
                  ┌──────────────────────────────┐
                  │   Staging (Cleaned Data)     │
                  └────────────┬─────────────────┘
                                │
                        ┌───────┴───────┐
                        ▼               ▼
           ┌─────────────────┐   ┌─────────────────┐
           │   Azure synapse		 Azure Blob    │
           │    (Analytics)            Storage     │
           └─────────────────┘   └─────────────────┘

Apache Airflow is central to this solution and triggers each step on a scheduled basis (i.e every two weeks) while ensuring proper error handling and task retry mechanisms.

### Features
>>> Web Scraping: Extracts trusted customer reviews from Trustpilot.


>>> Data Storage: Saves raw data in PostgreSQL for backup.


>>> Data Transformation: Cleans and standardizes review text and metadata, Removes duplicates and irrelevant reviews and performs sentiment analysis using TextBlob.


>>> Data Loading: Exports cleaned data to an analytical data warehouse (Azure synapse) and Archives data in Azure Blob Storage.


>>> Automation: Orchestrated with Apache Airflow for end-to-end scheduling and monitoring.


>>> Logging & Error Handling: Implements logging for debugging purposes and robust error handling to ensure the pipeline’s resilience.


### Installation
>>> Clone the repository:

 > git clone https://github.com/Nel-zi/Techgenius_capstone_project.git
 >  cd Techgenius_capstone_project


>>> Create and activate a virtual environment:

 python3 -m venv venv
 source venv/bin/activate  # On Windows use: venv\Scripts\activate


>>> Install the required Python packages:

 pip install -r requirements.txt
 Note: Ensure the following libraries are installed: requests, beautifulsoup4, python-dotenv, pandas, SQLAlchemy, lxml, psycopg2, azure-storage-blob, nltk, textblob.



>>> Download NLTK Data: The script downloads necessary NLTK data at runtime (e.g., punkt, averaged_perceptron_tagger, brown, and wordnet). Alternatively, pre-download them by running:

 import nltk
 nltk.download('punkt')
 nltk.download('averaged_perceptron_tagger')
 nltk.download('brown')
 nltk.download('wordnet')


>>> Configuration
Create a .env file in the project root directory with the following required variables:
# PostgreSQL Configuration
DB_NAME=your_database_name
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_HOST=your_database_host
DB_PORT=your_database_port

# Azure Blob Storage Configuration
AZURE_CONNECT_STR=your_azure_connection_string
CONTAINER_NAME=your_blob_container_name


#  For Azure Synapse connection
SYNAPSE_SERVER=tcp:your_synapse_server_name.database.windows.net
SYNAPSE_DATABASE=YourSynapseDatabase
SYNAPSE_USER=your_synapse_username
SYNAPSE_PASSWORD=your_synapse_password

Ensure that these values are set according to your environment.


### Usage
The project is designed to be integrated with Apache Airflow. Each function (extract_data, transform_data, load_data2) is intended to be a separate task within an Airflow DAG.
Airflow DAG Integration:


Create DAG definitions that call these functions in sequence:


>>> Task 1: Run extract_data to scrape and push the raw data.


>>> Task 2: Run transform_data to clean and enrich data, including sentiment analysis.


>>> Task 3: Run load_data2 to export the final data to Azure Blob Storage.


Running Tasks Manually (for testing):
 If needed, you can call the functions individually for testing outside Airflow:

 # Extract Data
reviews_df = extract_data()
print(reviews_df.head())

# Transform Data
transformed_df = transform_data(ti={'xcom_pull': lambda **kwargs: reviews_df, 'xcom_push': print})
print(transformed_df.head())

# Load Data (this will upload the CSV to Azure Blob Storage and Azure synapse)
load_data2(ti={'xcom_pull': lambda **kwargs: transformed_df})


### Project Structure
├── README.md                      # Documentation and usage guide
├── .env                           # Environment configuration file (not committed to source control)
├── requirements.txt               # Python package requirements
├── airflow_dags/                  # Apache Airflow DAGs for scheduling the pipeline
│   └── trustpilot_pipeline.py     # Example DAG file linking the functions
├── src/                           # Source code
│   ├── extract.py                 # Contains extract_data function and scraping logic
│   ├── transform.py               # Contains transform_data function for cleaning and sentiment analysis
│   └── load.py                    # Contains load_data2 function for exporting data to Azure Blob Storage
└── utils/                         # Helper scripts and modules (if any)

### Contributing
Contributions to the project are welcome! Please fork the repository, make improvements, and create a pull request.
Fork the repository.


>>> Create a new branch: git checkout -b feature/your_feature_name


>>> Commit your changes: git commit -am 'Add new feature'


>>> Push the branch: git push origin feature/your_feature_name


>>> Open a pull request.



This documentation should give a clear and professional overview of your project on GitHub while serving as a guide for other developers or stakeholders who may work on or use your code. Feel free to modify any sections to better match your project specifics or personal style.

