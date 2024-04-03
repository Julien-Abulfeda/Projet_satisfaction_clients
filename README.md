# Customer Satisfaction Project

The **Customer Satisfaction Project** is designed to answer the following question of how to maintain continuous competitive monitoring focused on customer experience in the banking sector. By basing itself on web data, it seeks to identify opportunities for improvement that will strengthen customer satisfaction competitiveness in this sector. This project employs a comprehensive approach involving data extraction, transformation, loading, analysis, and visualization to provide actionable insights.

## Table of Contents

- [Part 1: Project Overview](#part-1-project-overview)
  - [Extract](#extract)
  - [Transform](#transform)
  - [Load](#load)
  - [Consumption](#consumption)
- [Part 2: Getting Started](#part-2-getting-started)
  - [Setting Up](#setting-up)
  - [Launching the Project](#launching-the-project)
  - [Final Steps](#final-steps)

## Part 1: Project Overview

### Extract

Data comes from **Trustpilot**, featuring banking sector companies. We collect:
- General company information (e.g., overall rating, location, phone number).
- Customer reviews (e.g., comment, date, rating, company response).

Data extraction is performed using Python scripts with Beautiful Soup, storing the data in a JSON file.

### Transform

The extracted data undergoes processing to:
- Convert formats.
- Replace special characters to standardize the data.

This step is done using a Python script.

### Load

The processed data is stored in:
- **Elasticsearch**: For data quality checks via Kibana dashboards and aggregating customer ratings.
- **Postgres Database**: Temporary tables from the previous day's scrape are merged with historical tables through an upsert operation, updating or adding comments as needed.

A Python script, integrating Langchain and OpenAI's ChatGPT, analyzes comments to categorize feedback (e.g., customer relations, pricing) and assess sentiment (positive or negative). Comments not yet analyzed are queried, analyzed, added to a feedback table, and marked as "analyzed" in the comments table.

### Consumption

An interactive **Power BI report** connected to the Postgres database provides:
- A main tab for general data.
- A detail tab for company-specific information.
- An analysis tab for feedback insights.

The project is orchestrated with **Airflow**, running daily, and containerized using **Docker**.

## Part 2: Getting Started

### Setting Up

Navigate to the Docker folder (Docker\docker_Airflow_ES_Kibana) and initiate the setup by running:

```bash
./setup.sh
```
This is used to create the required folders needed by Postgres
### Launching the Project

Start the project components with Docker Compose:
```bash
docker-compose up
```
This will create all the images needed and launch the containers associated. 
### Final Steps
- **Connect to the Postgres Database:** Utilize the provided credentials within pgadmin at port 5050 (http://localhost:5050/).
- **Activate Airflow DAG:** Access Airflow to enable the DAG for daily operations at port 8080 (http://localhost:8080/).
- **Connect to Kibana dashboard:** Access Kibana dashboard to check the data at port 5601 (http://localhost:5601/).

## Warning 

In order to launch all the containers, Docker must have around 11GB of available memory
