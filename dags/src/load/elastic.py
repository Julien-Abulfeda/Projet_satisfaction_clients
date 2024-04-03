#! /usr/bin/python
from elasticsearch import Elasticsearch, exceptions, helpers
import json
import warnings
import pandas as pd
import uuid
import time


def bulk_index(index_name, filename):
    """
    this function connect to Elasticsearch and load the index of the document "filename"
    """
    # Connection to the cluster
    es = Elasticsearch(hosts="http://elasticsearch:9200")
    # Delete the index if it exists
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
    # Create the index
    es.indices.create(index=index_name)
    # Wait for the index to be ready
    es.cluster.health(index=index_name, wait_for_status="yellow")

    # Get the health status of the index
    health = es.cluster.health(index=index_name)
    # Print the health status
    print("The health status of the index is:", health["status"])

    # Uncomment this command if you are using a secure installation with 3 nodes
    # es = Elasticsearch(hosts = "https://elastic:datascientest@localhost:9200",ca_certs="./ca/ca.crt")
    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
            helpers.bulk(es, data, index=index_name)
    except exceptions.BadRequestError as e:
        for error in e.errors:
            print("///////////////////////////////////////////////////", error)


def data_generator(es_querry_response):
    """
    Create a generator to store the data of elasticsearsh querry
    """
    # Access the required data
    for bucket in es_querry_response["aggregations"]["by_company"]["buckets"]:
        # Create a dictionary to store the data of this company
        company_data = {
            "Company": bucket["key"],
            "Number of Comments": bucket["doc_count"],
        }
        # Add the rating data to the company
        for rating in bucket["ratings"]["buckets"]:
            company_data[f"Rating_{rating['key']}"] = rating["doc_count"]
        yield company_data


def split_preprocessed_json(filename):
    """
    This function split our data.preprocessing.json into 2 jsons comments.json and companies.json

    """
    # Charger le fichier JSON
    with open(filename, "r") as f:
        data = json.load(f)

    # Initialiser les listes pour stocker les informations de toutes les entreprises et tous les commentaires
    all_company_info = []
    all_comments = []

    for company in data:
        # print(company["company_name"])
        # Extraire les informations de l'entreprise
        company_info = {
            "company_id": company["company_id"],
            "company_name": company["company_name"],
            "reviews_nbr": company["reviews_nbr"],
            "global_appreciation": company["global_appreciation"],
            "global_rating": company["global_rating"],
            "company_categories": company["company_categories"],
            "company_location": company["company_location"],
            "company_phone": company["company_phone"],
            "company_email": company["company_email"],
            "company_verified": company["company_verified"],
        }
        all_company_info.append(company_info)
        # print(all_company_info)

        # Extraire les commentaires
        for comment in company["comments"]:
            comment["company_id"] = company["company_id"]
            comment["company_name"] = company["company_name"]
            comment["analysed"] = False
            comment["comment_id"] = str(uuid.uuid4())  # Ajouter un identifiant unique
            all_comments.append(comment)

    # Enregistrer les informations de toutes les entreprises dans un nouveau fichier JSON
    with open("/app/data/companies.json", mode="w", encoding="utf-8") as json_file:
        json.dump(all_company_info, json_file, indent=2)
    # Enregistrer tous les commentaires dans un nouveau fichier JSON
    with open("/app/data/comments.json", mode="w", encoding="utf-8") as json_file:
        json.dump(all_comments, json_file, indent=2)
    return


def elastic_query_rating(index_name):
    """
    This function do a query in the index already exist in elasticsearsh
    """
    warnings.filterwarnings("ignore")

    # Connect to the cluster
    client = Elasticsearch(hosts="http://elasticsearch:9200")

    query = {
        "size": 0,
        "aggs": {
            "by_company": {
                "terms": {"field": "company_name.keyword", "size": 100},
                "aggs": {
                    "ratings": {"terms": {"field": "rating", "order": {"_key": "asc"}}}
                },
            }
        },
    }

    # time sleep needed to wait that the index will be created
    time.sleep(5)

    try:
        es_querry_response = client.search(index=index_name, body=query)
        print(es_querry_response)
    except exceptions.ElasticsearchException as e:
        print(f"An error occurred while executing the Elasticsearch query: {e}")
        return None

    # Create a DataFrame from the generator
    df = pd.DataFrame(data_generator(es_querry_response))

    print(df)
    # close elasticshearsh cluster
    # client.close()
    # Return the DataFrame
    return df
