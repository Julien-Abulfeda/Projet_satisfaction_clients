import logging
import time
from src.toolbox.json_functions import (
    add_data_to_json,
    clean_json_file,
    get_data_from_json,
)
from datetime import datetime


def get_companies_preprocessed_data(testing=False):
    """
    preprocess all the companies raw data from data_raw.json
    """

    # En prod on va utiliser ce chemin pour notamment Airflow
    if not testing:
        filepath_raw_data = "/app/data/data_raw.json"
        filepath_preprocessed_data = "/app/data/data_preprocessed.json"
    else:  # Si on test on va utiliser les fichiers dans le dossier test/data_test
        filepath_raw_data = "./dags/src/test/data_test/data_test_raw.json"
        filepath_preprocessed_data = (
            "./dags/src/test/data_test/data_test_preprocessed.json"
        )

    # # We get logging started
    logging.basicConfig(
        filename="app.log",
        filemode="w",
        format="%(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
    logging.info(
        "\n////////////////////// get_companies_preprocessed_data() //////////////////////"
    )
    print(
        "\n////////////////////// get_companies_preprocessed_data() //////////////////////"
    )

    # # Start the timer
    start_time = time.time()

    # Cleaning the data_preprocessed.json file and initializing it to []
    clean_json_file(filepath_preprocessed_data)

    # Get the data from data_raw.json
    raw_data = get_data_from_json(filepath_raw_data)

    # We go through all the documents (companies data and comments) and preprocess them to the correct formating
    for i, company in enumerate(raw_data):
        logging.info(
            f" Pourcentage : {round(((i/len(raw_data))*100),2)} %  Company :{company['company_name']}"
        )
        print(
            f" Pourcentage : {round(((i/len(raw_data))*100),2)} %  Company n°{i+1} :{company['company_name']}"
        )

        # We preprocess company from raw_data
        company_preprocessed_data = preprocess_company_data(company)

        # We preprocess the company comments
        company_preprocessed_data["comments"] = preprocess_company_comments(company)

        # We add the raw data to the json
        add_data_to_json(filepath_preprocessed_data, company_preprocessed_data)

        # if i == 0:
        #     break

    # End the timer and log info
    end_time = time.time()
    logging.info(f"Execution time: {end_time - start_time} seconds")


def preprocess_company_data(company_data):
    """
    preprocess the company_data passed in parameters
    """
    preprocessed_company = {}

    if company_data["company_id"] == "":
        preprocessed_company["company_id"] = None
    else:
        preprocessed_company["company_id"] = company_data["company_id"]

    if company_data["company_name"] == "":
        preprocessed_company["company_name"] = None
    else:
        preprocessed_company["company_name"] = company_data["company_name"]

    if company_data["reviews_nbr"] == "":
        preprocessed_company["reviews_nbr"] = None
    else:
        preprocessed_company["reviews_nbr"] = int(
            company_data["reviews_nbr"].replace(",", "")
        )  # Remove ',' and convert to int

    if company_data["global_appreciation"] == "":
        preprocessed_company["global_appreciation"] = None
    else:
        preprocessed_company["global_appreciation"] = company_data[
            "global_appreciation"
        ]

    if company_data["global_rating"] == "":
        preprocessed_company["global_rating"] = None
    else:
        preprocessed_company["global_rating"] = float(
            company_data["global_rating"]
        )  # convert to float

    if company_data["company_categories"] == "":
        preprocessed_company["company_categories"] = None
    else:
        preprocessed_company["company_categories"] = company_data["company_categories"]

    if company_data["company_location"] == "":
        preprocessed_company["company_location"] = None
    else:
        preprocessed_company["company_location"] = company_data["company_location"]

    if company_data["company_phone"] == "":
        preprocessed_company["company_phone"] = None
    else:
        preprocessed_company["company_phone"] = int(
            company_data["company_phone"]
            .replace(" ", "")
            .replace("-", "")
            .replace("(", "")
            .replace(")", "")
        )  # Remove " ", "-", "(", ")" and convert to int

    if company_data["company_email"] == "":
        preprocessed_company["company_email"] = None
    else:
        preprocessed_company["company_email"] = company_data["company_email"]

    if company_data["company_verified"] == "":
        preprocessed_company["company_verified"] = None
    else:
        preprocessed_company["company_verified"] = company_data["company_verified"]

    return preprocessed_company


def preprocess_company_comments(company_data):
    """
    preprocess the company_comments passed in parameters
    """
    preprocessed_comments = []

    if company_data["comments"] == "":
        return None
    else:
        # preprocessed_company['comments'] = preprocess_company_comments(company_data['comments'])

        # We go through all the comments of this company and preprocess them
        for i, comment in enumerate(company_data["comments"]):
            preprocessed_comment = {}

            # We add an id for each comment that is not present in the raw data
            preprocessed_comment["comment_id"] = i + 1

            if comment["customer_name"] == "":
                preprocessed_comment["customer_name"] = None
            else:
                preprocessed_comment["customer_name"] = comment["customer_name"]

            if comment["rating"] == "":
                preprocessed_comment["rating"] = None
            else:
                preprocessed_comment["rating"] = int(comment["rating"])

            if comment["verification_status"] == "":
                preprocessed_comment["verification_status"] = None
            else:
                preprocessed_comment["verification_status"] = comment[
                    "verification_status"
                ]

            if comment["customer_location"] == "":
                preprocessed_comment["customer_location"] = None
            else:
                preprocessed_comment["customer_location"] = comment["customer_location"]

            if comment["comment_title"] == "":
                preprocessed_comment["comment_title"] = None
            else:
                preprocessed_comment["comment_title"] = comment["comment_title"]

            if comment["comment_content"] == "":
                preprocessed_comment["comment_content"] = None
            else:
                preprocessed_comment["comment_content"] = comment["comment_content"]

            if comment["comment_date"] == "":
                preprocessed_comment["comment_date"] = None
            else:
                preprocessed_comment["comment_date"] = comment["comment_date"]

            if comment["comment_date_of_experience"] == "":
                preprocessed_comment["comment_date_of_experience"] = None
            else:
                preprocessed_comment["comment_date_of_experience"] = datetime.strptime(
                    comment["comment_date_of_experience"], "%B %d, %Y"
                ).strftime("%Y-%m-%d")

            if comment["company_response"] == "":
                preprocessed_comment["company_response"] = None
            else:
                preprocessed_comment["company_response"] = comment["company_response"]

            if "company_response_date" in comment:
                if comment["company_response_date"] == "":
                    preprocessed_comment["company_response_date"] = None
                else:
                    preprocessed_comment["company_response_date"] = comment[
                        "company_response_date"
                    ]

            if "company_response_content" in comment:
                if comment["company_response_content"] == "":
                    preprocessed_comment["company_response_content"] = None
                else:
                    preprocessed_comment["company_response_content"] = comment[
                        "company_response_content"
                    ]

            # We add a company_response_date for each comment as it is not present in the raw data
            if "company_response_date" in comment:
                if (
                    comment["company_response_date"] == ""
                    or comment["comment_date"] == ""
                ):
                    preprocessed_comment["response_duration"] = None
                elif (
                    (
                        datetime.strptime(comment["company_response_date"], "%Y-%m-%d")
                        - datetime.strptime(comment["comment_date"], "%Y-%m-%d")
                    ).days
                ) >= 0:
                    preprocessed_comment["response_duration"] = (
                        datetime.strptime(comment["company_response_date"], "%Y-%m-%d")
                        - datetime.strptime(comment["comment_date"], "%Y-%m-%d")
                    ).days
                else:
                    preprocessed_comment["response_duration"] = (
                        datetime.strptime(comment["company_response_date"], "%Y-%m-%d")
                        - datetime.strptime(
                            preprocessed_comment["comment_date_of_experience"],
                            "%Y-%m-%d",
                        )
                    ).days

            # We add the preprocessed_company to the data
            preprocessed_comments.append(preprocessed_comment)

        return preprocessed_comments
