import _sqlite3
import pandas as pd
import json
from elastic import elastic_query_rating

def creation_db_relationnal():
    '''
    This function is designed to create a relationnal database. Here's the working of the function : 
    1- creation of a database using _sqlite frame
    2- Pulling data from the scrap companie file json 
    3- Creation of a dataframe using Pandas
    4- Retriving the dataframe created by using elasticsearch query
    5- Merge of the 2 dataframes into 1
    6- Pushing the dataframe into the created database 
    '''
    conn = _sqlite3.connect("../data/database/relational_database.db")
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS companies")
    c.execute("""CREATE TABLE IF NOT EXISTS companies (company_name, 
            company_location, 
            company_phone TEXT,
            company_email,
            company_verified,
            reviews_nbr INTEGER,
            global_appreciation,
            global_rating,
            nbr_rating_1,
            nbr_rating_2,
            nbr_rating_3,
            nbr_rating_4,
            nbr_rating_5)""")
    conn.commit()

    with open("../data/data_preprocessed.json", "r") as json_file : 
        data_preprocessed = json.load(json_file)

    df_data = {}
    df_data = {"company_name" : [element["company_name"] for element in data_preprocessed],
            "company_location" : [element["company_location"] for element in data_preprocessed],
            "company_phone" : [str(element["company_phone"]) for element in data_preprocessed],
            "company_email" : [element["company_email"] for element in data_preprocessed],
            "company_verified": [element["company_verified"] for element in data_preprocessed],
            "reviews_nbr": [element["reviews_nbr"] for element in data_preprocessed],
            "global_appreciation" : [element["global_appreciation"] for element in data_preprocessed],
            "global_rating" : [element["global_rating"] for element in data_preprocessed]}
    df_elastic = elastic_query_rating("comment_id")

    df_elastic = df_elastic.rename(columns={"Company":"company_name"})
    df = pd.DataFrame(data = df_data, index = [element["company_id"] for element in data_preprocessed])
    
    df_combined = df.merge(right=df_elastic,on="company_name",how="right")
    df_combined.to_sql('companies', conn, if_exists='replace', index = True)
    return