import _sqlite3
import psycopg2
import pandas as pd
import json
from src.load.elastic import elastic_query_rating
from sqlalchemy import create_engine


def creation_db_relationnal():
    """
    This function is designed to create a relationnal database. Here's the working of the function :
    1- creation of a database using _sqlite frame
    2- Pulling data from the scrap companie file json
    3- Creation of a dataframe using Pandas
    4- Retriving the dataframe created by using elasticsearch query
    5- Merge of the 2 dataframes into 1
    6- Pushing the dataframe into the created database
    """
    conn = _sqlite3.connect("/app/data/database/relational_database.db")
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

    with open("/app/data/data_preprocessed.json", "r") as json_file:
        data_preprocessed = json.load(json_file)

    df_data = {}
    df_data = {
        "company_name": [element["company_name"] for element in data_preprocessed],
        "company_location": [
            element["company_location"] for element in data_preprocessed
        ],
        "company_phone": [
            str(element["company_phone"]) for element in data_preprocessed
        ],
        "company_email": [element["company_email"] for element in data_preprocessed],
        "company_verified": [
            element["company_verified"] for element in data_preprocessed
        ],
        "reviews_nbr": [element["reviews_nbr"] for element in data_preprocessed],
        "global_appreciation": [
            element["global_appreciation"] for element in data_preprocessed
        ],
        "global_rating": [element["global_rating"] for element in data_preprocessed],
    }
    df_elastic = elastic_query_rating("comment_id")

    df_elastic = df_elastic.rename(columns={"Company": "company_name"})
    df = pd.DataFrame(data=df_data)

    df_combined = df.merge(right=df_elastic, on="company_name", how="right")
    df_combined.to_sql("companies", conn, if_exists="replace", index=False)
    return


def creation_postrgesql_db_relationnal():
    """
    This function is designed to create a relationnal database. Here's the working of the function :
    1- creation of a database using psycopg2 frame
    2- Pulling data from the scrap companie file json
    3- Creation of a dataframe using Pandas
    4- Retriving the dataframe created by using elasticsearch query
    5- Merge of the 2 dataframes into 1
    6- Pushing the dataframe into the created database
    """
    conn = psycopg2.connect(
        database="dst_db_project",
        user="dst_db_project",
        password="dst_db_project",
        host="postgres_db",
        port="5432",
    )
    c = conn.cursor()
    # c.execute("DROP TABLE IF EXISTS companies")
    c.execute("""CREATE TABLE IF NOT EXISTS companies (company_name TEXT, 
            company_location TEXT, 
            company_phone TEXT,
            company_email TEXT,
            company_verified BOOLEAN,
            reviews_nbr INTEGER,
            global_appreciation TEXT,
            global_rating REAL,
            nbr_rating_1 INTEGER,
            nbr_rating_2 INTEGER,
            nbr_rating_3 INTEGER,
            nbr_rating_4 INTEGER,
            nbr_rating_5 INTEGER)""")
    conn.commit()

    with open("/app/data/data_preprocessed.json", "r") as json_file:
        data_preprocessed = json.load(json_file)

    df_data = {
        "company_name": [element["company_name"] for element in data_preprocessed],
        "company_location": [
            element["company_location"] for element in data_preprocessed
        ],
        "company_phone": [
            str(element["company_phone"]) for element in data_preprocessed
        ],
        "company_email": [element["company_email"] for element in data_preprocessed],
        "company_verified": [
            element["company_verified"] for element in data_preprocessed
        ],
        "reviews_nbr": [element["reviews_nbr"] for element in data_preprocessed],
        "global_appreciation": [
            element["global_appreciation"] for element in data_preprocessed
        ],
        "global_rating": [element["global_rating"] for element in data_preprocessed],
    }
    df_elastic = elastic_query_rating("comment_id")

    df_elastic = df_elastic.rename(columns={"Company": "company_name"})
    df = pd.DataFrame(data=df_data)

    df_combined = df.merge(right=df_elastic, on="company_name", how="right")

    # Create an engine
    engine = create_engine(
        "postgresql://dst_db_project:dst_db_project@postgres_db:5432/dst_db_project"
    )
    # Use the engine in to_sql
    df_combined.to_sql("companies", engine, if_exists="replace", index=False)
    # df_combined.to_sql('companies', conn, if_exists='replace', index = True)
    return


def creation_postrgesql_db_relationnal_comments():
    """
        # to do
    This function is designed to create a relationnal database. Here's the working of the function :
    1- creation of a database using psycopg2 frame
    2- Pulling data from the scrap companie file json
    3- Creation of a dataframe using Pandas
    4- Retriving the dataframe created by using elasticsearch query
    5- Merge of the 2 dataframes into 1
    6- Pushing the dataframe into the created database
    """
    conn = psycopg2.connect(
        database="dst_db_project",
        user="dst_db_project",
        password="dst_db_project",
        host="postgres_db",
        port="5432",
    )
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS comments")
    c.execute("""CREATE TABLE IF NOT EXISTS comments (comment_id TEXT, 
            customer_name TEXT,
            rating INTEGER,
            verification_status BOOLEAN,
            customer_location TEXT,
            comment_title TEXT,
            comment_content TEXT,
            comment_date DATE,
            comment_date_of_experience DATE,
            company_response BOOLEAN,
            company_response_date DATE,
            company_response_content TEXT,
            response_duration INTEGER,
            company_id TEXT,
            company_name TEXT,
            analysed BOOLEAN )""")
    conn.commit()

    with open("/app/data/comments_analyzed.json", "r") as json_file:
        comments = json.load(json_file)

    df_data = {
        "comment_id": [element["comment_id"] for element in comments],
        "customer_name": [element["customer_name"] for element in comments],
        "rating": [str(element["rating"]) for element in comments],
        "verification_status": [element["verification_status"] for element in comments],
        "customer_location": [element["customer_location"] for element in comments],
        "comment_title": [element["comment_title"] for element in comments],
        "comment_content": [element["comment_content"] for element in comments],
        "comment_date": [element["comment_date"] for element in comments],
        "comment_date_of_experience": [
            element["comment_date_of_experience"] for element in comments
        ],
        "company_response": [element["company_response"] for element in comments],
        "company_response_date": [
            element["company_response_date"] if element["company_response"] else ""
            for element in comments
        ],
        "company_response_content": [
            element["company_response_content"] if element["company_response"] else ""
            for element in comments
        ],
        "response_duration": [
            element["response_duration"] if element["company_response"] else ""
            for element in comments
        ],
        "company_id": [element["company_id"] for element in comments],
        "company_name": [element["company_name"] for element in comments],
        "analysed": [element["analysed"] for element in comments],
    }

    df = pd.DataFrame(data=df_data)

    # Create an engine
    engine = create_engine(
        "postgresql://dst_db_project:dst_db_project@postgres_db:5432/dst_db_project"
    )
    # Use the engine in to_sql
    df.to_sql("comments", engine, if_exists="replace", index=False)
    # df_combined.to_sql('companies', conn, if_exists='replace', index = True)
    c.execute("""ALTER TABLE comments 
                ADD CONSTRAINT ma_contrainte_unique UNIQUE (comment_title,customer_name,company_name,comment_date);""")
    conn.commit()
    return


def get_comments_non_analysed():
    """
    # to do

    """
    conn = psycopg2.connect(
        database="dst_db_project",
        user="dst_db_project",
        password="dst_db_project",
        host="postgres_db",
        port="5432",
    )

    sql_query = "SELECT * FROM comments WHERE 	analysed = False"
    df = pd.read_sql_query(sql_query, conn)
    conn.commit()
    print(df.head(20))
    return df


def creation_postrgesql_db_relationnal_comments_tmp():
    """
        # to do
    This function is designed to create a relationnal database. Here's the working of the function :
    1- creation of a database using psycopg2 frame
    2- Pulling data from the scrap companie file json
    3- Creation of a dataframe using Pandas
    4- Retriving the dataframe created by using elasticsearch query
    5- Merge of the 2 dataframes into 1
    6- Pushing the dataframe into the created database
    """
    conn = psycopg2.connect(
        database="dst_db_project",
        user="dst_db_project",
        password="dst_db_project",
        host="postgres_db",
        port="5432",
    )
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS comments_tmp")
    c.execute("""CREATE TABLE IF NOT EXISTS comments_tmp (comment_id TEXT, 
            customer_name TEXT,
            rating INTEGER,
            verification_status BOOLEAN,
            customer_location TEXT,
            comment_title TEXT,
            comment_content TEXT,
            comment_date DATE,
            comment_date_of_experience DATE,
            company_response BOOLEAN,
            company_response_date DATE,
            company_response_content TEXT,
            response_duration INTEGER,
            company_id TEXT,
            company_name TEXT,
            analysed BOOLEAN )""")
    conn.commit()

    with open("/app/data/comments.json", "r") as json_file:
        comments = json.load(json_file)

    df_data = {
        "comment_id": [element["comment_id"] for element in comments],
        "customer_name": [element["customer_name"] for element in comments],
        "rating": [str(element["rating"]) for element in comments],
        "verification_status": [element["verification_status"] for element in comments],
        "customer_location": [element["customer_location"] for element in comments],
        "comment_title": [element["comment_title"] for element in comments],
        "comment_content": [element["comment_content"] for element in comments],
        "comment_date": [element["comment_date"] for element in comments],
        "comment_date_of_experience": [
            element["comment_date_of_experience"] for element in comments
        ],
        "company_response": [element["company_response"] for element in comments],
        "company_response_date": [
            element["company_response_date"] if element["company_response"] else ""
            for element in comments
        ],
        "company_response_content": [
            element["company_response_content"] if element["company_response"] else ""
            for element in comments
        ],
        "response_duration": [
            element["response_duration"] if element["company_response"] else ""
            for element in comments
        ],
        "company_id": [element["company_id"] for element in comments],
        "company_name": [element["company_name"] for element in comments],
        "analysed": [element["analysed"] for element in comments],
    }

    df = pd.DataFrame(data=df_data)
    df = df.drop_duplicates(
        ["customer_name", "comment_title", "comment_date", "company_name"], keep="first"
    )
    # Create an engine
    engine = create_engine(
        "postgresql://dst_db_project:dst_db_project@postgres_db:5432/dst_db_project"
    )
    # Use the engine in to_sql
    df.to_sql("comments_tmp", engine, if_exists="replace", index=False)
    # df_combined.to_sql('companies', conn, if_exists='replace', index = True)
    return


def upsert_postrgesql_db_relationnal_comments():
    """
    to do
    """
    conn = psycopg2.connect(
        database="dst_db_project",
        user="dst_db_project",
        password="dst_db_project",
        host="postgres_db",
        port="5432",
    )
    c = conn.cursor()
    c.execute("""INSERT INTO comments (comment_id, customer_name, rating, verification_status, customer_location, comment_title, comment_content, comment_date, comment_date_of_experience, company_response, company_response_date, company_response_content, response_duration, company_id, company_name, analysed)
                SELECT comment_id, customer_name, rating, verification_status, customer_location, comment_title, comment_content, comment_date, comment_date_of_experience, company_response, company_response_date, company_response_content, response_duration, company_id, company_name, analysed
                FROM comments_tmp
                ON CONFLICT (comment_title,customer_name,company_name,comment_date)
                DO UPDATE SET 
                customer_name = EXCLUDED.customer_name,
                rating = EXCLUDED.rating,
                verification_status = EXCLUDED.verification_status,
                customer_location = EXCLUDED.customer_location,
                comment_title = EXCLUDED.comment_title,
                comment_content = EXCLUDED.comment_content,
                comment_date = EXCLUDED.comment_date,
                comment_date_of_experience = EXCLUDED.comment_date_of_experience,
                company_response = EXCLUDED.company_response,
                company_response_date = EXCLUDED.company_response_date,
                company_response_content = EXCLUDED.company_response_content,
                response_duration = EXCLUDED.response_duration,
                company_name = EXCLUDED.company_name""")

    # c.execute("DROP TABLE IF EXISTS companies_tmp")
    conn.commit()
    return


def update_comments_analysis_status():
    """
    # to do

    """
    conn = psycopg2.connect(
        database="dst_db_project",
        user="dst_db_project",
        password="dst_db_project",
        host="postgres_db",
        port="5432",
    )
    c = conn.cursor()

    df = pd.read_json("/app/data/feedbacks.json")
    if "comment_id" in df:
        comments_id_list = df["comment_id"].unique().tolist()

        for id in comments_id_list:
            c.execute(
                """UPDATE public.comments
                                SET analysed= %s        
                                    WHERE comment_id = %s;""",
                (True, id),
            )
            conn.commit()
    return


def creation_postrgesql_db_relationnal_companies_tmp():
    """
    This function is designed to create a relationnal database. Here's the working of the function :
    1- creation of a database using psycopg2 frame
    2- Pulling data from the scrap companie file json
    3- Creation of a dataframe using Pandas
    4- Retriving the dataframe created by using elasticsearch query
    5- Merge of the 2 dataframes into 1
    6- Pushing the dataframe into the created database
    """
    conn = psycopg2.connect(
        database="dst_db_project",
        user="dst_db_project",
        password="dst_db_project",
        host="postgres_db",
        port="5432",
    )
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS companies_tmp")
    c.execute("""CREATE TABLE IF NOT EXISTS companies_tmp (company_name TEXT, 
                company_location TEXT, 
                company_phone TEXT,
                company_email TEXT,
                company_verified BOOLEAN,
                reviews_nbr INTEGER,
                global_appreciation TEXT,
                global_rating REAL,
                nbr_rating_1 INTEGER,
                nbr_rating_2 INTEGER,
                nbr_rating_3 INTEGER,
                nbr_rating_4 INTEGER,
                nbr_rating_5 INTEGER)""")
    conn.commit()

    with open("/app/data/data_preprocessed.json", "r") as json_file:
        data_preprocessed = json.load(json_file)


    df_data = {
        "company_name": [element["company_name"] for element in data_preprocessed],
        "company_location": [
            element["company_location"] for element in data_preprocessed
        ],
        "company_phone": [
            str(element["company_phone"]) for element in data_preprocessed
        ],
        "company_email": [element["company_email"] for element in data_preprocessed],
        "company_verified": [
            element["company_verified"] for element in data_preprocessed
        ],
        "reviews_nbr": [element["reviews_nbr"] for element in data_preprocessed],
        "global_appreciation": [
            element["global_appreciation"] for element in data_preprocessed
        ],
        "global_rating": [element["global_rating"] for element in data_preprocessed],
    }
    df_elastic = elastic_query_rating("comment_id")

    df_elastic = df_elastic.rename(columns={"Company": "company_name"})
    df = pd.DataFrame(data=df_data)
    df = df.drop_duplicates(["company_name"], keep="first")
    df_combined = df.merge(right=df_elastic, on="company_name", how="left")
    
    # Ajout des colonnes rating_1, ect.. si pas présente
    if "Rating_1" not in df_combined:
        df_combined = df_combined.assign(Rating_1=0)
    if "Rating_2" not in df_combined:
        df_combined = df_combined.assign(Rating_2=0)
    if "Rating_3" not in df_combined:
        df_combined = df_combined.assign(Rating_3=0)
    if "Rating_4" not in df_combined:
        df_combined = df_combined.assign(Rating_4=0)
    if "Rating_5" not in df_combined:
        df_combined = df_combined.assign(Rating_5=0) 
        

    # Create an engine
    engine = create_engine(
        "postgresql://dst_db_project:dst_db_project@postgres_db:5432/dst_db_project"
    )
    # Use the engine in to_sql
    df_combined.to_sql("companies_tmp", engine, if_exists="replace", index=False)
    # df_combined.to_sql('companies', conn, if_exists='replace', index = True)
    return


def upsert_postrgesql_db_relationnal_companies():
    """
    This function is designed to create a relationnal database. Here's the working of the function :
    1- creation of a database using psycopg2 frame
    2- Pulling data from the scrap companie file json
    3- Creation of a dataframe using Pandas
    4- Retriving the dataframe created by using elasticsearch query
    5- Merge of the 2 dataframes into 1
    6- Pushing the dataframe into the created database
    """
    conn = psycopg2.connect(
        database="dst_db_project",
        user="dst_db_project",
        password="dst_db_project",
        host="postgres_db",
        port="5432",
    )
    # c = conn.cursor()
    # c.execute("""ALTER TABLE companies
    #                 ADD CONSTRAINT companies_index_unique UNIQUE (company_name)""")
    # conn.commit()
    c = conn.cursor()
    c.execute(
        """INSERT INTO companies (company_name, company_location, company_phone, company_email, company_verified, reviews_nbr, global_appreciation, global_rating, "Number of Comments", "Rating_1", "Rating_2", "Rating_3", "Rating_4", "Rating_5")
            SELECT company_name, company_location, company_phone, company_email, company_verified, reviews_nbr, global_appreciation, global_rating, "Number of Comments", "Rating_1", "Rating_2", "Rating_3", "Rating_4", "Rating_5"
            FROM companies_tmp
            ON CONFLICT (company_name) 
            DO UPDATE SET
            company_name = excluded.company_name,
            company_location = excluded.company_location,
            company_phone = excluded.company_phone,
            company_email = excluded.company_email,
            company_verified = excluded.company_verified,
            reviews_nbr = excluded.reviews_nbr,
            global_appreciation = excluded.global_appreciation,
            global_rating = excluded.global_rating,
            "Number of Comments" = excluded."Number of Comments",
            "Rating_1" = excluded."Rating_1",
            "Rating_2" = excluded."Rating_2",
            "Rating_3" = excluded."Rating_3",
            "Rating_4" = excluded."Rating_4",
            "Rating_5" = excluded."Rating_5" 
        """
    )

    # c.execute("DROP TABLE IF EXISTS companies_tmp")
    conn.commit()
    return


def creation_postrgesql_db_relationnal_feedbacks():
    """
        # to do
    This function is designed to create a relationnal database. Here's the working of the function :
    1- creation of a database using psycopg2 frame
    2- Pulling data from the scrap companie file json
    3- Creation of a dataframe using Pandas
    4- Retriving the dataframe created by using elasticsearch query
    5- Merge of the 2 dataframes into 1
    6- Pushing the dataframe into the created database
    """
    conn = psycopg2.connect(
        database="dst_db_project",
        user="dst_db_project",
        password="dst_db_project",
        host="postgres_db",
        port="5432",
    )
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS feedbacks")
    c.execute("""CREATE TABLE IF NOT EXISTS feedbacks (comment_id TEXT, 
            Category TEXT,
            Nature TEXT,
            Analysis TEXT )""")
    conn.commit()

    with open("/app/data/feedbacks.json", "r") as json_file:
        comments = json.load(json_file)

    df_data = {
        "comment_id": [element["comment_id"] for element in comments],
        "Category": [element["Category"] for element in comments],
        "Nature": [element["Nature"] for element in comments],
        "Analysis": [element["Analysis"] for element in comments],
    }

    df = pd.DataFrame(data=df_data)

    # Create an engine
    engine = create_engine(
        "postgresql://dst_db_project:dst_db_project@postgres_db:5432/dst_db_project"
    )
    # Use the engine in to_sql
    df.to_sql("feedbacks", engine, if_exists="replace", index=False)
    # df_combined.to_sql('companies', conn, if_exists='replace', index = True)
    return


def insert_postrgesql_db_relationnal_feedbacks():
    """
    # to do
    This function is designed to create a relationnal database. Here's the working of the function :
    1- creation of a database using psycopg2 frame
    2- Pulling data from the scrap companie file json
    3- Creation of a dataframe using Pandas
    4- Retriving the dataframe created by using elasticsearch query
    5- Merge of the 2 dataframes into 1
    6- Pushing the dataframe into the created database
    """
    conn = psycopg2.connect(
        database="dst_db_project",
        user="dst_db_project",
        password="dst_db_project",
        host="postgres_db",
        port="5432",
    )
    c = conn.cursor()

    with open("/app/data/feedbacks.json", "r") as json_file:
        feedbacks = json.load(json_file)
    print(f"Number of feedbacks to insert: {len(feedbacks)}")
    for element in feedbacks:
        comment_id = element["comment_id"]
        Category = element["Category"]
        Nature = element["Nature"]
        Analysis = element["Analysis"]
        c.execute(
            """INSERT INTO public.feedbacks(comment_id, "Category", "Nature", "Analysis")
                        VALUES (%s, %s, %s, %s);""",
            (comment_id, Category, Nature, Analysis),
        )
        conn.commit()
    print(f"Inserted: {len(feedbacks)} feedbacks")
    return


def update_postrgesql_db_relationnal_feedbacks(comment_id):
    """
    # to do
    This function is designed to create a relationnal database. Here's the working of the function :
    1- creation of a database using psycopg2 frame
    2- Pulling data from the scrap companie file json
    3- Creation of a dataframe using Pandas
    4- Retriving the dataframe created by using elasticsearch query
    5- Merge of the 2 dataframes into 1
    6- Pushing the dataframe into the created database
    """
    conn = psycopg2.connect(
        database="dst_db_project",
        user="dst_db_project",
        password="dst_db_project",
        host="postgres_db",
        port="5432",
    )
    c = conn.cursor()

    with open("/app/data/feedbacks.json", "r") as json_file:
        feedbacks = json.load(json_file)

    for element in feedbacks:
        comment_id = element["comment_id"]
        Category = element["Category"]
        Nature = element["Nature"]
        Analysis = element["Analysis"]
        c.execute(
            """INSERT INTO public.feedbacks(comment_id, "Category", "Nature", "Analysis")
                        VALUES (%s, %s, %s, %s);""",
            (comment_id, Category, Nature, Analysis),
        )
        conn.commit()

    return
