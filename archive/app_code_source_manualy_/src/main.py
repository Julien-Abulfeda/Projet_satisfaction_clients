from get_raw_data import get_companies_raw_data
from get_preprocess_data import get_companies_preprocessed_data
from creation_db_relationnal import creation_db_relationnal
from elastic import elastic_query_rating, split_preprocessed_json, bulk_index
#from toolbox.test import test

'''
# # Exemple de scrabing avec 3 première entreprises
# # '''

# # #1) We get all companies raw data
# get_companies_raw_data()

# # #2) We get all companies preprocessed data
# get_companies_preprocessed_data()

# #3)  Create querry from elasticsearsh 
split_preprocessed_json("../data/data_preprocessed.json")
# bulk_index(index_name= "comment_id",filename="../data/comments.json" )

# bulk_index( index_name= "company_id",filename="../data/companies.json",)
# df = elastic_query_rating("comment_id")

# #4) sql creation 
# creation_db_relationnal()