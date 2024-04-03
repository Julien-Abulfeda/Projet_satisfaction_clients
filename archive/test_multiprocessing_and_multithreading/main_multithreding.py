import time
import logging
import concurrent.futures
from scrap_company import get_companies_url, scrap_company_comments, scrap_company_data
from json_functions import add_document, clean_json_file
#from toolbox.test import test
'''
Exemple de scrabing avec 3 première entreprises

'''



def process_url(url):
    logging.info(f'Processing Company :{url}')
    print(f'Processing Company :{url}')
    company_data = scrap_company_data(url)
    comments_data = scrap_company_comments(url)
    add_document("../data/data.json", company_data, comments_data)



# Configure logging
logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# print(f' Pourcentage : {round(((cmp/len(urls))*100),2)} %  Company n°{i+1} :{url}')
urls = get_companies_url()
clean_json_file("../data/data.json")  # Cleaning the data.json file and initializing it to []

start_time = time.time()  # Start the timer

with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(process_url, urls)


end_time = time.time()  # End the timer
logging.info(f"Execution time: {end_time - start_time} seconds")
