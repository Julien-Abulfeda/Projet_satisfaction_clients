import requests as rq
from bs4 import BeautifulSoup as bs
import bson
import re
import logging



def get_companies_url() : 
    '''
    get all the companies urls
    '''
    break_loop = False
    liste_url = []
    url_trustpilot = "https://www.trustpilot.com/categories/atm"

    while break_loop == False :
        page_trustpilot = rq.get(url_trustpilot)
        bs_trustpilot = bs(page_trustpilot.content, "lxml")
        main_bs = bs_trustpilot.find_all("div",class_ = "paper_paper__1PY90 paper_outline__lwsUX card_card__lQWDv card_noPadding__D8PcU styles_wrapper__2JOo2")

        for suffixe_url in main_bs : 
            liste_url.append(f"https://www.trustpilot.com{suffixe_url.find('a')['href']}") #add of the url prefix website

        try: #test if another page needs to be scrapped
            url_next_page = bs_trustpilot.find("a",attrs={"aria-label": "Next page"})["href"]
            url_trustpilot = f"https://www.trustpilot.com{url_next_page}"
        except KeyError :
            break_loop = True
    
    return liste_url


def scrap_company_data(url):
    '''
    scrap the company data from the company url

    '''

    page = rq.get(url)
    soup = bs(page.content, "lxml")
    company = {}

    # company_id : 12-bytes Id created by ObjectId() method from bson class composed of:
        # A 4-byte timestamp, representing the ObjectId's creation, measured in seconds since the Unix epoch.
        # A 5-byte random value generated once per process. This random value is unique to the machine and process.
        # A 3-byte incrementing counter, initialized to a random value.
    company_id = str(bson.ObjectId())
    
	# company_name: XXXXXX (string)
    result = soup.find('span', class_='typography_display-s__qOjh6 typography_appearance-default__AAY17 title_displayName__TtDDM')
    # Check if result != None to avoid throwing errors
    if result is not None:
        company_name = result.text.strip()
    else:
        company_name = ""

	# reviews_nbr : XXXXX (int)
    result = soup.find('span', class_='typography_body-l__KUYFJ typography_appearance-subtle__8_H2l styles_text__W4hWi')
    # Check if result != None to avoid throwing errors
    if result is not None:
        result = result.text.strip().split()
        # Check if len(result) == 3 to avoid throwing errors if not the case
        if len(result) == 3:
            reviews_nbr = int(result[0].replace(",",""))
            global_appreciation = result[2]
        else:
            reviews_nbr = ""
            global_appreciation = ""
    else:
        reviews_nbr = ""
        global_appreciation = ""

	# global_rating : XXXXXXXXXXX (int)
    result = soup.find('p', class_='typography_body-l__KUYFJ typography_appearance-subtle__8_H2l')
    # Check if result != None to avoid throwing errors
    if result is not None:
        global_rating = float(result.text.strip())
    else:
        global_rating = ""  

	# company_categories : XXXXXXXX (list)
    result = soup.find_all('a', class_='link_internal__7XN06 typography_appearance-action__9NNRY link_link__IZzHN link_underlined__OXYVM')
    # Check if result != None to avoid throwing errors
    if result is not None :
        company_categories = []
        [company_categories.append(category.text.strip()) for category in result if category.text not in company_categories]
    else:
        company_categories = []

    # company_mail :xxxxx (string) && company_phone :xxxxx (int)
    result = soup.find_all('a', class_='link_internal__7XN06 typography_body-m__xgxZ_ typography_appearance-action__9NNRY link_link__IZzHN link_underlined__OXYVM')
    # Check if result != None to avoid throwing errors
    if (result != None) and (len(result) != 0):
        # If there is 2 results then first cell is company_mail and second is company_phone
        if len(result) == 2:
            company_email = result[0].text
            # We remove html characters with strip, remove spaces with replace and then convert to int
            company_phone = int(result[1].text.strip().replace(" ","").replace("-","").replace("(","").replace(")",""))
        # else we do not have information on company email
        elif re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', result[0].text.strip()):
            company_email = result[0].text
            # We remove html characters with strip, remove spaces with replace and then convert to int
            company_phone = ""
        else:
            
            company_email = ""
            # We remove html characters with strip, remove spaces with replace and then convert to int
            company_phone = int(result[0].text.strip().replace(" ","").replace("-","").replace("(","").replace(")",""))
    else:
        company_email = ""
        company_phone = ""


	# company_location :xxxxx (string)
    result = soup.find('ul', class_='typography_body-m__xgxZ_ typography_appearance-default__AAY17 styles_contactInfoAddressList__RxiJI')
    # Check if result != None to avoid throwing errors
    if result is not None:
        company_location = ''
        for li in result.find_all("li"):
            company_location = company_location + li.text.strip() + ' '
    else:
        company_location = ''

    # company_verified :xxxxx (bool)
    result = soup.find('div', class_='typography_body-xs__FxlLP typography_appearance-default__AAY17 typography_weight-heavy__E1LTj styles_verificationIcon___X7KO')
    
    # Check if result != None to avoid throwing errors
    if result is not None:
        if result.text.strip() == "VERIFIED COMPANY":
            company_verified = True
        else:
            company_verified = False
    else:
        company_verified = ''
    

    company.update({
        'company_id':company_id,
        'company_name':company_name,
        'reviews_nbr':reviews_nbr,
        'global_appreciation':global_appreciation,
        'global_rating':global_rating,
        'company_categories':company_categories,
        'company_location':company_location,
        'company_phone':company_phone,
        'company_email':company_email,
        'company_verified':company_verified
    })

    return company


def scrap_company_comments(url_company):
    '''
    scrap all the company data from the company url

    '''
    page = rq.get(url_company)
    soup= bs(page.content,'lxml')
    # Extract the number of pages for the given company

    result = soup.find('nav', class_ = "pagination_pagination___F1qS").find_all('a')
    
    if len(result) > 3 : 
      pages_nbr = result[-2].get('href').split("=")[1]
    else:
      pages_nbr = 1 
    comments_data = [] #  list of comment fields lists
    
    # browse all pages to extract the data of available comments    
    for i in range(int(pages_nbr)):
        url_page = url_company +"?page="+str(i+1)
        logging.info(f'Processing Company :{url_page}')
        page = rq.get(url_page)
        soup= bs(page.content,'lxml')

        # Search for all div tags that make up the content for each comment
        comments_divs = soup.find_all('div',{'class':'styles_cardWrapper__LcCPA styles_show__HUXRb styles_reviewCard__9HxJJ'})
        
        # Iteration on each comment to extract data
        for comment_div in comments_divs:
            # Extract the customer's name
            customer_name  = comment_div.find('span', class_ = "typography_heading-xxs__QKBS8 typography_appearance-default__AAY17").text
            # Extract the rating 
            rating = int(comment_div.find('div', class_ = "star-rating_starRating__4rrcf star-rating_medium__iN6Ty").img.get('alt').split(' ')[1])
            # Check if the Verified status exists otherwise "not verified" assigned
            verification_status_exist = comment_div.find('div', class_ ="typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l styles_detailsIcon__yqwWi")

            if not verification_status_exist:
                verification_status ="not verified"
            else:
                verification_status =verification_status_exist.span.text
            # Extract the customer's location
            customer_location = comment_div.find(class_ ="typography_body-m__xgxZ_ typography_appearance-subtle__8_H2l styles_detailsIcon__Fo_ua").span.text
            # Extract the comment title
            comment_title = comment_div.find('h2' , class_ = 'typography_heading-s__f7029 typography_appearance-default__AAY17').text
            # Check for the existence of a comment + Extract the content of the commen
            comment_content_exist  = comment_div.find('p' , class_ = 'typography_body-l__KUYFJ typography_appearance-default__AAY17 typography_color-black__5LYEn')
            if not comment_content_exist:
                 comment_content= "" # empty if there is no comment 
            else:
                comment_content = comment_content_exist.text
            # Extract the comment publication date
            comment_date = comment_div.find('time').get('datetime').split('T')[0]
            # Extract the customer experience date
            comment_date_of_experience  = comment_div.find(class_ ="typography_body-m__xgxZ_ typography_appearance-default__AAY17").text.split(": ")[1]
            # Check if there is a company response then assign a bool depending on the presence or absence of a response
            company_response_check = comment_div.find('div' , class_ = 'paper_paper__1PY90 paper_outline__lwsUX paper_subtle__lwJpX card_card__lQWDv card_noPadding__D8PcU styles_wrapper__ib2L5')
            if not company_response_check:
                company_response = False # There is no company response
                # Create a list with all available attributes (except the company response info we don't have)
                comment_data = {
                                    "customer_name" : customer_name,
                                    "rating" : rating ,
                                    "verification_status" : verification_status,
                                    "customer_location" : customer_location,
                                    "comment_title" :comment_title,
                                    "comment_content" : comment_content,
                                    "comment_date" : comment_date,
                                    "comment_date_of_experience" : comment_date_of_experience,
                                    "company_response" : company_response,
                                }
            else:
                company_response = True  # There is a company response
                # Extraction of the response content
                company_response_content  = company_response_check.find('p' , class_ = 'typography_body-m__xgxZ_ typography_appearance-default__AAY17 styles_message__shHhX').text
                # Extraction of the response date
                company_response_date  = company_response_check.find('time').get('datetime').split("T")[0]
                # Creation of a list with all available attributes (here with details on the company's response)
                comment_data = {
                                    "customer_name" : customer_name,
                                    "rating" : rating ,
                                    "verification_status" : verification_status,
                                    "customer_location" : customer_location,
                                    "comment_title" :comment_title,
                                    "comment_content" : comment_content,
                                    "comment_date" : comment_date,
                                    "comment_date_of_experience" : comment_date_of_experience,
                                    "company_response" : company_response,
                                    "company_response_date" :company_response_date,
                                    "company_response_content" : company_response_content
                               }
           
            comments_data.append(comment_data)
    return  comments_data
