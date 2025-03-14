from src.transformation.get_preprocess_data import (
    preprocess_company_data,
    preprocess_company_comments,
    get_companies_preprocessed_data,
)

import os
import json


def test_preprocess_company_data():
    test_full_data = {
        "company_id": "65d876b27fd7977c849f9a9e",
        "company_name": "Evergreen Credit Union",
        "reviews_nbr": "292",
        "global_appreciation": "Excellent",
        "global_rating": "4.9",
        "company_categories": ["Financial Institution", "ATM", "Credit Union"],
        "company_location": "225 Riverside St. 04103 Portland United States ",
        "company_phone": "207 221 5000",
        "company_email": "",
        "company_verified": True,
        "comments": [
            {
                "customer_name": "Heidi Hoffman",
                "rating": "5",
                "verification_status": "Verified",
                "customer_location": "US",
                "comment_title": "Great people and banking experiences",
                "comment_content": "I have known about Evergreen Credit Union since my grandmother was a member when it was SD Warren credit union in Westbrook. Evergreen has helped me many times with not only banking but personal & auto loans as well as my family and friends. The staff I have worked with & interacted with at Evergreen are friendly, personable and willing to help whatever my needs might be. I feel good knowing I can make a call and whomever I speak with will try to help or find someone who can, and I look forward to many more year's banking with Evergreen. Thank you!    ",
                "comment_date": "2024-02-22",
                "comment_date_of_experience": "February 21, 2024",
                "company_response": False,
            }
        ],
    }

    test_company_result = {
        "company_id": "65d876b27fd7977c849f9a9e",
        "company_name": "Evergreen Credit Union",
        "reviews_nbr": 292,
        "global_appreciation": "Excellent",
        "global_rating": 4.9,
        "company_categories": ["Financial Institution", "ATM", "Credit Union"],
        "company_location": "225 Riverside St. 04103 Portland United States ",
        "company_phone": 2072215000,
        "company_email": None,
        "company_verified": True,
    }

    assert (preprocess_company_data(test_full_data)) == test_company_result


def test_preprocess_company_comments():
    test_full_data = {
        "company_id": "65d876b27fd7977c849f9a9e",
        "company_name": "Evergreen Credit Union",
        "reviews_nbr": "292",
        "global_appreciation": "Excellent",
        "global_rating": "4.9",
        "company_categories": ["Financial Institution", "ATM", "Credit Union"],
        "company_location": "225 Riverside St. 04103 Portland United States ",
        "company_phone": "207 221 5000",
        "company_email": "",
        "company_verified": True,
        "comments": [
            {
                "customer_name": "Heidi Hoffman",
                "rating": "5",
                "verification_status": "Verified",
                "customer_location": "US",
                "comment_title": "Great people and banking experiences",
                "comment_content": "I have known about Evergreen Credit Union since my grandmother was a member when it was SD Warren credit union in Westbrook. Evergreen has helped me many times with not only banking but personal & auto loans as well as my family and friends. The staff I have worked with & interacted with at Evergreen are friendly, personable and willing to help whatever my needs might be. I feel good knowing I can make a call and whomever I speak with will try to help or find someone who can, and I look forward to many more year's banking with Evergreen. Thank you!    ",
                "comment_date": "2024-02-22",
                "comment_date_of_experience": "February 21, 2024",
                "company_response": False,
            }
        ],
    }

    test_comments_result = [
        {
            "comment_id": 1,
            "customer_name": "Heidi Hoffman",
            "rating": 5,
            "verification_status": "Verified",
            "customer_location": "US",
            "comment_title": "Great people and banking experiences",
            "comment_content": "I have known about Evergreen Credit Union since my grandmother was a member when it was SD Warren credit union in Westbrook. Evergreen has helped me many times with not only banking but personal & auto loans as well as my family and friends. The staff I have worked with & interacted with at Evergreen are friendly, personable and willing to help whatever my needs might be. I feel good knowing I can make a call and whomever I speak with will try to help or find someone who can, and I look forward to many more year's banking with Evergreen. Thank you!    ",
            "comment_date": "2024-02-22",
            "comment_date_of_experience": "2024-02-21",
            "company_response": False,
        }
    ]

    result = preprocess_company_comments(test_full_data)
    # print(result)

    assert (result) == test_comments_result


def test_get_companies_preprocessed_data():
    data_test = [
        {
            "company_id": "6602824d5cc1cb08b8bdc698",
            "company_name": "Evergreen Credit Union",
            "reviews_nbr": 302,
            "global_appreciation": "Excellent",
            "global_rating": 4.8,
            "company_categories": ["Financial Institution", "ATM", "Credit Union"],
            "company_location": "225 Riverside St. 04103 Portland United States ",
            "company_phone": 2072215000,
            "company_email": None,
            "company_verified": True,
            "comments": [
                {
                    "comment_id": 1,
                    "customer_name": "Joshua Walton",
                    "rating": 5,
                    "verification_status": "Verified",
                    "customer_location": "US",
                    "comment_title": "My call to Member Services was answered\u2026",
                    "comment_content": "My call to Member Services was answered immediately. Rebecca fixed my issue promptly and walked me through the process until it was resolved. I appreciate this level of customer service. It is rarer these days and certainly doesn't go unnoticed. Thank you!",
                    "comment_date": "2024-03-22",
                    "comment_date_of_experience": "2024-03-21",
                    "company_response": False,
                },
                {
                    "comment_id": 2,
                    "customer_name": "Bri Mor",
                    "rating": 5,
                    "verification_status": "Verified",
                    "customer_location": "US",
                    "comment_title": "Jennel Berry was such a huge help\u2026",
                    "comment_content": "Jennel Berry was such a huge help though out the entire process.  I mentioned during the first meeting that I was nervous, Jennel calmly walked me through the initial process. ",
                    "comment_date": "2024-03-14",
                    "comment_date_of_experience": "2024-03-14",
                    "company_response": False,
                },
                {
                    "comment_id": 3,
                    "customer_name": "Christopher",
                    "rating": 5,
                    "verification_status": "Invited",
                    "customer_location": "US",
                    "comment_title": "I joined ECU when I lived in Maine",
                    "comment_content": "I joined ECU when I lived in Maine. Everyone was so helpful, that I decided to keep my account open when I moved to PA. Here I am almost 10 years later living in a different state and I will never switch to a local bank. Lindsay Nunley helped me finance a fun new toy very quickly.  She is one of the most helpful people that I have worked with.  Reminded me why I bank with you guys. Thanks Lindsay!",
                    "comment_date": "2020-07-31",
                    "comment_date_of_experience": "2020-07-31",
                    "company_response": True,
                    "company_response_date": "2020-07-31",
                    "company_response_content": "Hi Christopher! Thank you for your review. We're so glad you've chosen to stick with us - even in another state! Lindsay and all of us here at Evergreen are happy to help. Please don't hesitate to reach out if we can assist you in the future. Thanks again, and have a great weekend!Evergreen Credit Union",
                    "response_duration": 0,
                },
            ],
        },
        {
            "company_id": "660282575cc1cb08b8bdc699",
            "company_name": "Liberty First Credit Union",
            "reviews_nbr": 270,
            "global_appreciation": "Excellent",
            "global_rating": 4.8,
            "company_categories": [
                "Credit Union",
                "ATM",
                "Car Finance and Loan Company",
                "Financial Institution",
                "Mortgage Lender",
            ],
            "company_location": "501 N. 46th St 68503 Lincoln United States ",
            "company_phone": 4024651000,
            "company_email": "info@libertyfirstcu.com",
            "company_verified": True,
            "comments": [
                {
                    "comment_id": 1,
                    "customer_name": "Jason B",
                    "rating": 5,
                    "verification_status": "Verified",
                    "customer_location": "US",
                    "comment_title": "I\u2019d give liberty first 5 starts any day\u2026",
                    "comment_content": "I\u2019d give liberty first 5 starts any day of the week! Everyone was super helpful and they made my first time loan experience fast and easy. I have and will recommend them to anyone I can ",
                    "comment_date": "2024-03-25",
                    "comment_date_of_experience": "2024-03-07",
                    "company_response": True,
                    "company_response_date": "2024-03-25",
                    "company_response_content": "Jason we are so happy to hear that you had a great experience.  Thank you for being a member of Liberty First.",
                    "response_duration": 0,
                },
                {
                    "comment_id": 2,
                    "customer_name": "customerPaula Allen",
                    "rating": 5,
                    "verification_status": "Verified",
                    "customer_location": "US",
                    "comment_title": "I  had to identify myself first that is\u2026",
                    "comment_content": "I  had to identify myself first that is a plus and the communication is the greatest.",
                    "comment_date": "2024-03-24",
                    "comment_date_of_experience": "2024-03-06",
                    "company_response": True,
                    "company_response_date": "2024-03-25",
                    "company_response_content": "We are so glad you had a great experience. ",
                    "response_duration": 1,
                },
                {
                    "comment_id": 3,
                    "customer_name": "Sassy B's Delightful Desserts",
                    "rating": 5,
                    "verification_status": "Invited",
                    "customer_location": "US",
                    "comment_title": "Always a great experience each time I\u2026",
                    "comment_content": "Always a great experience each time I either talk to somebody or go in",
                    "comment_date": "2021-08-16",
                    "comment_date_of_experience": "2021-08-16",
                    "company_response": True,
                    "company_response_date": "2021-08-16",
                    "company_response_content": "Thank you. We appreciate your feedback Sassy B's Delightful Desserts!",
                    "response_duration": 0,
                },
            ],
        },
    ]

    filepath_preprocessed_data = "./dags/src/test/data_test/data_test_preprocessed.json"

    if os.path.exists(filepath_preprocessed_data):
        os.remove(filepath_preprocessed_data)
        print("\n", "The file does exist")
    else:
        print("\n", "The file does not exist")

    get_companies_preprocessed_data(True)

    with open(filepath_preprocessed_data, "r") as json_file:
        json_content = json.load(json_file)
        assert json_content == data_test
