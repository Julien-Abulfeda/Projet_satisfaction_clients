from src.extraction.get_raw_data import get_companies_url, scrap_company_raw_data


def test_list_companies_url_not_null():
    list = get_companies_url()
    # print("test")
    # print(list)
    assert len(list) > 0


def test_scrap_company_raw_data():
    company = scrap_company_raw_data("https://www.trustpilot.com/review/egcu.org", True)
    # print('\n',company)
    # print(company["comments"])
    assert len(company["comments"]) > 0
