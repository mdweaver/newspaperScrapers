#Import modules
import requests
import re
import csv
from bs4 import BeautifulSoup, SoupStrainer
import os
from time import sleep
from datetime import date, datetime, timedelta

def americas_historical_newspapers_scraper(search_terms, start_date, end_date, filepath):
    #Starting Values
    #URLs
    url = "http://infoweb.newsbank.com/iw-search/we/HistArchive/"
    stub = "http://infoweb.newsbank.com"

    #Functions
    #Define date generator
    def perdelta(start, end, delta):
        curr = start
        while curr < end:
            yield curr
            curr += delta

    #Make search form
    def make_form(search_terms, day, cookie):
        form_vals = {'p_nbid' : '', 'p_product' : 'EANX', 'p_theme' : 'ahnp', 'p_field_PTY-0' : 'PTY', 'p_field_PUBLANG-0' : 'publang', 'd_collections' : '', 'p_field_PLOC-0' : 'PLOC', 'p_bool_PLOC-0' : 'and', 'p_text_PLOC-0' : '', 'p_field_date-0' : 'YMD_date', 'p_params_date-0' : 'date:B,E', 'p_field_PUB-0' : 'ProductID', 'p_bool_PUB-0' : 'and', 'p_text_PUB-0' : '', 'p_queryname' : 1, 'p_text_base-0' : '', 'p_field_base-0' : '', 'p_bool_base-1' : 'AND', 'p_text_base-1' : '', 'p_field_base-1' : '', 'p_sort' : 'YMD_date:A', 'd_datetype' : 'custom', 'p_text_date-0' : '', 'd_dates' : '', 'd_datestext' : ''}
        date_1 = str(day) + " - " + str(day)
        date_2 = str(day.month) + "/" + str(day.day) + "/" + str(day.year)
        form_vals.update({'p_text_base-0':search_terms, 'p_text_date-0':date_1, 'd_dates':date_1, 'd_datestext':date_2, 'p_nbid':cookie})
        return form_vals

    #Make query
    def make_query(cookie):
        query_vals = {'p_product':'EANX', 'p_theme':'ahnp', 'p_nbid':'', 'p_action':'search', 'p_queryname':1, 'd_hlTerms':'', 'd_customSearchFields':0, 'd_locations':'', 'd_languages':'', 'd_locations_abbrev':'', 'd_locchecks':'', 'd_dates':'', 'd_datestext':'', 'd_datetype':'', 'd_publication':'', 'd_publicationHistory':'', 'f_lochistory':'reset', 'f_datehistory':'reset', 'd_collections':''}
        query_vals.update({'p_nbid':cookie})
        return query_vals

    #Parsing functions
    def get_article_text(article, text):
        match = article.find('div', text=re.compile(text))
        if match == None:
            return ""
        else:
            return match.next_sibling.next_sibling.get_text()

    #Extract article data
    def get_article_data(article):
        line = {}
        line['archive'] = "americashistoricalnewspapers"
        line['publication_title'] = get_article_text(article, "Published as").encode('utf8')
        line['href'] = article.find('a', text=re.compile("View Article"))['href']
        line['publication_id'] = re.search("(?<=:)([^:@]+)(?=@EANX)", line['href']).group(0)
        line["date.search"] = day
        page_no = article.div.get_text(strip=True)
        page_no = page_no.split(",")[1]
        line['page'] = " ".join(page_no.split()).encode('utf8')
        line['search_terms'] = search_terms
        return line

    #Scrape function
    def scrape(search_terms, day):
        sleep(0.05)
        print day

        #Open first page
        wait = 0
        while True:
            try:
                start = s.post(url, data=make_form(search_terms, day, cookie), params=make_query(cookie), cookies=s.cookies, timeout=(1,60))
                break
            except:
                print "... trying again ..."
                sleep(1.5 ** wait)
                wait += 1

        #Parse first page
        soup = BeautifulSoup(start.text, 'html.parser')

        #Check if there are results
        if soup.find('div', id="sorryBlock")==None:
            #Current page
            page = soup
            nextLink = []
            lines = []

            while nextLink != None:
                #Get articles
                articles = page.find_all('div', class_="articleLeft")

                for article in articles:
                    try:
                        lines.append(get_article_data(article))
                    except:
                        pass

                #Find url for next page
                try:
                    #Get next link
                    nextLink = stub + page.find('a', text="Next")['href']

                    #Open next link
                    wait = 0
                    while True:
                        try:
                            next_page = s.get(nextLink, cookies=s.cookies, timeout=(1,60))
                            break
                        except:
                            print "... trying again ..."
                            sleep(1.5**wait)
                            wait += 1
                    #Parse next link        
                    page = BeautifulSoup(next_page.text, 'html.parser')
                except:
                    nextLink = None

            return lines

        else:
            return None


    #Complete scraper    
    #Start session, get cookies
    s = requests.session()
    s.get(stub, allow_redirects=True)
    cookie = s.cookies.items()[0][1]

    #Create CSV
    #Create file name
    timeperiod = str(start_date) + "to" + str(end_date - timedelta(days=1))
    filename = "americas_historical_newspapers-" + timeperiod + ".csv"
    fields = ["archive", "publication_title", "publication_id", "search_date", "page", "href", "search_terms"]
    with open("/".join((filepath,filename)), "w") as w:
        writer = csv.DictWriter(w, fieldnames=fields)
        writer.writeheader()

        for day in perdelta(start_date, end_date, timedelta(days=1)):
            results = scrape(search_terms, day)
            if results != None:
                writer.writerows(results)