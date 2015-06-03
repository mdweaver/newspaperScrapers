import requests
import re
import csv
from bs4 import BeautifulSoup, SoupStrainer
import os
from time import sleep
from datetime import date, datetime, timedelta

#search_terms is a string of words separated by spaces. 
#This function will only perform a search for articles that mention any word in the string

#start_date and end_date are date objects created by the datetime module

#filepath is a string containing the path where the resulting csv should be written.

def chronicling_america_scraper(search_terms, start_date, end_date, filepath):
    #set starting values
    #set URLs
    url = "http://chroniclingamerica.loc.gov/search/pages/results/"
    stub = "http://chroniclingamerica.loc.gov"

    #Scraper Functions
    #Define date generator
    def perdelta(start, end, delta):
        curr = start
        while curr < end:
            yield curr
            curr += delta

    def make_search_query(search_terms, day, count):
        query_vals = {'dateFilterType':'range', 'date1':'', 'date2':'', 'language':'', 'ortext':'', 'andtext':'', 'phrasetext':'', 'proxtext':'', 'proxdistance':5, 'rows':count, 'searchType':'advanced'}
        date_1 = str(day.month) + "/" + str(day.day) + "/" + str(day.year)
        query_vals.update({'phrasetext':search_terms, 'date1':date_1, 'date2':date_1})
        return query_vals

    def get_article_data(article):
        matches = article.find('input', {'name':'words'})
        if matches['value'].lower() is search_terms.lower():
            line = {}
            line['archive'] = 'chronicling_america'
            m = re.search('^[^.]+(?=.)', info)
            line['publication_title'] = m.group(0).encode('utf8')
            line['publication_id'] = article.a['href'].split('/')[2]
            line['search_date'] = day
            p = re.search('(?<=Page\s)[0-9]+?', info)
            i = re.search('(?<=Image\s)[0-9]+?', info)
            if p:
                line['page'] = p.group(0).encode('utf8')
            else:
                line['page'] = i.group(0).encode('utf8')
            line['href'] = stub + article.a['href']
            line['search_terms'] = search_terms         
            return line
        else:
            return None

    def scrape(search_terms, day):
        print day

        #Search  
        wait = 0
        while True:
            try:
                start_page = requests.get(url, params=make_search_query(search_terms, day, 500), timeout=(1,60)).text
                break
            except:
                print "... trying again ..."
                sleep(1.5**wait)
                wait += 1

        test = BeautifulSoup(start_page, 'html.parser')

        if test.find('span', style = 'color:#900')==None:
            page = test
            nextLink = []
            lines = []

            while nextLink != None:
                #Select articles
                results = page.find('table')
                while True:
                    try:
                        articles = results.find_all('div', class_="highlite")
                        break
                    except AttributeError:
                        pass

                #Extract information on articles
                for article in articles:
                    data = get_article_data(article)
                    if data != None:
                        lines.append(data)
                    else:
                        pass
                #Find url for next page
                try:
                    nextLink = url + page.find('a', class_='next')['href']
                    wait = 0
                    while True:
                        try:
                            next_page = requests.get(nextLink, timeout=(1,60)).text
                            break
                        except:
                            print "... trying again ..."
                            sleep(1.5**wait)
                            wait += 1
                    page = BeautifulSoup(next_page, 'html.parser')
                except:
                    nextLink = None
            return lines
        else:
            return None


    #Complete scraper
    last_date = end_date - timedelta(days=1)
    if last_date.year < 1923:

        #Create CSV
        #Create file name
        timeperiod = str(start_date) + "to" + str(end_date - timedelta(days=1))
        filename = "chronicling_america-" + timeperiod + ".csv"
        fields = ["archive", "publication_title", "publication_id", "search_date", "page", "href", "search_terms"]
        with open("/".join((filepath,filename)), "w") as w:
            writer = csv.DictWriter(w, fieldnames=fields)
            writer.writeheader()

            #Loop over days
            for day in perdelta(start_date, end_date, timedelta(days=1)):
                results = scrape(search_terms, day)
                if results != None:
                    writer.writerows(results)
    else:
        print "Cannot search dates after 1922."