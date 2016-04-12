import requests
from lxml.html import fromstring
import lxml.html
import json
from json import dumps
import csv
import os
from time import sleep
from datetime import date, datetime, timedelta
from pathos.multiprocessing import ProcessingPool as Pool
import dill

def newspapers_com_scraper(search_terms, start_date, end_date, filepath):
    
    #Set starting values
    #Set URLs
    signin_url = "https://www.newspapers.com/signon.php"
    search_url = "http://www.newspapers.com/search/aj_getresults"
    search_content_url = "http://www.newspapers.com/search/aj_getsearchrecord"

    

    #Scraper Functions
    #Define date generator
    def perdelta(start, end, delta):
        curr = start
        while curr < end:
            yield curr
            curr += delta

    #Make search query
    def make_search_query(search_terms, search_date, count):
        query_terms = {"terms":[{"type":"keyword","values":{"value":search_terms}},{"type":"date","values":{"name":"year_month_day","value":str(search_date),"showMissing":"true"}}, {"type":"field", "values":{"name":"place","value":"United States of America"}}]}
        query_form = {"query_terms":dumps(query_terms), "start":0, "count":count, "engine":"solr", "sort":"score desc"}
        return query_form

    #Create record dictionary for content search
    def make_record_dict(records):
        out = []
        for record in records:
            temp = {}
            temp['records'] = [record['records'][0]]
            temp['rollup'] = record['id']
            out.append(temp)
        return out

    def get_content(records):
        groups = 1
        while True:
            try:
                records_list = []
                for group in range(groups):
                    records_list.append(records[group::groups])
                articles = []
                for records_group in records_list:
                    record_dict = make_record_dict(records_group)
                    content_query_form = {'records':dumps(record_dict), 'highlight_terms':search_terms.replace(" ", "|"), 'nonKeywordView' : 'false'}
                    
                    wait = 0
                    while True:
                        try:
                            content = session.post(search_content_url, data = content_query_form, cookies=session.cookies, allow_redirects=True, headers={'referer' : 'http://www.newspapers.com/search/'}, timeout=(1,60)).text
                            break
                        except:
                            print "... trying again ..."
                            sleep(1.5**wait)
                            wait += 1
                    articles = articles + json.loads(content)['records']
                break
            except ValueError:
                groups += 1
        return articles

    #Get article attributes
    def get_from_object(obj, *keys):
        try:
            value = obj
            for k in keys:
                if isinstance(value, dict):
                    value = value.get(k)
                elif isinstance(value, list) and len(value)>1:
                    value = (item for item in value if item['name'] == k).next()['value']
                elif isinstance(value, list) and len(value)==1:
                    value = value[k]
            return value
        except:
            return ''

    #Extract article data
    def get_article_data(record, search_date):
        line = {}
        line['archive'] = 'newspapers_com'
        line['publication_id'] = get_from_object(record, 'rec', 'cover', 'publicationId')
        line['publication_title'] = get_from_object(record, 'rec', 'pubMetadata', 'publication-title')
        line['search_date'] = search_date
        line['page'] = get_from_object(record, 'rec', 'cover', 'title')
        line['href'] = "http://www.newspapers.com/image/" + str(record['rec']['cover']['id']) + "/?terms=" + record['terms']
        line['search_terms'] = search_terms
        return line

    #Scrape function
    def scrape(search_terms, day):
        sleep(1)
        print day

        #Create search query
        query_form = make_search_query(search_terms, day, 1000)

        #POST search query
        wait = 0
        while True:
            try:
                matches = session.post(search_url, data = query_form, cookies=session.cookies, allow_redirects=True, headers={'referer' : 'http://www.newspapers.com/search/'}, timeout=(1,60)).text
                break
            except:
                print "... trying again ..."
                sleep(1.5**wait)
                wait += 1

        #Create search content query
        results = json.loads(matches)
        if results['recCount'] > 0:
            #records = make_record_dict(results['records'])
            #print "Made "
            
            #Get articles
            articles = get_content(results['records'])

            lines = []
            for article in articles:
                lines.append(get_article_data(article, day))

            return lines
        else:
            return None

    #Complete Scraper
    date_list = [str(date) for date in perdelta(start_date, end_date, timedelta(days=1))]
    #Start session
    session = requests.session()

    #Log in
    signin = session.get(signin_url)
    doc = lxml.html.fromstring(signin.text)
    signin_form = doc.forms[0]
    signin_form.fields['username'] = "email"
    signin_form.fields['password'] = "password"
    session.post(signin_url, data=signin_form.form_values(), allow_redirects=True)

    #Create CSV
    #Create file name
    timeperiod = str(start_date) + "to" + str(end_date - timedelta(days=1))
    filename = "newspapers_com-" + timeperiod + ".csv"
    fields = ["archive", "publication_title", "publication_id", "search_date", "page", "href", "search_terms"]
    
    pool = Pool(10)
    results_iter = pool.imap(scrape, [search_terms]*len(date_list), date_list)

    with open("/".join((filepath,filename)), "w") as w:
        writer = csv.DictWriter(w, fieldnames=fields)
        writer.writeheader()
        #Loop over days
        for results in results_iter:
            if results != None:
                writer.writerows(results)
