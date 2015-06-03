print "Importing modules..."
#Import modules
import requests
import json
import re
import csv
from bs4 import BeautifulSoup, SoupStrainer
import os
from time import sleep
from datetime import date, datetime, timedelta
from sets import Set
from pathos.multiprocessing import ProcessingPool as Pool
import dill

def perdelta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta

#Make search form
def make_form(search_terms, start_date, end_date, cookie):
    form_vals = {'p_action':'publications', 'p_nbid' : '', 'p_product' : 'EANX', 'p_theme' : 'ahnp', 'p_field_PTY-0' : 'PTY', 'p_field_PUBLANG-0' : 'publang', 'd_collections' : '', 'p_field_PLOC-0' : 'PLOC', 'p_bool_PLOC-0' : 'and', 'p_text_PLOC-0' : '', 'p_field_date-0' : 'YMD_date', 'p_params_date-0' : 'date:B,E', 'p_field_PUB-0' : 'ProductID', 'p_bool_PUB-0' : 'and', 'p_text_PUB-0' : '', 'p_queryname' : 1, 'p_text_base-0' : '', 'p_field_base-0' : '', 'p_bool_base-1' : 'AND', 'p_text_base-1' : '', 'p_field_base-1' : '', 'p_sort' : 'YMD_date:A', 'd_datetype' : 'custom', 'p_text_date-0' : '', 'd_dates' : '', 'd_datestext' : ''}
    date_1 = str(start_date) + " - " + str(end_date)
    date_2 = str(start_date.month) + "/" + str(start_date.day) + "/" + str(start_date.year) + " to " + str(end_date.month) + "/" + str(end_date.day) + "/" + str(end_date.year)
    form_vals.update({'p_text_base-0':search_terms, 'p_text_date-0':date_1, 'd_dates':date_1, 'd_datestext':date_2, 'p_nbid':cookie})
    return form_vals

def make_query(cookie):
    query_vals = {'p_product':'EANX', 'p_theme':'ahnp', 'p_nbid':'', 'p_action':'publications', 'p_queryname':1, 'd_hlTerms':'', 'd_customSearchFields':0, 'd_locations':'', 'd_languages':'', 'd_locations_abbrev':'', 'd_locchecks':'', 'd_dates':'', 'd_datestext':'', 'd_datetype':'', 'd_publication':'', 'd_publicationHistory':'', 'f_lochistory':'reset', 'f_datehistory':'reset', 'd_collections':''}
    query_vals.update({'p_nbid':cookie})
    return query_vals


#Get filepath
#Get file location
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
archive = "americas_historical_newspapers"
directory = "/".join((__location__,"Results",archive))
if not os.path.exists(directory):
    os.makedirs(directory)

#Starting Values
#URLs
url = "http://infoweb.newsbank.com/iw-search/we/HistArchive/"
stub = "http://infoweb.newsbank.com"

#set dates
start_date = date(1880, 1, 1)
end_date = date(1941, 1, 1)

#For day in date_list
date_list = [date for date in perdelta(start_date, end_date, timedelta(days=1))]
date_list_str = [str(date) for date in perdelta(start_date, end_date, timedelta(days=1))]

#Start session, get cookies
print "Creating session..."
s = requests.session()
wait = 0
while True:
    try:
        s.get(stub, allow_redirects=True, timeout=(1,60))
        break
    except:
        sleep(1.5**wait)
        wait += 1
        print "..."
cookie = s.cookies.items()[0][1]

#Get titles
print "Getting full title list..."
wait = 0
while True:
    try:
        get_titles = s.post(url, data=make_form("", start_date, end_date, cookie), params=make_query(cookie), cookies=s.cookies, timeout=(1,60)).text
        break
    except:
        sleep(1.5**wait)
        wait += 1
        print "..."

search_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

#parse titles into dictionary
titles = BeautifulSoup(get_titles, 'html.parser')
title_list = titles.find_all('tr', class_="slRow")

#Scraper function
def scrape_paper(paper_id, date_list):
    print paper_id
    t_stub = "http://infoweb.newsbank.com/WebLinks/listIssue/"
    date_stub = "/".join((date_list[0], date_list[-1]))
    paper_url =  "/".join((t_stub,paper_id,date_stub)) + ".json"
    #Try json
    wait = 0
    while True:
        try:
            dates_get = s.get(paper_url, params = {'style':'days'}, cookies=s.cookies, timeout=(1,60), allow_redirects=True).text
            break
        except:
            print "... trying again ..."
            sleep(1.5**wait)
            wait += 1
    paper_data = json.loads(dates_get)
    decades = [i for i in paper_data.keys() if i not in ['id']]
    day_list = ['-'.join((year,str(int(month)+1).zfill(2),day.zfill(2))) for decade in decades for year in paper_data[decade] for month in paper_data[decade][year] for day in paper_data[decade][year][month]]
    return {paper_id : Set(day_list)}

#Get paper data
print "Extacting paper data..."
papers_data = []
#Prepare title data
for title in title_list:
    line = {}
    line['archive'] = "americas_historical_newspapers"
    line['publication_title'] = title.input.text
    line['publication_id'] = title.input['value']
    city = title.find('td', class_="ci").text
    state = title.find('td', class_="st").text
    line['location'] = ", ".join((city,state))
    line['lastUpdated'] = search_datetime
    papers_data.append(line)


#Scrape publication dates in parallel
pub_ids = [paper['publication_id'] for paper in papers_data]

print "Scraping papers..."
pool = Pool(10)
result_iter = pool.imap(scrape_paper, pub_ids, [date_list_str]*len(pub_ids))
title_sets = {}
for result in result_iter:
    title_sets.update(result)

#Create file#
filename = "americas_historical_newspapers-allpubs.csv"
filepath = directory
fields = ['archive', 'publication_title', 'publication_id', 'location', 'lastUpdated'] + date_list_str

#Create output
print "Creating data rows..."
out = []
for paper in papers_data:
    title_id = paper['publication_id']
    date_match = {k : int(k in title_sets[title_id]) for k in date_list_str}
    paper.update(date_match)
    out.append([paper[k] for k in fields])

if len(out) != len(title_list):
    print "Missing papers!"

print "Writing file..."
outfile = "/".join((filepath,filename))
with open(outfile, 'wb') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(fields)
    writer.writerows(out)

print "Finished!"


