import json
import csv
import os
from time import sleep, strptime
from datetime import date, datetime, timedelta
import requests
from bs4 import BeautifulSoup
import re
from lxml.html import fromstring
import lxml.html
from sets import Set
from pathos.multiprocessing import ProcessingPool as Pool
import dill

#Define functions
def perdelta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta

#Get filepath
#Get file location
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
archive = "newspapers_com"
directory = "/".join((__location__,"Results",archive))
if not os.path.exists(directory):
    os.makedirs(directory)

#Set starting values
signin_url = "https://www.newspapers.com/signon.php"
get_papers_url = "http://www.newspapers.com/search/aj_getbasicfacets"
title_stub_url = "http://www.newspapers.com/title"
browse_url = "http://www.newspapers.com/browse/aj_getBrowseDocs"


#Get list of dates
  


#Start session
session = requests.session()

#Log in
signin = session.get(signin_url)
doc = lxml.html.fromstring(signin.text)
signin_form = doc.forms[0]
signin_form.fields['username'] = "email"
signin_form.fields['password'] = 'password'
session.post(signin_url, data=signin_form.form_values(), allow_redirects=True)

#################################
#Get title list for 1880 to 1940#
#################################
query_terms = {"terms":[{"type":"place","values":{"name":"place", "country":"usa", "value":"~none"}},{"type":"date","values":{"name":"year_month_day","start":"1880-01-01","end":"1880-12-31","showMissing":"false"}}]}
query_form = {"query_terms":json.dumps(query_terms), "engine":"solr"}

matches = session.post(get_papers_url, data = query_form, cookies=session.cookies, allow_redirects=True, headers={'referer' : 'http://www.newspapers.com/papers/'}, timeout=(1,60))
title_list = json.loads(matches.text)['titleData']


#############################
#Get newspapers for each day#
#############################
def scrape_day(day):
    print day
    query_terms = {"terms":[{"type":"place","values":{"name":"place", "country":"usa", "value":"~none"}},{"type":"date","values":{"name":"year_month_day","value":day,"showMissing":"true"}}]}
    query_form = {"query_terms":json.dumps(query_terms), "engine":"solr"}
    wait=0
    while True:
        try:
            m = session.post(get_papers_url, data = query_form, cookies=session.cookies, allow_redirects=True, headers={'referer' : 'http://www.newspapers.com/papers/'}, timeout=(1,60)).text
            r = json.loads(m)
            count = r['recCount']
            break
        except:
            print "... trying again ..."
            sleep(1.5**wait)
            wait += 1    
    if count > 0:
        t = r['titleData']
        return {day : Set([x['value'] for x in t])}
    else:
        return {day : Set()}

start_date = date(1880,1,1)
end_date = date(1941,1,1)
date_list = [str(date) for date in perdelta(start_date, end_date, timedelta(days=1))]


pool = Pool(10)
result_iter = pool.imap(scrape_day, date_list)
title_sets = {}
for result in result_iter:
    title_sets.update(result)

###################################
#Make dictionary of daily matches #
###################################
def scrape_paper(title_id):
    title_url = "_".join((title_stub_url, title_id))
    wait=0
    while True:
        try:
            title_get = session.get(title_url,  cookies=session.cookies, allow_redirects=True).text
            break
        except:
            print "... trying again ..."
            sleep(1.5**wait)
            wait += 1 
    title_soup = BeautifulSoup(title_get, 'html.parser')
    try:
        paper_id = re.search('(?<=#)[^#]+', title_soup.find_all('a', text="Browse")[1]['href']).group(0)
    except IndexError: 
        paper_id = None
    #get paper information
    if paper_id is not None:
        query = {'docid':paper_id,'type':'surrounding','count':'1'}
        sleep(0.5) 
        wait=0
        while True:
            try:
                title_info = session.get(browse_url, params = query, cookies=session.cookies, allow_redirects=True, headers={'referer' : 'http://www.newspapers.com/browse/'}, timeout=(1,60)).text
                break
            except:
                print "... trying again ..."
                sleep(1.5**wait)
                wait += 1
        title_info = json.loads(title_info)['nodes']
        print title_info[0]['title']
        #Make data row
        line = {}
        line['archive'] = "newspapers_com"
        line['publication_title'] = title_info[0]['title']
        line['publication_id'] = title_info[0]['publication']['publicationId']
        line['location'] = title_info[0]['displayLocation']
        line['lastUpdated'] = title_info[0]['publication']['lastUpdated']
        return line
    else:
        return None

#Create data by title
title_ids = [title['value'] for title in title_list]
print len(title_ids)

pool = Pool(10)
papers_iter = pool.imap(scrape_paper, title_ids)

#Create file#
#directory = "/Users/mdweaver/Dropbox/Newspaper Scraper/Title Lists/Results/newspapers_com"
filename = "newspapers_com-allpubs.csv"
filepath = directory
fields = ['archive', 'publication_title', 'publication_id', 'location', 'lastUpdated'] + date_list

out = []
for line in papers_iter:
    if line is not None:
        print line['publication_id']
        title_id = str(line['publication_id'])
        date_match = {k : int(title_id in title_sets[k]) for k in date_list}
        line.update(date_match)
        out.append([line[k] for k in fields])

if len(out) != len(title_ids):
    print "Missing papers!"


#Write csv
outfile = "/".join((filepath,filename))
with open(outfile, 'wb') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(fields)
    writer.writerows(out)
