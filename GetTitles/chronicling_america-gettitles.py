#Import modules
print "Importing modules..."
import json
import csv
import os
import re
from time import sleep, strptime
from datetime import date, datetime, timedelta
import requests
from bs4 import BeautifulSoup
import lxml.html
from sets import Set
from pathos.multiprocessing import ProcessingPool as Pool
import dill
from fuzzywuzzy import process

#Get filepath
#Get file location
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
archive = "chronicling_america"
directory = "/".join((__location__,"Results",archive))
if not os.path.exists(directory):
    os.makedirs(directory)

#Define functions
def perdelta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta

#Set values
stub = "http://chroniclingamerica.loc.gov"

#Date list
start_date = date(1880,1,1)
end_date = date(1841,1,1)
date_list = [str(date) for date in perdelta(start_date, end_date, timedelta(days=1))]

#Get list of digitized papers
print "Obtaining list of papers..."
get_url = stub + "/newspapers/"
get_list = requests.get(get_url).text

parsed = BeautifulSoup(get_list, 'html.parser')

paper_list = parsed.find('tbody').find_all('tr')

paper_stubs = []
for paper in paper_list:
    paper_stub = paper.find_all('a')[1]['href'][:-1]
    paper_stubs.append(paper_stub)

#Define paper scraper
def scrape_paper(paper_stub, date_list):
    print paper_stub
    #Create paper_url
    paper_url = stub + paper_stub + ".json"
    #Try to get json
    wait = 0
    while True:
        try:
            paper_get = requests.get(paper_url, timeout=(1,60)).text
            paper_data = json.loads(paper_get)
            search_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            break
        except:
            print "... trying again ..."
            sleep(1.5**wait)
            wait += 1
    #Extract data
    line = {}
    line['archive'] = "chronicling_america"
    line['publication_title'] = paper_data['name']
    line['publication_id'] = paper_data['lccn']
    #Get publication location
    if len(paper_data['place']) == 1:
        location_raw = paper_data['place'][0]
    else:
        pub_place = paper_data['place_of_publication']
        place_list = paper_data['place']
        location_raw = process.extract(pub_place, place_list, limit=1)[0][0]
    city = re.search("(?<=--)[^(--)]+$", str(location_raw)).group(0)
    state = re.search("^[^(--)]+(?=--)", str(location_raw)).group(0)
    line['location'] = ", ".join((city, state))
    line['lastUpdated'] = search_datetime
    #Get paper publication dates
    paper_date_set = Set([x['date_issued'] for x in paper_data['issues']])
    date_match = {k : int(k in paper_date_set) for k in date_list}
    line.update(date_match)
    return line




#Scrape publication data
print "Getting publication data..."
pool = Pool(10)
result_iter = pool.imap(scrape_paper, paper_stubs, [date_list]*len(paper_stubs))
lines = []
for result in result_iter:
    lines.append(result)

#Prepare for write
filename = "chronicling_america-allpubs.csv"
filepath = directory
fields = ['archive', 'publication_title', 'publication_id', 'location', 'lastUpdated'] + date_list

print "Creating data rows..."
out = []
for line in lines:
    line['publication_title'] = line['publication_title'].encode('utf8')
    line['location'] = line['location'].encode('utf8')
    out.append([line[k] for k in fields])

"Writing file..."
outfile = "/".join((filepath,filename))
with open(outfile, 'wb') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(fields)
    writer.writerows(out)

print "Finished!"
