print "Importing modules"
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


#Get filepath
#Get file location
__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
archive = "newspaperarchive"
directory = "/".join((__location__,"Results",archive))
if not os.path.exists(directory):
    os.makedirs(directory)

#Define functions
def perdelta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta

def get_town_urls(state):	
	print state
	wait = 0
	while True:
		try:
			get_towns = requests.get(state, timeout=(1,60)).text
			break
		except:
			print "... trying again ..."
			sleep(1.5**wait)
			wait += 1
	state_parsed = BeautifulSoup(get_towns, 'html.parser')
	towns = [a['href'] for a in state_parsed.find('div', class_='newLocUSListArea').find_all('a')]
	return towns

def get_paper_urls(town):
	print town
	wait = 0
	while True:
		try:
			get_papers = requests.get(town, timeout=(1,60)).text
			break
		except:
			print "... trying again ..."
			sleep(1.5**wait)
			wait += 1
	town_parsed = BeautifulSoup(get_papers, 'html.parser')
	papers = [a['href'] for a in town_parsed.find('div', class_='newLocUSListArea').find_all('a')]
	return papers

def get_paper_data(paper, date_set):
	print paper
	#Get years
	wait = 0
	while True:
		try:
			get_years = requests.get(paper, timeout=(1,60)).text
			search_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
			break
		except:
			sleep(1.5**wait)
			wait += 1
	paper_parsed = BeautifulSoup(get_years, 'html.parser')
	try:
		year_urls = [a['href'] for a in paper_parsed.find('div', class_='newLocUSListArea').find_all('a')]
	except:
		year_urls = []
	year_urls = [re.sub("st\.-","st-",url) for url in year_urls]
	#Get dates
	paper_dates = []
	for year in year_urls:
		wait = 0
		while True:
			try:
				get_days = requests.get(year, timeout=(1,60)).text
				break
			except:
				sleep(1.5**wait)
				wait += 1
		year_parsed = BeautifulSoup(get_days, 'html.parser')
		try:
			days = [i.text for i in year_parsed.find('div', class_="newLocUSListArea").find_all('a')]
		except:
			days = []
		paper_dates += days
	paper_dates_set = Set(paper_dates)
	if len(date_set.intersection(paper_dates_set)) > 0:
		line = {}
		line['archive'] = "newspaperarchive"
		title = re.search("[^/]+$", paper).group(0)
		line['publication_title'] = re.sub("-"," ", title).title()
		line['publication_id'] = re.sub("http://access\.newspaperarchive\.com/","",paper)
		location = re.search("([^/]+)/([^/]+)/([^/]+$)", paper).group(2,1)
		line['location'] = re.sub("-", " ", ", ".join(location)).title()
		line['lastUpdated'] = search_datetime
		line.update({k : int(k in paper_dates_set) for k in date_set})
		return line
	else:
		return None

#Starting values
nation_url = "http://access.newspaperarchive.com/us/"

#Date list
start_date = date(1880,1,1)
end_date = date(1941,1,1)
date_list = [str(date) for date in perdelta(start_date, end_date, timedelta(days=1))]
date_set = Set(date_list)

#################
#Get state links#
#################
print "Getting state URLs..."
wait = 0
while True:
	try:
		get_states = requests.get(nation_url, timeout=(1,60)).text
		break
	except:
		sleep(1.5**wait)
		wait += 1

parsed = BeautifulSoup(get_states, 'html.parser')
state_urls = [a['href'] for a in parsed.find('div', class_='newLocUSListArea').find_all('a')]

################
#Get town links#
################
print "Getting town URLs..."
pool = Pool(10)
result_iter = pool.imap(get_town_urls, state_urls)

town_urls = []
for result in result_iter:
	town_urls += result

#Clean up town URLs
town_urls = [re.sub("st\.-","st-",url) for url in town_urls]

#################
#Get paper links#
#################
print "Getting paper URLs..."
result_iter = pool.imap(get_paper_urls, town_urls)

paper_urls = []
for result in result_iter:
	paper_urls += result

#fix paper links
paper_urls = [re.sub("st\.-","st-",url) for url in paper_urls]

################
#Get paper data#
################

#Create file#
filename = "newspaperarchive-allpubs.csv"
filepath = directory
fields = ['archive', 'publication_title', 'publication_id', 'location', 'lastUpdated'] + date_list

print "Getting paper data..."
outfile = "/".join((filepath,filename))
with open(outfile, 'wb') as csvfile:
	writer = csv.writer(csvfile)
	writer.writerow(fields)
	result_iter = pool.imap(get_paper_data, paper_urls, [date_set]*len(paper_urls))
	for result in result_iter:
		if result is not None:
			out = [result[k] for k in fields]
			writer.writerow(out)

print "Finished!"

