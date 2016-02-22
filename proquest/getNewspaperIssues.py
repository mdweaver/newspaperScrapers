import requests
from datetime import date, timedelta
from time import sleep
from bs4 import BeautifulSoup
import lxml
import os
import json

#Get file location
__location__ = os.path.realpath(
	os.path.join(os.getcwd(), os.path.dirname(__file__)))

#Define functions
def perdelta(start, end, delta):
	curr = start
	while curr < end:
		yield curr
		curr += delta

def makeDate(start_date, end_date):
	return 'dc.date >= ' + "\"%s\"" % str(start_date) + ' and dc.date <= ' + "\"%s\"" % str(end_date)

def makeKeywords(search_list):
	keywords = [i for i in search_list if ' ' not in i]
	qwords = [' AND ', ' NEAR ']
	keyphrases = [i for i in search_list if ' ' in i and not any(s in i for s in qwords)]
	andwords = [i for i in search_list if ' AND ' in i]
	proxwords = [i for i in search_list if ' NEAR ' in i]
	#make keywords:
	kw_query = "(cql.serverChoice any \"%s\")" % " ".join(keywords)
	#make keyphrases:
	kp_query = ["(cql.serverChoice adj \"%s\")" % kp for kp in keyphrases]
	#and words
	and_query = ["(cql.serverChoice all \"%s\")" % aw.replace(" AND ", " ") for aw in andwords]
	#proximity query 
	prox_query = ["(cql.serverChoice=%s prox/distance<11 cql.serverChoice=%s)" % tuple(pw.split(" NEAR ")) for pw in proxwords]
		#Return word que)ry:
	return " or ".join([kw_query] + kp_query + and_query + prox_query)

def makeQuery(search_list, start_date, end_date):
	return "(%s)" % makeDate(start_date, end_date) + " and " + "(%s)" % makeKeywords(search_list)

def executeQuery(search_list, start_date, end_date, per_page = 1):
	pq_stub = "http://fedsearch.proquest.com/search/sru/subject_news"
	#query text
	query = {'operation' : 'searchRetrieve',
	 'version': 1.2,
	 'x-username' : 'mdweaver',
	 'x-password' : 'usa773107',
	 'x-navigators' : 'database',
	 'startRecord' : 1,
	 'maximumRecords' : per_page,
	 'query' : makeQuery(search_list, start_date, end_date)
	 }
	#GET request
	record_list = []
	print start_date
	while True:
		try:
			get_results = requests.get(pq_stub, params = query, timeout = (2,60))
			break
		except requests.exceptions.ConnectTimeout:
			print "Connection timed out. Trying again."
			sleep(1)
	#Parse
	parsed = BeautifulSoup(get_results.text, 'lxml')
	#Get records
	try:
		records = parsed.find_all('resultcountfordatabase')
		for record in records:
			line = {}
			line['databaseID'] = record.databaseid.get_text()
			line['databaseCode'] = record.databasecode.get_text()
			line['databaseName'] = record.databasename.get_text()
			record_list.append(line)
	except:
		pass
	#Return list of record matches
	return record_list


start_date = date(1880, 1, 1)
end_date = date(1941, 1, 1)

#Prepare list of months
days = [d for d in perdelta(start_date, end_date, timedelta(days=1))]

#
pubList = []

for d in days:
	pubs = executeQuery(['the'], d, d, 1)
	pubList.append({'date' : str(d), 'publications' : pubs})
