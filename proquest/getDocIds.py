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

def executeQuery(search_list, start_date, end_date, per_page = 1000):
	pq_stub = "http://fedsearch.proquest.com/search/sru/subject_news"
	#query text
	query = {'operation' : 'searchRetrieve',
	 'version': 1.2,
	 'x-username' : 'mdweaver',
	 'x-password' : 'usa773107',
	 'startRecord' : 1,
	 'maximumRecords' : per_page,
	 'query' : makeQuery(search_list, start_date, end_date)
	 }
	#GET request
	record_list = []
	print "Extracting matches:"
	r = 1
	while r is not None:
		print str(r) + " to " + str(r+per_page-1)
		query['startRecord'] = r
		while True:
			try:
				get_results = requests.get(pq_stub, params = query, timeout = (2,60))
				break
			except requests.exceptions.ConnectTimeout:
				print "Connection timed out. Trying again."
				sleep(1)
		#Parse
		parsed = BeautifulSoup(get_results.text)
		#Get records
		records = parsed.find_all('zs:recorddata')
		for record in records:
			line = {}
			line['docID'] = record.find('controlfield', attrs = {'tag':'001'}).get_text()
			#line['date'] = record.find('datafield', attrs = {'tag':'045'}).get_text(strip=True)[1:]
			#line['publication'] = record.find('datafield', attrs = {'tag':'786'}).find('subfield', attrs = {'code':'t'}).get_text()
			record_list.append(line['docID'])
		#Get record count/position
		record_position = int( parsed.find('zs:recordposition').get_text() )
		record_count = int( parsed.find('zs:numberofrecords').get_text() )
		#print record_count
		if (record_position + per_page) < record_count:
			r = record_position + per_page
		else:
			r = None
			print "Search returned " + str(record_count)
	#Return list of record matches
	print "Collected " + str(len(record_list)) + " docIDs"
	return record_list




###################
#Search parameters#
###################

lynching_terms = ['lynching', 'lynched', 'lynch', 'lynchings', 'lyncher', 'lynchers', 
			   'lynch mob', 'judge lynch', 'lynches', 'swift justice', 'rough justice',
			   'mob law', 'mob violence', 'mob murder', 'lynch law', 'mob rule',
			   'overpowered NEAR constable', 'overpowered NEAR guard', 
			   'overpowered NEAR sheriff', 'overpowered NEAR deputy',
			   'overpowered NEAR jailer', 'taken from jail', 'hanged', 'strung up',
			   'riddled with bullets', 'masked men', 'seized NEAR jail', 'stormed NEAR jail',
			   'taken NEAR cell', 'taken NEAR courthouse', 'seized NEAR cell', 'stormed NEAR cell',
			   'seized NEAR courthouse', 'mob NEAR foiled', 'mob NEAR thwarted',
			   'vigilante justice', 'vigilance committee', 'mob killing', 'gang killing',
			   'lawless', 'lawlessness', 'sheriff NEAR powerless', 'governor NEAR powerless',
			   'authorities NEAR powerless', 'necktie party', 'posse NEAR killed'
			   ]

antilynching_terms = ['Wells Barnett', 'Ida Wells', 'Ida B Wells', 'Ida AND Wells', 
				"National Association for the Advancement of Colored People", 
				"James Weldon Johnson", "Walter White", 'James W Johnson', 
				'John Shillady', 'Du Bois', 'National AND Association AND Advancement AND Colored AND People', 
				"National AND Association AND Colored", "Advancement AND Colored AND People", 
				"NAACP", "Scottsboro", "Dyer Bill", "Wagner AND Costigan", "ASWPL", 
				"Association of Southern Women for the Prevention of Lynching", 
				"Association AND Southern AND Women AND Prevention", 
				"Southern AND Women AND Prevention", "Jessie Ames", "Afro NEAR League", 
				"Afro American League"]

deathpenalty_terms = ["capital punishment","death sentence","sentenced to death",
					"sentenced to die","sentenced to be shot","sentenced to be hanged",
					"sentenced to be electrocuted","hanged AND sentenced",
					"hanged AND scaffold","hanged AND gallows","pronounced dead",
					"hanged","executed","execution AND sentenced",
					"execution AND scaffold", "execution AND gallows",
					"hanged for murder","death warrant","warrant of death",
					"execution by gas","execution by electricity",
					"execution by guillotine","execution by strangulation",
					"execution by strangling","execution by hanging",
					"execution by electrocution","execution by lethal gas",
					"execution by shooting","execution by cloroform",
					"execution by state","death chair","electric chair",
					"judicial execution","legal execution","firing squad",
					"an execution","execution set","public execution"]


##########################
#Collect Lynching doc IDs#
##########################

lynching_search = lynching_terms + antilynching_terms

start_date = date(1880, 1, 1)
end_date = date(1941, 1, 1)

#Prepare list of months
months = [d for d in perdelta(start_date, end_date, timedelta(days=28))]

#Prepare list to hold 
documentIDs = []

for month in months:
	results = executeQuery(lynching_search, month, month + timedelta(days = 27) )
	documentIDs += results

out_lynching = {'search_terms' : lynching_search, 'date_range' : [str(start_date), str(end_date)], 'docIDs' : documentIDs}

with open('/'.join((__location__,'lynchingDocIDs.json')), 'w') as outfile:
	json.dump(out_lynching, outfile)
