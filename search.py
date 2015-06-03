from datetime import date, datetime, timedelta
import os

#Get file location
__location__ = os.path.realpath(
	os.path.join(os.getcwd(), os.path.dirname(__file__)))

#List of archives
archives = ["chronicling_america"]
#["americas_historical_newspapers", "chronicling_america", "newspaperarchive", "newspapers_com"]

#Create folders for results
for archive in archives:
	directory = "/".join((__location__,"Results",archive))
	if not os.path.exists(directory):
		os.makedirs(directory)

#Import scraper functions
scraper_files = [s + "-scraper.py" for s in archives]
for scraper in scraper_files:
	execfile("/".join((__location__,"Scrapers",scraper)))

#dictionary of archives
#filepath out, scraper function
archive_dict = {}
for archive in archives:
	filepath_out = "/".join((__location__,"Results",archive))
	func = eval(archive + "_scraper")
	archive_dict[archive] = {"filepath":filepath_out, "scraper":func}


years = range(1880,1881)

search_terms = "lynching"

for year in years:
	start_date = date(year, 1, 1)
	end_date = date(year + 1, 1, 1)

	for key in archive_dict:
		print key
		filepath = archive_dict[key]['filepath']
		scraper = archive_dict[key]['scraper']

		scraper(search_terms, start_date, end_date, filepath)