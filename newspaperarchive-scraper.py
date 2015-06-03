import requests
import csv
from bs4 import BeautifulSoup, SoupStrainer
import os
from time import sleep
from datetime import date, datetime, timedelta
from pathos.multiprocessing import ProcessingPool as Pool
import dill

#Define NewspaperArchive scraper
def newspaperarchive_scraper(search_terms, start_date, end_date, filepath):
    #Define functions
    #Define date generator
    def perdelta(start, end, delta):
        curr = start
        while curr < end:
            yield curr
            curr += delta

    #Define URL grabber
    def newspaperarchive_url(search_terms, date):
        day = "&pd={0}".format(date.day)
        month = "&pm={0}".format(date.month)
        year = "&py={0}".format(date.year)
        search_terms = "&pep={0}".format(search_terms.replace(" ", "-"))
        url = "http://access.newspaperarchive.com/tags/?pci=7&ndt=ex" + day + month + year + search_terms + "&pr=100"
        return url

    def test_matches(html):
        test = BeautifulSoup(html, 'html.parser')
        test_result = test.find('h2', text="0 Results for search")
        return test_result

    def extract_articles(page):    
        #Grab articles
        articles = page.find_all('div', class_="searchResultBlock searchResultBlockWithThumb")
        return articles

    def extract_data(article, day):
        line = {}
        line['archive'] = "newspaperarchive"
        try:
            line["publication_title"] = article.h4.a.get_text().strip().encode('utf8')
        except:
            line["publication_title"] = ""
        line["href"] = article.a['href']    
        try:
            line['publication_id'] = re.search("(?<=http://access\.newspaperarchive\.com/)([^/]+/[^/]+/[^/]+/[^/]+)", line['href']).group(0)
        except:
            line['publication_id'] = ""
        line["search_date"] = day
        try:
            line['page'] = re.search("(?<=/page-)(\d\d?)", line['href']).group(0)
        except:
            line['page'] = ""
        line['search_terms'] = search_terms
        return line

    def scrape(search_terms, day):
        sleep(1)
        print day
        #Visit URL and parse
        url = newspaperarchive_url(search_terms, day)
        wait = 0
        while True:
            try:
                start = requests.get(url, timeout=(1,180)).text
                break
            except:
                print "... trying again ..." + str(day)
                sleep(1.5**wait)
                wait += 1

        #Are there any hits?
        if test_matches(start) == None:
            lines = []
            nextLink = []
            page = start
            page_number = 2

            while nextLink != None:
                soup = BeautifulSoup(page, 'html.parser')
                articles = extract_articles(soup)
                #extract article data
                for article in articles:
                    lines.append(extract_data(article, day))

                #Get nextLink
                try:
                    nextLink = soup.find('a', text=page_number)['href']
                    wait = 0
                    while True:
                        try:
                            page = requests.get(nextLink, timeout=(1,180)).text
                            break
                        except:
                            print "... trying again ..." + str(day)
                            sleep(1.5**wait)
                            wait += 1
                    page_number += 1
                except TypeError:
                    nextLink = None           

            return lines

        else:
            return None

    #Complete scraper
    #Parallel processing
    if __name__ == "__main__":
        #Create file name
        timeperiod = str(start_date) + "to" + str(end_date - timedelta(days=1))
        filename = "newspaperarchive-" + timeperiod + ".csv"

        pool = Pool(10)

        date_list = []
        for date in perdelta(start_date, end_date, timedelta(days=1)):
            date_list.append(date)

        search_terms_list = [search_terms] * len(date_list)

        result_iter = pool.imap(scrape, search_terms_list, date_list)

        #Create CSV
        fields = ["archive", "publication_title", "publication_id", "search_date", "page", "href", "search_terms"]
        with open("/".join((filepath,filename)), "w") as w:
            writer = csv.DictWriter(w, fieldnames=fields)
            writer.writeheader()
            for result in result_iter:
                if result != None:
                    writer.writerows(result)