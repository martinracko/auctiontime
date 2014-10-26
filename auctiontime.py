# -*- coding: utf-8 -*-
from _testcapi import test_datetime_capi
from pymongo.errors import DuplicateKeyError

__author__ = 'mr'

import sys
import datetime
import random
import time
import re
import traceback
from xml.etree import ElementTree as ET
from configparser import  ConfigParser
from PyQt4.QtGui import *
from PyQt4.QtCore import *
from PyQt4.QtWebKit import *
from PyQt4.QtNetwork import *
from bs4 import BeautifulSoup
from pymongo import MongoClient

class CrawlerConfig(ConfigParser):
    def __init__(self):
        ConfigParser.__init__(self)
        self.id = None
        self.name = None
        self.on = False
        self.force = False
        self.ttl = 365
        self.sleep = 0

    def parseCrawlerConfig(self):
        file = cfg.get('main', 'crawler-config')
        id = cfg.get('main', 'crawler-id')
        root = ET.parse(file).getroot()
        mbElem = root.find("crawler/[@id='" + id + "']")
        self.id = id
        self.on = True if mbElem.attrib['status'] == '1' else False
        self.name = mbElem.find('name').text
        self.sleep = int(int(mbElem.find('sleep').text) / 1000)
        self.ttl = int(mbElem.find('ttl').text)


class Crawler(QWebView):
    def __init__(self, app, cfg):
        self.app = app
        self.cfg = cfg
        self.sitemap = []
        self.modelList = []
        self.listings = []
        self.nextList = None
        self.nextPage = None
        self.nextModified = None
        self.baseUrl = "http://www.auctiontime.com"
        self.requests = 0
        self.noTitle = 0
        QWebView.__init__(self)
        self.loadFinished.connect(self._loadFinished)
        uri = "mongodb://" + \
              cfg.get('mongo', 'user') + ":" + \
              cfg.get('mongo', 'pass') + "@" + \
              cfg.get('mongo', 'host') + ":" + \
              cfg.get('mongo', 'port') + "/" + \
              cfg.get('mongo', 'db')
        client = MongoClient(uri)
        self.db = client[cfg.get('mongo', 'db')]
        doc = self.db['meta.auctiontime'].find_one()
        if doc is not None:
            self.round = doc['round'] if doc['round'] is not None else 0
        else:
            self.round = 0

    def _loadFinished(self):
        html = self.page().mainFrame().toHtml()
        soup = BeautifulSoup(html)
        #if this is a content page, recognize page type from URL and parse the relevant information
        if soup.title != None:
            self.noTitle = 0
            if soup.title.string.strip() == 'ERROR':
                #this session was terminated by server, need to restart crawling with new session (handled by external cron)
                self.saveMetaData()
                self.terminate('Error page')
            try:
                url = self.url().toString()
                if '/manulist.aspx' in url:
                    self.parseSitemap(soup)
                elif '/modellist.aspx' in url:
                    self.parseModelList(soup)
                elif '/list.aspx' in url:
                    self.parseList(soup)
                elif '/Details.aspx' in url:
                    self.parseListing(soup)
                elif 'registration/passport.aspx' in url:
                    # this is a known but not interesting page type
                    self.log('Not interested in this type of page: ' + url)
                else:
                    self.nextPage = None
                    self.saveMetaData()
                    self.terminate("Unknown page type: " + url)
                self.proceed()
            except Exception as e:
                self.terminate(format_exception(e))
        else:
            self.log("Page doesn't contain a title, considered not a finished page loading (expects redirection, ...)")
            self.log(html)
            self.noTitle += 1
            if self.noTitle > 1 or html == '<html><head></head><body></body></html>':
                self.nextPage = None #not to get to the same No Title situation in case of immediate termination
                self.nextModified = None
                self.log("Too many No Title pages or a corrupt page, proceeding to the next page")
                self.proceed()

    def proceed(self):
        self.saveMetaData()
        self.nextPage = None
        self.nextModified = None
        self.cfg.parseCrawlerConfig()
        if not self.cfg.on and not self.cfg.force:
            self.terminate("Via configuration file")
        self.requests += 1
        self.log("Number of requests: " + str(self.requests) + "/" + str(self.cfg.getint("main", "max-requests")))
        if self.requests >= self.cfg.getint("main", "max-requests"):
            self.terminate("Max requests exceeded: " + str(self.requests))
        else:
            self.loadNextPage()

    def loadNextPage(self):
        time.sleep(self.cfg.sleep)
        if self.nextPage != None:
            self.log("Loading next page directly: " + self.nextPage)
            self.load(QUrl(self.nextPage))
        else:
            if len(self.modelList) > 0:
                self.nextPage = self.modelList.pop(0)
            elif len(self.listings) > 0:
                record = self.listings.pop(0)
                self.nextPage = record["url"]
                self.nextModified = record["modified"]
            elif self.nextList != None:
                self.nextPage = self.nextList
            elif len(self.sitemap) > 0:
                self.nextPage = self.sitemap.pop(0)
            else: # end of round
                self.log("End of round: " + str(self.round))
                self.round += 1
                self.nextPage = None
                self.saveMetaData()
                self.terminate("End of round")
            if self.nextPage is not None:
                self.log("Next page chosen from meta data: " + self.nextPage)
                self.load(QUrl(self.nextPage))

    def parseSitemap(self, soup):
        self.log('Parsing sitemap: ' + self.url().toString())
        mans = soup.find(id='ctl00_ContentPlaceHolder1_DrillDown1_trInformation')
        if mans != None:
            links = mans.find_all('a')
            cnt = len(links)
            for i in range(0, cnt):
                text = links[i].string
                pattern = re.compile("\(([0-9]+)\)")
                listingsCnt = int(pattern.search(text).group(1))
                href = links[i]['href']
                if listingsCnt < 10000:
                    self.sitemap.append(self.baseUrl + href.replace('drilldown/modellist.aspx', 'list/list.aspx'))
                else:
                    self.modelList.append(self.baseUrl + href)
            random.shuffle(self.sitemap)
        else:
            self.terminate('No manufacturers to parse in manufacturer list')

    def parseModelList(self, soup):
        self.log('Parsing model list: ' + self.url().toString())
        mans = soup.find(id='ctl00_ContentPlaceHolder1_DrillDown1_trInformation')
        if mans != None:
            links = mans.find_all('a')
            cnt = len(links)
            for i in range(0, cnt):
                href = links[i]['href'];
                if 'mdlx=exact' in href:
                    self.sitemap.append(self.baseUrl + href)
            random.shuffle(self.sitemap)
        else:
            self.terminate('No models to parse in model list')

    def parseList(self, soup):
        self.log('Parsing List: ' + self.url().toString())
        links = soup.find_all('a')
        modifieds = soup.find_all("span", {"class": "smallblack"})
        cnt = len(links)
        self.listings = []
        listingsCnt = 0
        duplicatesCnt = 0
        used = []
        i = 0
        for link in links:
            url = None
            try:
                url = self.baseUrl + link['href']
            except:
                pass
            if url is not None \
                    and url not in used \
                    and re.match('http://www\.auctiontime\.com/OnlineAuctions/Details\.aspx\?OHID=[0-9]+&lp=mat$', url) is not None:
                used.append(url)
                if not self.isDuplicateListing(url):
                    listingsCnt += 1
                    pattern = re.compile("([0-9]{1,2}/[0-9]{1,2}/[0-9]{1,4})")
                    date = datetime.datetime.strptime(pattern.search(modifieds[i].get_text()).group(1), "%m/%d/%Y")
                    record = {"url": url, "modified": date}
                    self.listings.append(record)
                else:
                    duplicatesCnt += 1
                i += 1
        self.log("List loaded, new: " + str(listingsCnt) + ", duplicates: " + str(duplicatesCnt))
        random.shuffle(self.listings)
        #check if there's next page available
        pager = soup.find(id='ctl00_ContentPlaceHolder1_ctl19_Paging1_tblPaging')
        if pager != None and pager.a.string == 'Click Here':
            self.nextList = self.baseUrl + pager.a['href']
        else:
            self.nextList = None

    def parseListing(self, soup):
        url = self.url().toString()
        self.log('Parsing Listing: ' + url)
        price, manufacturer, model, year, country, company, counter, serial, category, currency, condition = \
            (None, None, None, None, None, None, None, None, None, None, None)
        isY, isMan, isMod, isSerial, isLoc, isHrs, isCond = (False, False, False, False, False, False, False)

        currBidText = soup.find(id='ctl00_ContentPlaceHolder1_AuctionInformationBox1_lblCurrentBidText')
        if currBidText is not None and currBidText.get_text().strip() != 'Final Bid:':
            self.log('Not a final bid: ' + currBidText.get_text() + ' | ' + url)
            return
        else:
            priceElements = soup.find_all("span", {"class": "OALDetailCurrentBid"})
            for p in priceElements:
                price = p.get_text().strip()
            currencyElement = soup.find(id='ctl00_ContentPlaceHolder1_AuctionInformationBox1_lblCurrencyCode')
            if currencyElement is not None:
                currency = re.sub(r'[^A-Z]', '', currencyElement.get_text())

        specs = soup.find(id='tblSpecs')
        if specs is not None:
            tds = specs.find_all('td')
            for td in tds:
                t = td.get_text().strip()
                if isY: year = td.get_text().strip(); isY = False; continue
                if t == 'Year': isY = True; continue
                if isMan: manufacturer = td.get_text().strip(); isMan = False; continue
                if t == 'Manufacturer': isMan = True; continue
                if isMod: model = td.get_text().strip(); isMod = False; continue
                if t == 'Model': isMod = True; continue
                if isLoc: country = td.get_text().strip(); isLoc = False; continue
                if t == 'Location': isLoc = True; continue
                if isSerial: serial = td.get_text().strip(); isSerial = False; continue
                if t == 'Serial Number': isSerial = True; continue
                if isHrs: counter = td.get_text().strip(); isHrs = False; continue
                if t == 'Hours': isHrs = True; continue
                if isCond: condition = td.get_text().strip(); isCond = False; continue
                if t == 'Condition': isCond = True; continue

        if manufacturer is not None and model is not None:
            l = len(manufacturer) + len(model) + 1 # +1 is the space char
            l += len(year) + 1 if year is not None else 0
            category = soup.title.string.strip()[(l + 1):].replace(' For Auction At AuctionTime.com', '').strip()

        companyElement = soup.find(id='ctl00_ContentPlaceHolder1_SellerInformation1_hlContact')
        if companyElement is not None:
            company = companyElement.get_text().strip()
        else:
            companyElement = soup.find(id='ctl00_ContentPlaceHolder1_SellerInformation1_lblContact')
            if companyElement is not None:
                company = companyElement.get_text().strip()

        date = self.nextModified if self.nextModified != None else datetime.datetime.utcnow()
        doc = {"date":      date,
               "createdAt": datetime.datetime.utcnow(),
               "ttl":       datetime.datetime.utcnow() + datetime.timedelta(days=int(self.cfg.ttl)),
               "url":       url,
               "todo":      1,
               "portalId":  int(self.cfg.id)}
        if manufacturer != None:
            doc["manName"] = manufacturer
        if model != None:
            doc["modelName"] = model
        if year != None:
            doc["year"] = year
        if price != None:
            doc["price"] = price
        if currency != None:
            doc["currency"] = currency
        if country != None:
            doc["country"] = country
            doc["region"] = country
        if category != None:
            doc["category"] = category
            doc["catLang"] = "EN"
        if counter != None:
            doc["counter"] = counter
        if company != None:
            doc["company"] = company
        if serial != None:
            doc["serial"] = serial
        if condition == 'New':
            doc["new"] = 1

        if manufacturer == None or model == None:
            self.log("Mandatory fields missing: " + url)
        else:
            try:
                self.db.listings.insert(doc)
            except DuplicateKeyError as e:
                self.log(str(e))

    def run(self, url):
        self.log("Starting crawler")
        l = len(url)
        self.nextPage = url[self.round % l]
        self.loadMetaData()
        if self.cfg.getboolean('main', 'gui'):
            self.show()
        self.loadNextPage()

    def terminate(self, message=""):
        self.log('Terminating crawler: ' + message)
        self.app.quit()

    def log(self, message):
        if self.cfg.getboolean('log', 'log'):
            doc = {'date': datetime.datetime.utcnow(),
                   'ttl': datetime.datetime.utcnow() + datetime.timedelta(hours=int(self.cfg.get('log', 'ttl-hours'))),
                   'message': message}
            self.db['log.auctiontime'].insert(doc)
        if self.cfg.getboolean('log', 'debug'):
            print(message)

    def saveMetaData(self):
        doc = {'nextPage': self.nextPage,
               'nextModified': self.nextModified,
               'nextList': self.nextList,
               'sitemap': self.sitemap,
               'modelList': self.modelList,
               'listings': self.listings,
               'round': self.round}
        self.db['meta.auctiontime'].remove()
        self.db['meta.auctiontime'].insert(doc)
        self.log("Metadata saved")

    def loadMetaData(self):
        self.log("Loading metadata")
        doc = self.db['meta.auctiontime'].find_one()
        if doc != None:
            self.nextPage = doc['nextPage'] if doc['nextPage'] is not None else self.nextPage
            self.nextModified = doc['nextModified']
            self.nextList = doc['nextList']
            self.sitemap = doc['sitemap']
            self.listings = doc['listings']
            self.modelList = doc['modelList']
            self.log("Metadata loaded successfully")
        else:
            self.log("No metadata to load")
        if self.nextPage != None and self.isDuplicateListing(self.nextPage):
            self.log("Metadata NextPage is a duplicate listing, removing from URL queue")
            self.nextPage = None

    def isDuplicateListing(self, url):
        doc = self.db.listings.find_one({"url": url})
        if doc == None:
            return False
        else:
            return True

def format_exception(e):
    exception_list = traceback.format_stack()
    exception_list = exception_list[:-2]
    exception_list.extend(traceback.format_tb(sys.exc_info()[2]))
    exception_list.extend(traceback.format_exception_only(sys.exc_info()[0], sys.exc_info()[1]))

    exception_str = "Traceback (most recent call last):\n"
    exception_str += "".join(exception_list)
    # Removing the last \n
    exception_str = exception_str[:-1]

    return exception_str

if __name__ == '__main__':
    cfg = CrawlerConfig()
    cfg.read('auctiontime.ini')
    cfg.parseCrawlerConfig()
    force = False
    for i in range(0, len(sys.argv)):
        if sys.argv[i] == "--force":
            cfg.force = True
    #if the crawler is off, just exit unless need to force
    if not cfg.on and not cfg.force:
        sys.exit(1)
    app = QApplication(sys.argv)
    if cfg.getboolean('main', 'proxy'):
        proxy = QUrl(cfg.get('main', 'proxy-url'))
        QNetworkProxy.setApplicationProxy(QNetworkProxy(QNetworkProxy.HttpProxy, proxy.host(), proxy.port(), proxy.userName(), proxy.password()))
        print("Using application proxy:", proxy.toString())
    crawler = Crawler(app, cfg)
    crawler.run(['http://www.auctiontime.com/drilldown/manulist.aspx?LP=MAT&ETID=5&OALResults=1'])
    sys.exit(app.exec_())