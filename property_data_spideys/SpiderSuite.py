from twisted.internet import reactor, defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging

from spiders.propData import pierceCountyScraper, duvalCountyScraper

configure_logging()
runner = CrawlerRunner()

@defer.inlineCallbacks
def crawl():
    yield runner.crawl(pierceCountyScraper)
    yield runner.crawl(duvalCountyScraper)
    reactor.stop()

crawl()
reactor.run() # the script will block here until the last crawl call is finished