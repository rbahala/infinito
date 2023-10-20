import scrapy
from twisted.internet import defer
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from crochet import setup
from crochet import setup, wait_for
from site1.items import Site1Item
from site1.pipelines import Site1Pipeline

setup()

articles = []

class CloudflareSpider(scrapy.Spider):
    name = "CloudflareSpider"

    def start_requests(self):
        urls = [
            "https://blog.cloudflare.com/",
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        global articles
        item = Site1Item()  
      
        articles.extend(list(set(response.xpath("//article//h2/parent::a/@href").getall())))

        next_page = response.xpath("//nav[contains(@class, 'pagination')]//div[contains(@class, 'ml3')]//a[1]/@href").get()
        if next_page is not None:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(next_page, callback=self.parse)


class CloudflareDataSpider(scrapy.Spider):
    name = "CloudflareDataSpider"

    custom_settings = {
        'ITEM_PIPELINES': {
            'site1.pipelines.Site1Pipeline': 300,
        }
    }

    def start_requests(self):
        global articles

        for url in articles:
            self.current_url = "https://blog.cloudflare.com/" + url
            yield scrapy.Request(url=self.current_url, callback=self.parse,cb_kwargs={'url':self.current_url})
    
    def parse(self, response, url=None):
        title = response.xpath("//article/h1/text()").get()
        link = response.url
        date = response.xpath("//article//noscript/p/text()").get()        
        authors = "; ".join(response.xpath("//article/ul/li/div/a/text()").getall())
        content = " ".join(response.xpath("//section[contains(@class, 'post-full-content')][1]//*/text()").getall())
        
        yield {"title": title, "link": link, "date": date, "authors": authors, "content": content}


configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})
runner = CrawlerRunner()

@defer.inlineCallbacks
def crawl():
    global logs, volume_urls
    yield runner.crawl(CloudflareSpider)
    yield runner.crawl(CloudflareDataSpider)
    
@wait_for(timeout=3600)
def _crawl():
    return crawl()

_crawl()