import logging
import scrapy
from scrapy.crawler import CrawlerProcess

from datetime import datetime
from elasticsearch import Elasticsearch

es = Elasticsearch(['http://elastic:supersecret@localhost:9200'])
es_index='cl-post'

class CraigSpider(scrapy.Spider):
    name = 'craigspider'
    base_url='https://albuquerque.craigslist.org/search/sss?s='
    start_urls=list()
    for i in range(0, 1):
        start_urls.append(base_url+str(i*120))

    def parse(self, response):
        for post in response.css('p.result-info'):
            doc = dict()
            try:

                post_time = str(post.css('time').xpath('@datetime').extract_first()),
                timestamp=datetime.datetime.strptime(post_time, '%Y-%m-%d %H:%M').isoformat()
                doc = {
                        'title': str(post.css('a').xpath('text()').extract_first()),
                        'url': str(post.css('a').xpath('@href').extract_first()),
                        'price:': str(post.css('.result-price').xpath('text()').extract_first())
                }
            except Exception as e:
                logging.log(logging.WARN, 'failed to get doc: '+repr(e))
                continue
            logging.log(logging.DEBUG, 'ingesting doc: '+str(doc))
            res = es.create(index=es_index, timestamp=timestamp, body=doc)
            logging.log(logging.DEBUG, str(res))

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })

process.crawl(CraigSpider)
process.start()
res = es.search(index=es_index, body={'query': {'match_all': {}}})
print('Got %d Hits:' % res['hits']['total'])
for hit in res['hits']['hits']:
    print('hit: '+str(hit['_source']))
