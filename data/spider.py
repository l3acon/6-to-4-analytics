import logging
import scrapy
import hashlib
from scrapy.crawler import CrawlerProcess

import datetime
from elasticsearch import Elasticsearch

es = Elasticsearch(['http://elastic:supersecret@muffuletta:9200'])
es_index='cl-post'

mappings='''
{
  "mappings": {
    "doc": {
        "properties": {
            "title": { "type": "text", "fielddata": "true" },
            "url": { "type": "text" },
            "price": { "type": "float" }
        }
    }
  }
}
'''

class CraigSpider(scrapy.Spider):
    name = 'craigspider'
    base_url='https://albuquerque.craigslist.org/search/sss?s='
    start_urls=list()
    for i in range(0, 25):
        start_urls.append(base_url+str(i*120))

    def parse(self, response):
        h=hashlib.new('sha1')
        for post in response.css('p.result-info'):
            doc = dict()
            try:
                post_time = datetime.datetime.strptime(str(post.css('time').xpath('@datetime').extract_first()), '%Y-%m-%d %H:%M')
                timestamp=post_time.isoformat()
                title=str(post.css('a').xpath('text()').extract_first())
                this_index=str(es_index)+'-'+str(post_time.month)+'-'+str(post_time.day)
                h.update(timestamp+title)
                doc = {
                        'title': title,
                        'url': str(post.css('a').xpath('@href').extract_first()),
                        'price': str(post.css('.result-price').xpath('text()').extract_first()).replace('$', ''),
                        '@timestmp': timestamp
                }
            except Exception as e:
                logging.log(logging.WARN, 'failed to get doc: '+repr(e))
                continue
            logging.log(logging.DEBUG, 'ingesting doc: '+str(doc))
            if not es.indices.exists(this_index):
                logging.log(logging.DEBUG, 'creating index: '+str(this_index))
                res = es.indices.create(index=this_index, body=mappings)
                logging.log(logging.DEBUG, str(res))
            res = es.index(index=this_index, id=h.hexdigest(), doc_type='doc', body=doc)
            logging.log(logging.DEBUG, str(res))

process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
    })

process.crawl(CraigSpider)
process.start()
res = es.search(index=es_index+'*', body={'query': {'match_all': {}}})
print('Got %d Hits:' % res['hits']['total'])
for hit in res['hits']['hits']:
    print('hit: '+str(hit['_source']))
