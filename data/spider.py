import logging
import scrapy
import hashlib
from scrapy.crawler import CrawlerProcess

from datetime import datetime
from elasticsearch import Elasticsearch

es = Elasticsearch(['http://elastic:supersecret@muffuletta:9200'])

mappings='''
{
  "mappings": {
    "doc": {
        "properties": {
            "title": {
                "type": "text",
                "fields": {
                    "raw": {
                        "type": "keyword"
                    }
                }
            },
            "message": {
                "type": "text",
                "fields": {
                    "raw": {
                        "type": "keyword"
                    }
                }
            },
            "location": { "type": "geo_point"},
            "url": { "type": "text" },
            "price": { "type": "float" },
            "access_time": { "type": "date" }
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
        for post in response.css('p.result-info'):
            url=str(post.css('a').xpath('@href').extract_first())
            access_time = datetime.strptime(
                    str(post.css('time').xpath('@datetime').extract_first()), 
                    '%Y-%m-%d %H:%M')
            accessed_timestamp=access_time.isoformat()
            title=str(post.css('a').xpath('text()').extract_first())
            logging.log(logging.DEBUG, "title: "+str(title))
            price=str(post.css('.result-price').xpath('text()').extract_first()).replace('$', '')
            logging.log(logging.DEBUG, "price: "+str(price))
            yield scrapy.Request(url, 
                    callback=self.parse_page, 
                    meta={'URL': url, 'TITLE': title, 'PRICE': price})
    
    def parse_page(self, response):
        url = response.meta.get('URL')
        title = response.meta.get('TITLE')
        price = response.meta.get('PRICE')
        h = hashlib.new('sha1')
        cl_timestamp = response.css('p').xpath('time').xpath('@datetime').extract_first()
        messages = response.xpath('//*[@id="postingbody"]').xpath('text()').extract()
        message = ' '.join(messages)
        lat = response.xpath('//*[@id="map"]').xpath('@data-latitude').extract_first()
        lon = response.xpath('//*[@id="map"]').xpath('@data-longitude').extract_first()
        logging.log(logging.DEBUG, "lat: "+str(lat)+" lon: "+str(lon))
        doc = {
                'url': url,
                'title': title,
                'price': price,
                'location': [float(lat), float(lon)],
                'message': message,
                '@timestamp': cl_timestamp
                }
        cl_timestamp=doc['@timestamp']
        h.update(str(cl_timestamp)+str(doc['title']))
        timestamp=datetime.strptime(cl_timestamp, '%Y-%m-%dT%H:%M:%S-%f')
        this_index=datetime.strftime(timestamp, 'cl-%Y.%m.%d')
        if not es.indices.exists(this_index):
            logging.log(logging.DEBUG, 'creating index: '+str(this_index))
            res = es.indices.create(index=this_index, body=mappings)
            logging.log(logging.DEBUG, str(res))
        res = es.index(index=this_index, id=h.hexdigest(), doc_type='doc', body=doc)


process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)',
    'HTTPCACHE_ENABLED': 'True',
    'HTTPCACHE_EXPIRATION_SECS': '0' # Set to 0 to never expire
    })

process.crawl(CraigSpider)
process.start()
#res = es.search(index=es_index+'*', body={'query': {'match_all': {}}})
#print('Got %d Hits:' % res['hits']['total'])
#for hit in res['hits']['hits']:
#    print('hit: '+str(hit['_source']))
