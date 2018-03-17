from datetime import datetime
from elasticsearch import Elasticsearch

es = Elasticsearch(['http://elastic:supersecret@localhost:9200'])

doc = {
        'price': '$120',
        'url': 'something.carigslist.org',
        'text': 'Jackson JS3 Concert Bass',
        'timestamp': '2018-03-17 11:42'
    }

res = es.index(index="test-index", doc_type='post', body=doc)
print(res)

es.indices.refresh(index="test-index")

res = es.search(index="test-index", body={"query": {"match_all": {}}})
print("Got %d Hits:" % res['hits']['total'])
for hit in res['hits']['hits']:
        print(hit["_source"])

