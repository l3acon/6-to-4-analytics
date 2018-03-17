from datetime import datetime
from elasticsearch import Elasticsearch

es = Elasticsearch(['http://elastic:supersecret@localhost:9200'])
timestamp=datetime.strptime('2018-03-17 11:42', '%Y-%m-%d %H:%M').isoformat()
es_index="test-new-index"+str(timestamp)

#doc = {
#        'price': '$120',
#        'url': 'something.carigslist.org',
#        'text': 'Jackson JS3 Concert Bass',
#        'timestamp': '2018-03-17 11:42'
#    }
#
#res = es.index(index="test-index", doc_type='post', body=doc)
#print(res)
#
#es.indices.refresh(index="test-index")
#
#res = es.search(index="test-index", body={"query": {"match_all": {}}})
#print("Got %d Hits:" % res['hits']['total'])
#for hit in res['hits']['hits']:
#        print(hit["_source"])
#
if not es.indices.exists(index=es_index):
    print('creating index: '+str(es_index))
    es.indices.create(index=es_index, ignore=400)
    es.index(
            index=es_index, 
            doc_type="test-type", 
            body = {
                    'price': '$120',
                    'url': 'something.carigslist.org',
                    'text': 'Jackson JS3 Concert Bass',
                    'timestamp': '2018-03-17 11:42'
                }
            )
else:
  print('index already created: '+str(es_index))

print('refreshig indeices')
es.indices.refresh(index=es_index)
res = es.search(index=es_index, body={'query': {'match_all': {}}})

print('Got %d Hits:' % res['hits']['total'])
for hit in res['hits']['hits']:
    print('hit: '+str(hit['_source']))
