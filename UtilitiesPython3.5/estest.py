from datetime import datetime
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta

## create an index with the name of current date - delta
      
delta=4

# index_to_create='test-index'+'-'+datetime.strftime(datetime.now(), '%Y-%m-%d')
todelete='test-index'+'-'+datetime.strftime(datetime.now() - timedelta(delta), '%Y-%m-%d')

today=todelete

print(today)

print(todelete)

es = Elasticsearch(['https://search-ssm-esdomain-lbyrp7kpnj336o5igvlbow7kiu.eu-central-1.es.amazonaws.com'])

doc = {
    'author': 'kimchy 2',
    'text': 'Elasticsearch: cool. bonsai cool 2.',
    'timestamp': datetime.now(),
}
res = es.index(index=today, doc_type='tweet', id=1, body=doc)
print("res")
print(res)
print("res['resull]")
print(res['result'])

res = es.get(index=today, doc_type='tweet', id=1)
print("res['_source]")
print(res['_source'])

es.indices.refresh(index=today)

res = es.search(index=today, body={"query": {"match_all": {}}})
print(res)
print("Got %d Hits:" % res['hits']['total'])




# print(res['hits']['total'])
# for hit in res['hits']['hits']:
#     print("%(timestamp)s %(author)s: %(text)s" % hit["_source"])