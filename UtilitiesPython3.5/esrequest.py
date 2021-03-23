import requests
from datetime import datetime as dt
from datetime import timedelta


#to delete an index with the name of delta days before

delta=4

# index_to_create='test-index'+'-'+datetime.strftime(datetime.now(), '%Y-%m-%d')
# todelete='test-index'+'-'+dt.strftime(dt.now() - timedelta(delta), '%Y-%m-%d')

# url = 'https://search-ssm-esdomain-lbyrp7kpnj336o5igvlbow7kiu.eu-central-1.es.amazonaws.com/_cat/indices/'
# url = 'https://search-ssm-esdomain-lbyrp7kpnj336o5igvlbow7kiu.eu-central-1.es.amazonaws.com/_stats'
#
# response = requests.get(url)
#
# if(response.status_code==200):
#     print(response.content)
# else:
#     print("Error"+response.status_code)

host = 'https://search-ssm-esdomain-lbyrp7kpnj336o5igvlbow7kiu.eu-central-1.es.amazonaws.com'

url2 = 'https://search-ssm-esdomain-lbyrp7kpnj336o5igvlbow7kiu.eu-central-1.es.amazonaws.com/test-index-2020-04-29'


# url2 = host+'/'+todelete

print(url2)

# response = requests.delete(url2)

response = requests.put(url2)
if(response.status_code==200):
    print(response.content)
else:
    print(response.status_code)

