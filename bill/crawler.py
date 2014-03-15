#!/usr/bin/python
# -*- coding: utf-8 -*
import requests
import json
import codecs


def write_file(data, file_name):
    file = codecs.open(file_name, 'w', encoding='utf-8')
    file.write(data)
    file.close()

response = []
try:
    for sk in range(1, requests.get('http://api.ly.g0v.tw/v0/collections/bills/').json()['paging']['count'], 100):
        response.append(requests.get('http://api.ly.g0v.tw/v0/collections/bills/', params={'l': '100', 'sk': str(sk)}).json()['entries'])
    objs = json.loads({'entries': response})
except Exception, e:
    print sk
    raise
dump_data = json.dumps(objs)
write_file(dump_data, 'lyapi_bills.json')
dump_data = json.dumps(objs, sort_keys=True, indent=4, ensure_ascii=False)
write_file(dump_data, 'lyapi_bills(pretty_format).json')

response = []
try:
    for sk in range(1, requests.get('http://api.ly.g0v.tw/v0/collections/ttsmotions/').json()['paging']['count'], 100):
        response.append(requests.get('http://api.ly.g0v.tw/v0/collections/ttsmotions/', params={'l': '100', 'sk': str(sk)}).json()['entries'])
    objs = json.loads({'entries': response})
except Exception, e:
    print sk
    raise
dump_data = json.dumps(objs)
write_file(dump_data, 'lyapi_ttsmotions.json')
dump_data = json.dumps(objs, sort_keys=True, indent=4, ensure_ascii=False)
write_file(dump_data, 'lyapi_ttsmotions(pretty_format).json')
