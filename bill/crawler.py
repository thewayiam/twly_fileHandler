#!/usr/bin/python
# -*- coding: utf-8 -*
import requests
import json
import codecs


def write_file(data, file_name):
    file = codecs.open(file_name, 'w', encoding='utf-8')
    file.write(data)
    file.close()

bill_count = {'l': requests.get('http://api.ly.g0v.tw/v0/collections/bills/').json()['paging']['count']}
response = requests.get('http://api.ly.g0v.tw/v0/collections/bills/', params=bill_count)
objs = json.loads(response.text)
dump_data = json.dumps(objs, sort_keys=True, ensure_ascii=False)
write_file(dump_data, 'lyapi_bills.json')

bill_count = {'l': requests.get('http://api.ly.g0v.tw/v0/collections/ttsmotions/').json()['paging']['count']}
response = requests.get('http://api.ly.g0v.tw/v0/collections/ttsmotions/', params=bill_count)
objs = json.loads(response.text)
dump_data = json.dumps(objs, sort_keys=True, ensure_ascii=False)
write_file(dump_data, 'lyapi_ttsmotions.json')
