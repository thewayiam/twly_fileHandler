#! /usr/bin/env python
# -*- coding: utf-8 -*-
import os
import re
import json
import codecs
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def rename_dict_key(d):
    columns = {
        u"縣市代碼": "county_code",
        u"縣市名稱": "county_name",
        u"區里代碼": "town_code",
        u"區鄉鎮名稱": "town_name",
        u"村里代碼": "village_code",
        u"村里名稱": "village_name",
        u"村里代碼": "village_code"
    }
    for verbose_key, key in columns.items():
        d[key] = d.pop(verbose_key)
    return d

# get_or_create village code maps
file_path = 'data/candidates/village_code_2016.json'
if os.path.isfile(file_path):
    maps = json.load(open(file_path))
else:
    scope = ['https://spreadsheets.google.com/feeds']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('candidates/credential.json', scope)
    gc = gspread.authorize(credentials)
    sh = gc.open_by_key('1X0iuWD4Jrh1M-79X6FhtWOK1Tc4wY06N53E2wszWNBE')
    worksheets = sh.worksheets()
    for wks in worksheets:
        maps = {}
        rows = wks.get_all_records(head=4)
        for row in rows:
            row = rename_dict_key(row)
            maps['%s_%s_%s' % (re.sub(u'台', u'臺', row['county_name']), row['town_name'], row['village_name'], )] = {
                'county_code': row['county_code'],
                'town_code': row['town_code'],
                'village_code': row['village_code']
            }
    with codecs.open('data/candidates/village_code_2016.json', 'w', encoding='utf-8') as outfile:
        outfile.write(json.dumps(maps, indent=2, ensure_ascii=False))


target = json.load(open('data/candidates/election_region_2016.json'))
target_out = target.copy()
for county, v in target.items():
    for region in v['regions']:
        for district, villages in region['district'].items():
            villages_code = []
            for village in villages:
                key = u'%s_%s_%s' % (county, district, village, )
                if not maps.get(key):
                    key = u'%s_%s_%s' % (county, district, re.sub(u'台', u'臺', village), )
                print key
                villages_code.append(maps[key]['village_code'])
            region['district'][district] = villages_code
with codecs.open('data/candidates/election_region_with_village_code_2016.json', 'w', encoding='utf-8') as outfile:
    outfile.write(json.dumps(target, indent=2, ensure_ascii=False))

