#! /usr/bin/env python
# -*- coding: utf-8 -*-
import os
import re
import json
import codecs
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def rename_dict_key(d):
    return {
        "county_code": d[0].strip(),
        "county_name": d[1].strip(),
        "town_code": d[2].strip(),
        "town_name": d[3].strip(),
        "village_code": d[6].strip(),
        "village_name": d[5].strip(),
        "new_village": d[7].strip()
    }

# get_or_create village code maps
file_path = 'data/candidates/village_code_2018.json'
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
        rows = wks.get_all_values()[4:]
        for row in rows:
            row = rename_dict_key(row)
            if not row['new_village']:
                maps['%s_%s_%s' % (re.sub(u'台', u'臺', row['county_name']), row['town_name'], row['village_name'], )] = {
                    'county_code': row['county_code'],
                    'town_code': row['town_code'],
                    'village_code': row['village_code']
                }
            else:
                maps['_'.join(re.search(u'(.+?[縣市])(.+?[鄉鎮市區])(.+)', row['new_village']).groups())] = {
                    'town_code': row['village_code'][:7],
                    'village_code': row['village_code']
                }
    with codecs.open('data/candidates/village_code_2018.json', 'w', encoding='utf-8') as outfile:
        outfile.write(json.dumps(maps, indent=2, ensure_ascii=False))


wiki_ref = json.load(open('data/candidates/national_constituencies_from_wikidata.json'))

num_ref = [u'一', u'二', u'三', u'四', u'五', u'六', u'七', u'八', u'九', u'十', u'十一', u'十二', u'十三', u'十四', u'十五', u'十六', u'十七', u'十八', u'十九']
target = json.load(open('data/candidates/election_region_2016.json'))
for county, v in target.items():
    for region in v['regions']:
        for r in wiki_ref:
            if r['itemLabel'] == u'%s第%s選舉區' % (county, num_ref[region['constituency']-1]):
                region['wikidata_item'] = r['item']
                break
        for district, villages in region['district'].items():
            villages_code = []
            for village in villages:
                key = u'%s_%s_%s' % (county, district, village, )
                if not maps.get(key):
                    key = u'%s_%s_%s' % (county, district, re.sub(u'台', u'臺', village), )
                print key
                villages_code.append(maps[key]['village_code'])
            region['district'][district] = villages_code
with codecs.open('data/candidates/election_region_with_village_code_and_wikidata_id_2016.json', 'w', encoding='utf-8') as outfile:
    outfile.write(json.dumps(target, indent=2, ensure_ascii=False))
