#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../')
import re
import json
import codecs
import requests
from datetime import datetime
import collections

import ly_common
import db_settings


def updateCandidates(candidate):
    c.execute('''
        update candidates_terms
        set cec_data = %(cec_data)s, number = %(drawno)s, gender = %(gender)s
        where ad = %(ad)s and county = %(cityname)s and constituency = %(constituency)s and name = %(candidatename)s returning candidate_id
    ''', candidate)
    resp = c.fetchone()
    if not resp:
        # English in name
        # contains name, same county, latest ad
        m = re.match(u'(?P<cht>.+?)[a-zA-Z]', candidate['candidatename'])
        candidate['name_like'] = '%s%%' % m.group('cht') if m else '%s%%' % candidate['candidatename']
        c.execute('''
            update candidates_terms
            set cec_data = %(cec_data)s, number = %(drawno)s, gender = %(gender)s
            where ad = %(ad)s and county = %(cityname)s and constituency = %(constituency)s and name like %(name_like)s returning candidate_id
        ''', candidate)
        resp = c.fetchone()
    try:
        candidate['candidate_id'] = resp[0]
        c.execute('''
            update candidates_candidates
            set birth = %(birthdate)s
            where uid = %(candidate_id)s
        ''', candidate)
    except:
        print candidate['candidatename']

conn = db_settings.con()
c = conn.cursor()
ad = 9

for datatype, key in [('3', u'區域立委公報')]:
    r = requests.get('http://2016.cec.gov.tw/opendata/cec2016/getJson?dataType=%s' % datatype)
    for candidate in r.json()[key]:
        candidate['cec_data'] = json.dumps(candidate)
        candidate['ad'] = re.search(u'第(?P<ad>\d+)屆', candidate['electiondefinename']).group(1)
        match = re.search(u'第(?P<constituency>\d+)選舉?區', candidate['sessionname'])
        candidate['constituency'] = match.group('constituency') if match else 1
        candidate['candidatename'] = re.sub(u'黄玉芬', u'黃玉芬', candidate['candidatename'])
        if candidate.get('drawno'):
            candidate['gender'] = u'男' if candidate['gender'] == 'M' else u'女'
            updateCandidates(candidate)
# unqualify
for candidate in [(u'桃園市', 5, u'羅文欽'), (u'桃園市', 3, u'黃志浩')]:
    c.execute('''
        delete from candidates_terms
        where ad = %s and county = %s and constituency = %s and name = %s returning candidate_id
    ''', (ad, candidate[0], candidate[1], candidate[2]))
    res = c.fetchone()
    if res:
        candidate_id = res[0]
        c.execute('''
            delete from candidates_candidates
            where uid = %s
        ''', [candidate_id])
#

for datatype, key in [('4', u'山地原住民立委'), ('5', u'平地原住民立委')]:
    r = requests.get('http://2016.cec.gov.tw/opendata/cec2016/getJson?dataType=%s' % datatype)
    for candidate in r.json()[key]:
        candidate['cec_data'] = json.dumps(candidate)
        candidate['ad'] = re.search(u'第(?P<ad>\d+)屆', candidate['electiondefinename']).group(1)
        match = re.search(u'第(?P<constituency>\d+)選舉?區', candidate['sessionname'])
        candidate['cityname'] = re.sub(u'立委', '', key)
        candidate['constituency'] = 1
        candidate['candidatename'] = re.sub(u'[。˙・･•．.]', u'‧', candidate['candidatename'])
        candidate['candidatename'] = re.sub(u'[　\s()（）^]', '', candidate['candidatename'])
        if candidate.get('drawno'):
            candidate['gender'] = u'男' if candidate['gender'] == 'M' else u'女'
            updateCandidates(candidate)

for datatype, key in [('2', u'全國不分區及僑居國外國民立委公報')]:
    r = requests.get('http://2016.cec.gov.tw/opendata/cec2016/getJson?dataType=%s' % datatype)
    for candidate in r.json()[key]:
        candidate['cec_data'] = json.dumps(candidate)
        candidate['ad'] = re.search(u'第(?P<ad>\d+)屆', candidate['electiondefinename']).group(1)
        candidate['constituency'] = 1
        candidate['candidatename'] = re.sub(u'[。˙・･•．.]', u'‧', candidate['candidatename'])
        candidate['candidatename'] = re.sub(u'[　\s()（）^]', '', candidate['candidatename'])
        candidate['cityname'] = u'僑居國外國民' if re.search(u'(連元章|童惠珍)', candidate['candidatename']) else u'全國不分區'
        if candidate.get('drawno'):
            candidate['gender'] = u'男' if candidate['gender'] == 'M' else u'女'
            updateCandidates(candidate)

counties = {}
r = requests.get('http://2016.cec.gov.tw/opendata/cec2016/getJson?dataType=6')
for region in r.json()[u'選舉區應選人數/對應之行政區']:
    match = re.search(u'第(?P<constituency>\d+)選舉?區', region['sessionname'])
    region['constituency'] = int(match.group('constituency')) if match else 1
    dv = {}
    for x in region['sessiontownship']:
        if x.get('areaname'):
            if x['areaname'] not in dv.keys():
                dv.update({x['areaname']: []})
            dv[x['areaname']].append(x['villagename'])
    if region['cityname'] not in counties.keys():
        counties.update({
            region['cityname']: {
                'regions': [],
                'duplicated': []
            }
        })
    counties[region['cityname']]['regions'].append({
        'constituency': region['constituency'],
        'electedperson': region['electedperson'],
        'district': dv,
    })
    c.execute('''
        update candidates_terms
        set district = %s
        where ad = %s and county = %s and constituency = %s
    ''', (u'、'.join(dv.keys()), ad, region['cityname'], region['constituency']))

for county, v in counties.items():
    regions = v['regions']
    counties[county]['regions'] = sorted(counties[county]['regions'], key=lambda x: x['constituency'])
    districts, duplicate_detail = [], []
    for region in v['regions']:
        districts.extend(region['district'].keys())
    duplicated = [item for item, count in collections.Counter(districts).items() if count > 1]
    if duplicated:
        for region in v['regions']:
            for district, villages in region['district'].items():
                if district in duplicated:
                    duplicate_detail.append({
                        'constituency': region['constituency'],
                        'district': district,
                        'villages': villages
                    })
        duplicate_detail = sorted(duplicate_detail, key=lambda x: [x['district'], len(x['villages'])])
        dv = [{'district': d, 'detail': []} for d in {x['district'] for x in duplicate_detail}]
        for d in duplicate_detail:
            for u in dv:
                if d['district'] == u['district']:
                    u['detail'].append({
                        'constituency': d['constituency'],
                        'villages': d['villages']
                    })
                    break
        counties[county].update({'duplicated': dv})
with codecs.open('election_region_2016.json', 'w', encoding='utf-8') as outfile:
    outfile.write(json.dumps(counties, indent=2, ensure_ascii=False))
c.execute('''
    update elections_elections
    set data = %s
    where id = %s
''', (counties, str(ad)))
c.execute('''
    INSERT INTO elections_elections(id, data)
    SELECT %s, %s
    WHERE NOT EXISTS (SELECT 1 FROM elections_elections WHERE id = %s)
''', (str(ad), counties, str(ad)))
conn.commit()
