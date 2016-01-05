#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../')
import re
import json
import requests
from datetime import datetime

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
        where ad = 9 and county = %s and constituency = %s and name = %s returning candidate_id
    ''', (candidate[0], candidate[1], candidate[2]))
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
conn.commit()
