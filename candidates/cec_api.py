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
    try:
        candidate['candidate_id'] = c.fetchone()[0]
        c.execute('''
            update candidates_candidates
            set birth = %(birthdate)s
            where uid = %(candidate_id)s
        ''', candidate)
    except:
        for k, v in candidate.items():
            print k, v

conn = db_settings.con()
c = conn.cursor()

r = requests.get('http://2016.cec.gov.tw/opendata/cec2016/getJson?dataType=3')
for candidate in r.json()[u"區域立委公報"]:
    candidate['cec_data'] = json.dumps(candidate)
    candidate['ad'] = re.search(u'第(?P<ad>\d+)屆', candidate['electiondefinename']).group(1)
    match = re.search(u'第(?P<constituency>\d+)選舉?區', candidate['sessionname'])
    candidate['constituency'] = match.group('constituency') if match else 1
    candidate['candidatename'] = re.sub(u'黄玉芬', u'黃玉芬', candidate['candidatename'])
    if candidate.get('drawno'):
        candidate['gender'] = u'男'if candidate['gender'] == 'M' else u'女'
        updateCandidates(candidate)
conn.commit()
