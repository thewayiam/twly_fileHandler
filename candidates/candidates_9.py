#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../')
import os
import re
import uuid
import json
import glob
from datetime import datetime

import pandas as pd

import ly_common
import db_settings


def latest_term(candidate):
    # same name, same county, latest ad
    c.execute('''
        SELECT id
        FROM legislator_legislatordetail
        WHERE name = %(name)s and ad < %(ad)s
        ORDER BY ad DESC, (
            CASE
                WHEN county = %(previous_county)s THEN 1
            END
        )
        limit 1
    ''', candidate)
    r = c.fetchone()
    if r:
        return r
    # English in name
    # contains name, same county, latest ad
    m = re.match(u'(?P<cht>.+?)[a-zA-Z]', candidate['name'])
    candidate['name_like'] = '%s%%' % m.group('cht') if m else candidate['name']
    c.execute('''
        SELECT id
        FROM legislator_legislatordetail
        WHERE name like %(name_like)s and ad < %(ad)s
        ORDER BY ad DESC, (
            CASE
                WHEN county = %(previous_county)s THEN 1
            END
        )
        limit 1
    ''', candidate)
    r = c.fetchone()
    if r:
        return r

def get_or_create_uid(person):
    c.execute('''
        SELECT candidate_id
        FROM candidates_terms
        WHERE name = %(name)s and ad = %(ad)s and county = %(county)s and constituency = %(constituency)s
        LIMIT 1
    ''', person)
    r = c.fetchone()
    if r:
        return r[0]
    c.execute('''
        SELECT candidate_id
        FROM candidates_terms
        WHERE name = %(name)s and ad != %(ad)s
        ORDER BY (
            CASE
                WHEN county = %(county)s and constituency = %(constituency)s THEN 1
                WHEN county = %(county)s THEN 2
            END
        )
        LIMIT 1
    ''', person)
    r = c.fetchone()
    return r[0] if r else uuid.uuid4().hex

def insertCandidates(candidate):
    candidate['ad'] = ad
    candidate['previous_county'] = candidate['county']
    for county_change in county_versions.get(str(ad), []):
        if candidate['county'] == county_change['to']:
            candidate['previous_county'] = county_change['from']
    candidate['term_id'] = latest_term(candidate)
    candidate['uid'] = get_or_create_uid(candidate)
    candidate['id'] = '%s-%s' % (candidate['uid'], candidate['ad'])
    c.execute('''
        SELECT district
        FROM legislator_legislatordetail
        WHERE county = %(previous_county)s AND constituency = %(constituency)s
        ORDER BY ad DESC
    ''', candidate)
    r = c.fetchone()
    if r:
        candidate['district'] = r[0]
    complement = {"number": None, "birth": None, "gender": '', "party": '', "priority": None, "contact_details": None, "district": '', "elected": None, "votes": None, "education": None, "experience": None, "remark": None, "image": '', "links": None, "platform": ''}
    complement.update(candidate)
    c.execute('''
        INSERT INTO candidates_candidates(uid, name, birth)
        SELECT %(uid)s, %(name)s, %(birth)s
        WHERE NOT EXISTS (SELECT 1 FROM candidates_candidates WHERE uid = %(uid)s)
    ''', complement)
    c.execute('''
        INSERT INTO candidates_terms(id, candidate_id, latest_term_id, ad, number, priority, name, gender, party, constituency, county, district, elected, contact_details, votes, education, experience, remark, image, links, platform)
        SELECT %(id)s, %(uid)s, %(term_id)s, %(ad)s, %(number)s, %(priority)s, %(name)s, %(gender)s, %(party)s, %(constituency)s, %(county)s, %(district)s, %(elected)s, %(contact_details)s, %(votes)s, %(education)s, %(experience)s, %(remark)s, %(image)s, %(links)s, %(platform)s
        WHERE NOT EXISTS (SELECT 1 FROM candidates_terms WHERE id = %(id)s)
    ''', complement)

conn = db_settings.con()
c = conn.cursor()

ad = 9
county_versions = json.load(open('county_versions.json'))
files = [f for f in glob.glob('%s/daily/*.xlsx' % ad)]
for f in files:
    if re.search('全國不分區', f):
        df = pd.read_excel(f, names=['date', 'party', 'priority', 'name', 'area', 'cec', 'remark'], usecols=[0, 1, 2, 3, 5, 6, 7])
    elif re.search('區域', f):
        df = pd.read_excel(f, names=['date', 'area', 'name_1', 'name_2', 'party', 'cec', 'remark'], usecols=[0, 1, 2, 3, 4, 6, 7])
        df['name'] = pd.concat([df['name_1'].dropna(), df['name_2'].dropna()])
    else:
        df = pd.read_excel(f, names=['date', 'area', 'name', 'party', 'cec', 'remark'], usecols=[0, 1, 2, 3, 4, 5])
    df = df[df['remark'].isnull() & df['name'].notnull()]
    candidates = json.loads(df.to_json(orient='records'))
    for candidate in candidates:
        candidate = ly_common.normalize_person(candidate)
        for target, replacement in [(u'選舉?區', u''), (u'全國$', u'全國不分區')]:
            candidate['area'] = re.sub(target, replacement, candidate['area'])
        match = re.search(u'第(?P<constituency>\d+)', candidate['area'])
        if match:
            candidate['constituency'] = match.group('constituency')
            candidate['county'] = re.sub(u'第\d+', '', candidate['area'])
        else:
            candidate['constituency'] = 1
            candidate['county'] = candidate['area']
        insertCandidates(candidate)
conn.commit()
