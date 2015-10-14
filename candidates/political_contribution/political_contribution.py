#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../../')
import re
import json
import glob
import codecs
import psycopg2
from psycopg2.extras import Json
import pandas as pd

import db_settings
import ly_common


def candidate_uid(candidate):
    # same name, county, ad
    c.execute('''
        SELECT uid
        FROM candidates_candidates
        WHERE name = %(name)s and county = %(county)s and ad = %(ad)s
        ORDER BY ad DESC
    ''', candidate)
    r = c.fetchone()
    if r:
        return r
    # same name, ad, differrnt county
    c.execute('''
        SELECT uid
        FROM candidates_candidates
        WHERE name = %(name)s and ad = %(ad)s
        ORDER BY ad DESC
    ''', candidate)
    r = c.fetchone()
    if r:
        return r
    # English in name
    # contains name, same county, ad
    candidate['name_like'] = '%s%%' % re.sub('[a-zA-Z]', '', candidate['name'])
    c.execute('''
        SELECT uid
        FROM candidates_candidates
        WHERE name like %(name_like)s and county = %(county)s and ad = %(ad)s
        ORDER BY ad DESC
    ''', candidate)
    r = c.fetchone()
    if r:
        return r
    # contains name, different county, same ad
    c.execute('''
        SELECT uid
        FROM candidates_candidates
        WHERE name like %(name_like)s and ad = %(ad)s
        ORDER BY ad DESC
    ''', candidate)
    r = c.fetchone()
    if r:
        return r

def PoliticalContributions(data):
    try:
        c.execute('''
            UPDATE candidates_candidates
            SET politicalcontributions = %(politicalcontributions)s
            WHERE uid = %(uid)s
        ''', data)
    except Exception, e:
        print data
        raise

conn = db_settings.con()
c = conn.cursor()
for f in glob.glob('*.json'):
    dict_list = json.load(open(f))
    for candidate in dict_list:
        for wrong, right in [(u'楊煌', u'楊烱煌')]:
            candidate['name'] = re.sub(wrong, right, candidate['name'])

        income = {key: candidate[key] for key in ["in_individual", "in_profit", "in_party", "in_civil", "in_anonymous", "in_others"]}
        expenses = {key: candidate[key] for key in ["out_personnel", "out_propagate", "out_campaign_vehicle", "out_campaign_office", "out_rally", "out_travel", "out_miscellaneous", "out_return", "out_exchequer", "out_public_relation"]}
        pc = {key: candidate[key] for key in ["in_total", "out_total", "balance"]}
        pc.update({'in': income, 'out': expenses})
        candidate['politicalcontributions'] = json.dumps(pc)
        candidate['uid'] = candidate_uid(candidate)
        PoliticalContributions(candidate)
conn.commit()
