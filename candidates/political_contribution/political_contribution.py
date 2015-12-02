#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../../')
import re
import json
import glob
import codecs
import psycopg2

import db_settings
import ly_common


def candidate_term_id(candidate):
    # same name, county, ad
    c.execute('''
        SELECT id
        FROM candidates_terms
        WHERE name = %(name)s AND ad = %(ad)s AND county = %(county)s
    ''', candidate)
    r = c.fetchone()
    if r:
        return r[0]
    # English within name
    m = re.match(u'(?P<cht>.+?)[a-zA-Z]', candidate['name'])
    candidate['name_like'] = '%s%%' % m.group('cht') if m else '%s%%' % candidate['name']
    c.execute('''
        SELECT id
        FROM candidates_terms
        WHERE name like %(name_like)s AND ad = %(ad)s AND county = %(county)s
    ''', candidate)
    r = c.fetchone()
    if r:
        return r[0]

def PoliticalContributions(data):
    try:
        c.execute('''
            UPDATE candidates_terms
            SET politicalcontributions = %(politicalcontributions)s
            WHERE id = %(id)s
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
        candidate['id'] = candidate_term_id(candidate)
        PoliticalContributions(candidate)
conn.commit()
