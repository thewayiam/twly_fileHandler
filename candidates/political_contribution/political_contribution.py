#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../../')
import re
import json
import glob
import codecs
import psycopg2

from common import db_settings
from common import ly_common


def candidate_term_id(candidate):
    # same name, county, ad
    c.execute('''
        SELECT candidate_id
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
        SELECT candidate_id
        FROM candidates_terms
        WHERE name like %(name_like)s AND ad = %(ad)s AND county = %(county)s
    ''', candidate)
    r = c.fetchone()
    if r:
        return r[0]

def PoliticalContributions(data):
    '''
    Store political contrbution history to candidate, e.g. all record before 2018(<=) will store into candidate which election_year less than 2018(<=)
    '''

    c.execute('''
        UPDATE candidates_terms
        SET politicalcontributions = COALESCE(politicalcontributions, '[]'::jsonb) || %(politicalcontributions)s::jsonb
        WHERE candidate_id = %(candidate_uid)s AND ad >= %(ad)s
    ''', data)
    c.execute('''
        UPDATE candidates_terms
        SET politicalcontributions = (SELECT jsonb_agg(x) FROM (
            SELECT x from (
                SELECT DISTINCT(value) as x
                FROM jsonb_array_elements(politicalcontributions)
                WHERE value->'title' is not null
            ) t ORDER BY x->'ad' DESC
        ) tt)
        WHERE candidate_id = %(candidate_uid)s AND ad >= %(ad)s
    ''', data)

ad_election_year = {'2016': '9', '2012': '8'}
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
        if not candidate.has_key('election_year'):
            for year, ad in ad_election_year.items():
                if ad == candidate['ad']:
                    candidate['election_year'] = year
                    break
        if not candidate.has_key('ad'):
            candidate['ad'] = ad_election_year[candidate['election_year']]
        pc = [{
            'ad': candidate['ad'],
            'election_year': candidate['election_year'],
            'title': candidate.get('title', 'legislators'),
            'election_name': candidate.get('election_name', u'%s立法委員選舉' % candidate['county']),
            'pc': pc
        }]
        candidate['politicalcontributions'] = json.dumps(pc)
        candidate['candidate_uid'] = candidate_term_id(candidate)
        PoliticalContributions(candidate)
conn.commit()
