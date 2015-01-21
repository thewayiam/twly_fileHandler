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


def PoliticalContributions(data):
    try:
        c.execute('''
            UPDATE candidates_candidates
            SET politicalcontributions = %(politicalcontributions)s
            WHERE name = %(name)s AND ad = %(ad)s AND county = %(county)s
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
        PoliticalContributions(candidate)
conn.commit()
