#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../../')
import re
import codecs
import psycopg2
from psycopg2.extras import Json
import json
import db_settings
import ly_common
import pandas as pd


def PoliticalContributions(data_set):
    try:
        c.execute('''
            UPDATE legislator_politicalcontributions
            SET in_individual = %(in_individual)s, in_profit = %(in_profit)s, in_party = %(in_party)s, in_civil = %(in_civil)s, in_anonymous = %(in_anonymous)s, in_others = %(in_others)s, in_total = %(in_total)s, out_personnel = %(out_personnel)s, out_propagate = %(out_propagate)s, out_campaign_vehicle = %(out_campaign_vehicle)s, out_campaign_office = %(out_campaign_office)s, out_rally = %(out_rally)s, out_travel = %(out_travel)s, out_miscellaneous = %(out_miscellaneous)s, out_return = %(out_return)s, out_exchequer = %(out_exchequer)s, out_public_relation = %(out_public_relation)s, out_total = %(out_total)s, balance = %(balance)s
            WHERE legislator_id = %(legislator_id)s
        ''', data_set)
        c.execute('''
            INSERT into legislator_politicalcontributions(legislator_id, in_individual, in_profit, in_party, in_civil, in_anonymous, in_others, in_total, out_personnel, out_propagate, out_campaign_vehicle, out_campaign_office, out_rally, out_travel, out_miscellaneous, out_return, out_exchequer, out_public_relation, out_total, balance)
            SELECT %(legislator_id)s, %(in_individual)s, %(in_profit)s, %(in_party)s, %(in_civil)s, %(in_anonymous)s, %(in_others)s, %(in_total)s, %(out_personnel)s, %(out_propagate)s, %(out_campaign_vehicle)s, %(out_campaign_office)s, %(out_rally)s, %(out_travel)s, %(out_miscellaneous)s, %(out_return)s, %(out_exchequer)s, %(out_public_relation)s, %(out_total)s, %(balance)s
            WHERE NOT EXISTS (SELECT 1 FROM legislator_politicalcontributions WHERE legislator_id = %(legislator_id)s)
        ''', data_set)
    except Exception, e:
        print data_set
        raise

conn = db_settings.con()
c = conn.cursor()
ad = 8
df = pd.read_csv(u'第八屆立法委員選舉經費.csv', index_col='name')
print df.columns
df.index = map(lambda x: str(x).rstrip().lstrip(), df.index)
dump_data = json.loads(df.to_json(orient='index'))
for legislator in dump_data:
    legislator_id = ly_common.GetLegislatorId(c, legislator)
    legislatordetail_id = ly_common.GetLegislatorDetailId(c, legislator_id, ad)
    dump_data[legislator].update({"legislator_id": legislatordetail_id})
    PoliticalContributions(dump_data[legislator])
conn.commit()
print 'Succeed'
