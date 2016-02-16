#! /usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import re
import codecs
import psycopg2
import glob
import json

from common import ly_common
from common import db_settings


def Bill(bill):
    c.execute('''
        update bill_bill
        set ad = %(ad)s, data = %(data)s, for_search = %(for_search)s
        WHERE uid = %(uid)s
    ''', bill)
    c.execute('''
        INSERT into bill_bill(uid, ad, data, for_search)
        SELECT %(uid)s, %(ad)s, %(data)s, %(for_search)s
        WHERE NOT EXISTS (SELECT 1 FROM bill_bill WHERE uid = %(uid)s)
    ''', bill)

def LegislatorBill(legislator_id, bill_id, role):
    c.execute('''
        INSERT into bill_legislator_bill(legislator_id, bill_id, role)
        SELECT %s, %s, %s
        WHERE NOT EXISTS (SELECT 1 FROM bill_legislator_bill WHERE legislator_id = %s AND bill_id = %s)
    ''', (legislator_id, bill_id, role, legislator_id, bill_id))

conn = db_settings.con()
c = conn.cursor()

for f in glob.glob('bill/crawler/bills_1.json'):
    dict_list = json.load(open(f))
    print len(dict_list)
    print 'uids num: %d' % len(set([bill[u'系統號'] for bill in dict_list]))
    for i, bill in enumerate(dict_list):
        #bill
        bill['uid'] = bill[u'系統號']
        bill['ad'] = int(re.search(u'(\d+)屆', bill[u'會期']).group(1))
        bill['for_search'] = ' '.join([bill[key].replace(';', '') for key in [u'分類', u'主題', u'提案名稱'] if bill.get(key)])
        bill['links'][u'關係文書'] = 'http://lis.ly.gov.tw/lgcgi/lgmeetimage?' + bill['links'][u'關係文書'].split('^')[-1]
        for key in [u'主提案', u'連署提案']:
            if type(bill.get(key, [])) != type([]):
                bill[key] = [bill[key]]
        bill['data'] = json.dumps(bill)
        print bill[u'uid'], bill['ad']
        Bill(bill)
        # legilator_bill
        for legislator in bill[u'主提案']:
            uid = ly_common.GetLegislatorId(c, legislator)
            if uid:
                legislator_id = ly_common.GetLegislatorDetailId(c, uid, bill['ad'])
                if legislator_id:
                    LegislatorBill(legislator_id, bill['uid'], 'sponsor')
        for legislator in bill.get(u'連署提案', []):
            uid = ly_common.GetLegislatorId(c, legislator)
            if uid:
                legislator_id = ly_common.GetLegislatorDetailId(c, uid, bill['ad'])
                if legislator_id:
                    LegislatorBill(legislator_id, bill['uid'], 'cosponsor')
conn.commit()
print 'bills done'

c.execute('''
    SELECT
        legislator_id,
        COUNT(*) total,
        SUM(CASE WHEN role = 'sponsor' THEN 1 ELSE 0 END) sponsor,
        SUM(CASE WHEN role = 'cosponsor' THEN 1 ELSE 0 END) cosponsor
    FROM bill_legislator_bill
    GROUP BY legislator_id
''')
response = c.fetchall()
for r in response:
    param = dict(zip(['total', 'sponsor', 'cosponsor'], r[1:]))
    c.execute('''
        UPDATE legislator_legislatordetail
        SET bill_param = %s
        WHERE id = %s
    ''', (param, r[0]))
conn.commit()
print 'Update Bill param of People'
print 'Succeed'
