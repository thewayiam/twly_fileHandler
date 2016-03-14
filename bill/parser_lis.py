#! /usr/bin/env python
# -*- coding: utf-8 -*-
import requests
import re
import codecs
import psycopg2
import glob
import json
import ast
from sys import argv

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

def ROC2AD(roc_date):
    return '%d-%s-%s' % (int(roc_date[:-4])+1911, roc_date[-4:-2], roc_date[-2:])

conn = db_settings.con()
c = conn.cursor()
ad = ast.literal_eval(argv[1])['ad']

for f in glob.glob('bill/crawler/bills_%d.json' % ad):
    dict_list = json.load(open(f))
    print len(dict_list)
    print 'uids num: %d' % len(set([bill[u'系統號'] for bill in dict_list]))
    for i, bill in enumerate(dict_list):
        try:
            #bill
            bill['uid'] = bill[u'系統號']
            bill['ad'] = int(re.search(u'(\d+)屆', bill[u'會期']).group(1))
            bill['date'] = ROC2AD(bill[u'提案日期'])
            for motion in bill['motions']:
                motion['date'] = ROC2AD(motion[u'日期'])
            for_search = bill.get(u'主題', []) + bill.get(u'分類', [])
            bill['for_search'] = ' '.join(for_search) + bill.get(u'提案名稱', '')
    #       bill['links'][u'關係文書'] = 'http://lis.ly.gov.tw/lgcgi/lgmeetimage?' + bill['links'][u'關係文書'].split('^')[-1]
            bill['data'] = json.dumps(bill)
            Bill(bill)
            # legilator_bill
            for category, role in [(u'主提案', 'sponsor'), (u'連署提案', 'cosponsor')]:
                for legislator in bill.get(category, []):
                    legislator = ly_common.normalize_person_name(legislator)
                    uid = ly_common.GetLegislatorId(c, legislator)
                    if uid:
                        legislator_id = ly_common.GetLegislatorDetailId(c, uid, bill['ad'])
                        if legislator_id:
                            LegislatorBill(legislator_id, bill['uid'], role)
                    elif not re.search(u'(黨團|聯盟)', legislator):
                        raise
        except:
            print 'bill id: %s, ad: %d, name: %s not legislator?' % (bill['uid'], bill['ad'], legislator)
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


c.execute('''
    select uid, ad
    from bill_bill
    where ad = %s
''', [ad])
for bill_id, ad in c.fetchall():
    c.execute('''
        select jsonb_object_agg("role", "detail")
        from (
            select role, jsonb_build_object('party_list', json_agg(party_list), 'sum', sum(count)) as detail
            from (
                select role, jsonb_build_object('party', party, 'legislators', legislators, 'count', json_array_length(legislators)) as party_list, json_array_length(legislators) as count
                from (
                    select role, party, json_agg(detail) as legislators
                    from (
                        select role, party, jsonb_build_object('name', name, 'legislator_id', legislator_id) as detail
                        from (
                            select
				bl.role,
                                d.name as party,
                                l.name,
                                l.legislator_id
                            from legislator_legislatordetail l, jsonb_to_recordset(l.party) d(name text, start_at date, end_at date), bill_bill b, bill_legislator_bill bl
                            where b.uid = %%s and b.uid = bl.bill_id and bl.legislator_id = l.id %s
                        ) _
                    ) __
                    group by role, party
                    order by role, party
                ) ___
                order by count desc
            ) ____
            group by role
        ) row
    ''' % ("and d.start_at < to_date(b.data->>'date', 'YYYY-MM-DD') and d.end_at > to_date(b.data->>'date', 'YYYY-MM-DD')" if ad > 1 else ''), [bill_id])
    group_list = c.fetchone()
    c.execute('''
        update bill_bill
        set data = jsonb_set(data, '{group_list}', %s)
        where uid = %s
    ''', [group_list, bill_id])
conn.commit()
print 'Update bill group list'
print 'Succeed'
