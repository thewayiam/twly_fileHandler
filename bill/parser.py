#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../')
import requests
import re
import codecs
import psycopg2
import json
import db_settings
import ly_common


def BillExist(bill_ref):
    c.execute('''
        SELECT uid
        FROM bill_bill
        WHERE uid = %s
    ''', (bill_ref,))
    return c.fetchone()

def Bill(bill):
    c.execute('''
        INSERT into bill_bill(uid, api_bill_id, ad, abstract, summary, bill_type, doc, proposed_by, sitting_introduced)
        SELECT %(bill_ref)s, %(bill_id)s, %(ad)s, %(abstract)s, %(summary)s, %(bill_type)s, %(doc)s, %(proposed_by)s, %(sitting_introduced)s
        WHERE NOT EXISTS (SELECT 1 FROM bill_bill WHERE uid = %(bill_ref)s)
    ''', bill)

def LegislatorBill(legislator_id, bill_id, priproposer, petition):
    c.execute('''
        INSERT into bill_legislator_bill(legislator_id, bill_id, priproposer, petition)
        SELECT %s, %s, %s, %s
        WHERE NOT EXISTS (SELECT 1 FROM bill_legislator_bill WHERE legislator_id = %s AND bill_id = %s)
    ''', (legislator_id, bill_id, priproposer, petition, legislator_id, bill_id))

def BillMotions(motion):
    c.execute('''
        INSERT into bill_billmotions(bill_id, sitting_id, agenda_item, committee, item, motion_class, resolution, status)           SELECT %(bill_id)s, %(sitting_id)s, %(agenda_item)s, %(committee)s, %(item)s, %(motion_class)s, %(resolution)s, %(status)s
        WHERE NOT EXISTS (SELECT 1 FROM bill_billmotions WHERE bill_id = %(bill_id)s AND sitting_id = %(sitting_id)s)
    ''', motion)

def ttsMotions(motion):
    c.execute('''
        INSERT into bill_ttsmotions(bill_id, sitting_id, agencies, category, chair, date, memo, motion_type, progress, resolution, summary, tags, topic, tts_key)
        SELECT %(bill_id)s, %(sitting_id)s, %(agencies)s, %(category)s, %(chair)s, %(date)s, %(memo)s, %(motion_type)s, %(progress)s, %(resolution)s, %(summary)s, %(tags)s, %(topic)s, %(tts_key)s
        WHERE NOT EXISTS (SELECT 1 FROM bill_ttsmotions WHERE bill_id = %(bill_id)s AND sitting_id = %(sitting_id)s)
    ''', motion)

conn = db_settings.con()
c = conn.cursor()

ad = 8
dict_list = json.load(open('lyapi_bills.json'))
for bill in dict_list['entries']:
    if not bill['bill_ref'] or not re.search(u'L', bill['bill_ref']):
        continue
    print bill['bill_ref']
    bill['ad'] = ad
    Bill(bill)
    for motion in bill['motions']:
        sitting_dict = {"uid": motion['sitting_id'], "ad": int(motion['sitting_id'].split('-')[0]), "session": int(motion['sitting_id'].split('-')[1][:2]), "date": motion['dates'][0]['date']}
        ly_common.InsertSitting(c, sitting_dict)
        motion_dict = motion.copy()
        motion_dict.update({"bill_id": bill['bill_ref']})
        BillMotions(motion_dict)
    priproposer, petition = True, False
    for proposal_type in ['sponsors', 'cosponsors']:
        if bill.get(proposal_type):
            for legislator in bill[proposal_type]:
                uid = ly_common.GetLegislatorId(c, legislator)
                if uid:
                    legislator_id = ly_common.GetLegislatorDetailId(c, uid, ad)
                    if legislator_id:
                        LegislatorBill(legislator_id, bill['bill_ref'], priproposer, petition)
                priproposer = False
            petition = True
conn.commit()
print 'bills done'

f = codecs.open('bills_not_found_in_lyapi_bills.txt', 'w', encoding='utf-8')
dict_list = json.load(open('lyapi_ttsmotions.json'))
for motion in dict_list['entries']:
    if not motion['bill_refs']:
        continue
    print json.dumps(motion, sort_keys=True, indent=4, ensure_ascii=False)
    for bill_ref in motion['bill_refs']:
        if re.search(u'L', bill_ref):
            if BillExist(bill_ref):
                motion.update({"bill_id": bill_ref})
                ttsMotions(motion)
            else:
                f.write('not found in lyapi_bills:, %s, %s\n' % (bill_ref, motion['tts_key']))
f.close()
conn.commit()
print 'billmotions done'

c.execute('''
    select t.date, t.progress, t.bill_id
    from bill_ttsmotions t
    inner join (
        select bill_id, max(date) as MaxDate
        from bill_ttsmotions
        group by bill_id
    ) tm on t.bill_id = tm.bill_id and t.date = tm.MaxDate'''
)
response = c.fetchall()
c.executemany('''
    UPDATE bill_bill
    SET last_action_at = %s, last_action = %s
    WHERE uid = %s
''', response)
conn.commit()
print 'last action information of bill done'

c.execute('''
    SELECT
        legislator_id,
        COUNT(*) total,
        SUM(CASE WHEN priproposer=True THEN 1 ELSE 0 END) chief,
        SUM(CASE WHEN priproposer=False AND petition=False THEN 1 ELSE 0 END) proposal,
        SUM(CASE WHEN petition=True THEN 1 ELSE 0 END) petition
    FROM bill_legislator_bill
    GROUP BY legislator_id
''')
response = c.fetchall()
for r in response:
    param = dict(zip(['total', 'chief', 'proposal', 'petition'], r[1:]))
    c.execute('''
        UPDATE legislator_legislatordetail
        SET bill_param = %s
        WHERE id = %s
    ''', (param, r[0]))
conn.commit()
print 'Update Bill param of People'
print 'Succeed'
