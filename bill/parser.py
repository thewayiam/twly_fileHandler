#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../')
import requests
import re
import codecs
import psycopg2
import json
import db_ly
import ly_common


def Bill(bill):
    c.execute('''INSERT into bill_bill(uid, abstract, summary, bill_type, doc, proposed_by, sitting_introduced) 
            SELECT %(bill_ref)s, %(abstract)s, %(summary)s, %(bill_type)s, %(doc)s, %(proposed_by)s, %(sitting_introduced)s 
            WHERE NOT EXISTS (SELECT 1 FROM bill_bill WHERE uid = %(bill_ref)s)''', bill
    )  

def LegislatorBill(legislator_id, bill_id, priproposer, petition):
    c.execute('''INSERT into bill_legislator_bill(legislator_id, bill_id, priproposer, petition)
            SELECT %s, %s, %s, %s
            WHERE NOT EXISTS (SELECT 1 FROM bill_legislator_bill WHERE legislator_id = %s AND bill_id = %s)''', (legislator_id, bill_id, priproposer, petition, legislator_id, bill_id)
    )      

def BillMotions(motion):
    c.execute('''INSERT into bill_billmotions(bill_id, sitting_id, agenda_item, committee, item, motion_class, resolution, status)
        SELECT %(bill_id)s, %(sitting_id)s, %(agenda_item)s, %(committee)s, %(item)s, %(motion_class)s, %(resolution)s, %(status)s 
        WHERE NOT EXISTS (SELECT 1 FROM bill_billmotions WHERE bill_id = %(bill_id)s AND sitting_id = %(sitting_id)s)''', motion
    ) 

conn = db_ly.con()
c = conn.cursor()

#f = codecs.open('no_committees.txt','w', encoding='utf-8')
dict_list = json.load(open('lyapi_bills.json'))
for bill in dict_list['entries']:
    if not bill['bill_ref'] or not re.search(u'L', bill['bill_ref']):
        continue
    print bill['bill_ref']
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
                print legislator
                legislator_id = ly_common.GetLegislatorId(c, legislator)
                if legislator_id:
                    legislator_id = ly_common.GetLegislatorDetailId(c, legislator_id, 8)
                    LegislatorBill(legislator_id, bill['bill_ref'], priproposer, petition)
                priproposer = False
            petition = True
#f.close()
conn.commit()
print 'Succeed'
