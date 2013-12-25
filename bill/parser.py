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


def Bill(uid, summary):
    c.execute('''INSERT into bill_bill(uid, summary, hits) 
            SELECT %s, %s, 0
            WHERE NOT EXISTS (SELECT 1 FROM bill_bill WHERE uid = %s)''', (uid, summary, uid)
    )  

def LegislatorBill(legislator_id, bill_id, priproposer, petition):
    c.execute('''INSERT into bill_legislator_bill(legislator_id, bill_id, priproposer, petition)
            SELECT %s, %s, %s, %s
            WHERE NOT EXISTS (SELECT 1 FROM bill_legislator_bill WHERE legislator_id = %s AND bill_id = %s)''', (legislator_id, bill_id, priproposer, petition, legislator_id, bill_id)
    )      

def BillDetail(bill_id,article,before,after,description):
    c.execute('''INSERT into bill_billdetail(bill_id,article,before,after,description)
        SELECT %s,%s,%s,%s,%s
        WHERE NOT EXISTS (SELECT 1 FROM bill_billdetail WHERE bill_id = %s AND article = %s)''',(bill_id,article,before,after,description,bill_id,article)) 

conn = db_ly.con()
c = conn.cursor()

#f = codecs.open('no_committees.txt','w', encoding='utf-8')
dict_list = json.load(open('lyapi_bills.json'))
for bill in dict_list['entries']:
    if not bill['bill_ref'] or not re.search(u'L', bill['bill_ref']):
        continue
    print bill['bill_ref']
    #response_json = requests.get('http://api.ly.g0v.tw/v0/collections/bills/%s/data' % bill['bill_ref']).json()
    print 'bill summary: %s' %  bill['summary']
    #law_match = re.search(u'(法|條例)', name[1:])
    #law = re.sub(u'[「(（｛]', '', name[:law_match.end()+1])
    Bill(bill['bill_ref'], bill['summary'])
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
