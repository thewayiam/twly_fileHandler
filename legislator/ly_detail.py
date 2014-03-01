#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../')
import re
import codecs
import psycopg2
import json
import db_ly
import ly_common

def GetdistrictDetail(text, eleDistrict, name):
    ms, me = re.search(eleDistrict, text) , re.search(name, text)
    if ms and me:
        return text[ ms.end(): me.start()].strip()

def GetSessionEndPoint(text):
    return text.find(u"電話：")

def LyID(name):
    c.execute('''
        SELECT id
        FROM legislator_legislator
        WHERE name = %s
    ''', [name])
    return c.rowcount,c.fetchone()

def LyDetail(lyid, eleDistrict, district, party, districtDetail):
    c.execute('''
        UPDATE legislator_legislator
        SET "eleDistrict" = %s, district = %s, party = %s, "districtDetail" = %s, enabledate = '2012-02-01'
        WHERE id = %s
    ''', (eleDistrict, district, party, districtDetail, lyid))

def LiterateLegislator(text, eleDistrict, district, party, sourcetext2):
    firstName = ''
    for name in text.split():
        rowcount,lyid = LyID(name)
        if rowcount == 1 and lyid:
            districtDetail = GetdistrictDetail(sourcetext2, eleDistrict, name)
            LyDetail(lyid[0], eleDistrict, district, party, districtDetail)
            break

conn = db_ly.con()
c = conn.cursor()
sourcetext = codecs.open(u"08立委個資.txt", "r", "utf-8").read()
sourcetext2 = codecs.open(u"08立委選區範圍.txt", "r", "utf-8").read()
endP = GetSessionEndPoint(sourcetext)
while(endP != -1):
    singleSessionText = sourcetext[:endP]
    eleDistrict = singleSessionText.rstrip().split()[-1]
    party = singleSessionText.rstrip().split()[-2]
    m = re.search(u'([\W]{2})(縣|市)', eleDistrict)
    if m:
        district = m.group(1) + m.group(2)
    else:
        district = eleDistrict
    #print district
    LiterateLegislator(singleSessionText, eleDistrict, district, party, sourcetext2)
    sourcetext = sourcetext[endP+1:]
    endP = GetSessionEndPoint(sourcetext)

c.execute('''
    update legislator_legislator
    set term_end=CAST('2016-02-01' AS DATE)
    where term_end isnull
''')
c.execute('''
    update legislator_legislator
    set term_start=CAST('2012-02-01' AS DATE)
    where term_start isnull
''')
conn.commit()
print 'Succeed'
