#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys # 正常模組
sys.path.append('../')
import re,codecs,psycopg2
import db_ly

def AddPartyPolitic(politic,category,party):
    c.execute('''INSERT into legislator_politics(politic,category,party) 
        SELECT %s,%s,%s
        WHERE NOT EXISTS (SELECT politic,category,party FROM legislator_politics WHERE politic = %s AND party = %s )''',(politic,category,party,politic,party))  
def LyID(name):
    c.execute('''SELECT id FROM legislator_legislator WHERE name = %s''',[name])
    return c.rowcount,c.fetchone()
def AddPolitic(legislator_id,politic,category):
    c.execute('''INSERT into legislator_politics(legislator_id,politic,category) 
        SELECT %s,%s,%s
        WHERE NOT EXISTS (SELECT legislator_id,politic,category FROM legislator_politics WHERE legislator_id = %s AND politic = %s )''',(legislator_id,politic,category,legislator_id,politic))  
conn = db_ly.con()
c = conn.cursor()
sourcetext = codecs.open(u"08立委政見.txt", "r", "utf-8")
ly = []
for line in sourcetext.readlines():
    line = line.strip()
    if not line:
        continue
    if len(line) < 5:
        rowcount,lyid = LyID(line)
        if rowcount == 1 and lyid:
            ly = lyid[0]
            print line
        else:
            print 'can not find ly' , line
    else:
        if ly:
            if re.search(u'[：]$',line):
                AddPolitic(ly,line,0)
            else:
                AddPolitic(ly,line,1)
conn.commit()
print u'08立委政見Succeed'
sourcetext = codecs.open(u"08政黨政見.txt", "r", "utf-8")
party = []
for line in sourcetext.readlines():
    line = line.strip()
    if not line:
        continue
    if len(line) < 7 and re.search(u'(?<!：)$',line):
        party = line
        print line
    else:
        if party:
            if re.search(u'[：]$',line):
                AddPartyPolitic(line,0,party)
            else:
                AddPartyPolitic(line,1,party)
conn.commit()
print u'08政黨政見Succeed'
