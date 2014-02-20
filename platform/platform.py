#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../')
import re
import codecs
import db_ly
import ly_common


def PartyPlatform(content, category, party):
    c.execute('''
        INSERT into legislator_platform(content, category, party)
        SELECT %s, %s, %s
        WHERE NOT EXISTS (SELECT 1 FROM legislator_platform WHERE content = %s AND party = %s )
    ''', (content, category, party, content, party))

def PersonalPlatform(legislator_id, content, category):
    c.execute('''
        INSERT into legislator_platform(legislator_id, content, category)
        SELECT %s, %s, %s
        WHERE NOT EXISTS (SELECT 1 FROM legislator_platform WHERE legislator_id = %s AND content = %s )
    ''',(legislator_id, content, category, legislator_id, content))

conn = db_ly.con()
c = conn.cursor()
sourcetext = codecs.open(u"08立委政見.txt", "r", "utf-8")
ly = []
for line in sourcetext.readlines():
    line = line.strip()
    if not line:
        continue
    uid = ly_common.GetLegislatorId(c, line)
    if uid:
        legislator_id = ly_common.GetLegislatorDetailId(c, uid, 8)
    else:
        if legislator_id:
            if re.search(u'[：]$',line):
                PersonalPlatform(legislator_id, line, 0)
            else:
                PersonalPlatform(legislator_id, line, 1)
conn.commit()
print u'08立委政見Succeed'
sourcetext = codecs.open(u"08政黨政見.txt", "r", "utf-8")
party = []
for line in sourcetext.readlines():
    line = line.strip()
    if not line:
        continue
    if len(line) < 7 and re.search(u'(?<!：)$', line):
        party = line
        print line
    else:
        if party:
            if re.search(u'[：]$', line):
                PartyPlatform(line, 0, party)
            else:
                PartyPlatform(line, 1, party)
conn.commit()
print u'08政黨政見Succeed'
