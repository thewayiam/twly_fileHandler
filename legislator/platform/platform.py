#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../../')
import re
import codecs
import db_settings
import ly_common


def personalPlatform(platform, id):
    platform = '\n'.join(platform)
    c.execute('''
        UPDATE legislator_legislatordetail
        SET platform = %s
        WHERE id = %s
    ''', (platform, id))

def partyPlatform(platform, ad, party):
    platform = '\n'.join(platform)
    c.execute('''
        UPDATE legislator_legislatordetail
        SET platform = %s
        WHERE ad = %s AND party = %s AND constituency = 0
    ''', (platform, ad, party))

conn = db_settings.con()
c = conn.cursor()

ad = 8
sourcetext = codecs.open(u"%d立委政見.txt" % ad, "r", "utf-8")
lines = []
for line in sourcetext.readlines():
    line = line.strip()
    lines.append(line)
    if not line:
        uid = ly_common.GetLegislatorId(c, lines[0])
        if uid: # if this line is name of legislators
            legislator_id = ly_common.GetLegislatorDetailId(c, uid, ad)
        else:
            print lines[0]
            raw_input()
        personalPlatform(lines[1:], legislator_id)
        lines = []
conn.commit()
print u'8立委政見Succeed'

sourcetext = codecs.open(u"%d政黨政見.txt" % ad, "r", "utf-8")
lines = []
for line in sourcetext.readlines():
    line = line.strip()
    lines.append(line)
    if not line:
        partyPlatform(lines[1:], ad, lines[0])
        lines = []
conn.commit()
print u'8政黨政見Succeed'
