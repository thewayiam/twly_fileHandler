#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../')
import codecs
import db_ly
import ly_common


def LegislatorDetail(legislator_id, district):
    c.execute('''UPDATE legislator_legislatordetail
            SET district = %s
            WHERE id = %s''', (district, legislator_id)
    )

conn = db_ly.con()
c = conn.cursor()

f = codecs.open('08立委選區範圍.txt','r', encoding='utf-8')
for line in f:
    counter = 1
    for word in line.split()[1:]:
        legislator_id = ly_common.GetLegislatorId(c, word)
        if legislator_id:
            legislator_id = ly_common.GetLegislatorDetailId(c, legislator_id, 8)
            LegislatorDetail(legislator_id, ' '.join(line.split()[1:counter]))
        counter += 1
conn.commit()
f.close()
print 'Succeed'
