#! /usr/bin/env python
# -*- coding: utf-8 -*-
import re
import codecs
import psycopg2
from datetime import datetime

def GetSessionROI(text):
    ms ,me, uid = re.search(u'立法院(第(?P<ad>[\d]+)屆第(?P<session>[\d]+)會期第(?P<times>[\d]+)次(臨時會第(?P<temptimes>[\d]+)次)?會議)議事錄',text) , None, None
    if ms:
        if ms.group('temptimes'):
            uid = '%02d-%02dT%02d-YS-%02d' % (int(ms.group('ad')), int(ms.group('session')), int(ms.group('temptimes')), int(ms.group('times')))
        else:
            uid = '%02d-%02d-YS-%02d' % (int(ms.group('ad')), int(ms.group('session')), int(ms.group('times')))
        me = re.search(u'立法院第(\d){1,2}屆第[\d]{1,2}會期第[\d]{1,2}次(臨時會第\d次)?會議議事錄',text[1:])     
    return ms ,me, uid

def GetDate(text):
    matchTerm = re.search(u'(?P<year>[\d]+)年(?P<month>[\d]+)月(?P<day>[\d]+)',text)
    if matchTerm:
        return '%04d-%02d-%02d' % (int(matchTerm.group('year'))+1911, int(matchTerm.group('month')), int(matchTerm.group('day')))
    else:
        return None              

def GetLegislatorId(c, name):
    name_like = name + '%'
    c.execute('''SELECT uid FROM legislator_legislator WHERE name like %s''',[name_like])
    r = c.fetchone()
    if r:
        return r[0]

def InsertSitting(c, sitting_dict):
    c.execute('''INSERT into sittings_sittings(uid, name, date, ad, session)
        SELECT %(uid)s,%(name)s,%(date)s,%(ad)s,%(session)s
        WHERE NOT EXISTS (SELECT 1 FROM sittings_sittings WHERE uid = %(uid)s )''', sitting_dict)
