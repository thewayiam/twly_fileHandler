#! /usr/bin/env python
# -*- coding: utf-8 -*-
import re
import codecs
import psycopg2
from datetime import datetime

def GetSessionROI(text):
    ms , me = re.search(u'立法院(第(?P<ad>[\d]+)屆第(?P<session>[\d]+)會期第[\d]{1,2}次(臨時會第\d次)?會議)議事錄',text) , None
    if ms:
        me = re.search(u'立法院第(\d){1,2}屆第[\d]{1,2}會期第[\d]{1,2}次(臨時會第\d次)?會議議事錄',text[1:])     
    return ms , me
def GetDate(text):
    matchTerm = re.search(u'(?P<year>[\d]+)年(?P<month>[\d]+)月(?P<day>[\d]+)',text)
    if matchTerm:
        return '%04d-%02d-%02d' % (int(matchTerm.group('year'))+1911, int(matchTerm.group('month')), int(matchTerm.group('day')))
    else:
        return None              
