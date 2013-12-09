#! /usr/bin/env python
# -*- coding: utf-8 -*-
import re
import codecs
import psycopg2
from datetime import datetime

def GetSessionROI(text):
    ms , me = re.search(u'立法院第(\d){1,2}屆第[\d]{1,2}會期第[\d]{1,2}次(臨時會第\d次)?會議議事錄',text) , None
    if ms:
        me = re.search(u'立法院第(\d){1,2}屆第[\d]{1,2}會期第[\d]{1,2}次(臨時會第\d次)?會議議事錄',text[1:])     
    return ms , me
def GetDate(text):
    matchTerm = re.search(u'(\d)+年(\d)+月(\d)+',text)
    if not matchTerm:
        return None
    matchDate = re.sub('[^0-9]', ' ', matchTerm.group())
    if matchDate:
        dateList = matchDate.split()
        sessionDate = str(int(dateList[0])+1911)
        for e in dateList[1:]:
            if len(e) == 1:
                sessionDate = sessionDate + '-0' + e
            else:
                sessionDate = sessionDate + '-' + e
    else:
        sessionDate = None              
    return sessionDate



