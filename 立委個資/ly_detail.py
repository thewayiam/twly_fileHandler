#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys # 正常模組
sys.path.append('../')
import re,codecs,psycopg2
import db_ly

def GetdistrictDetail(text,eleDistrict,name):
    ms, me = re.search(eleDistrict,text) , re.search(name,text)
    if ms and me:
        return text[ ms.end(): me.start()].strip()
def GetSessionEndPoint(text):
    return text.find(u"電話：")
def LyID(name):
    c.execute('''SELECT id FROM legislator_legislator WHERE name = %s''',[name])
    return c.rowcount,c.fetchone()
def LyDetail(lyid,eleDistrict,district,party,districtDetail):
    c.execute('''UPDATE legislator_legislator SET "eleDistrict" = %s, district = %s, party = %s, "districtDetail" = %s, enabledate='2012-02-01' WHERE id = %s''',(eleDistrict,district,party,districtDetail,lyid))
def LiterateProposer(text,eleDistrict,district,party,sourcetext2):
    firstName = ''
    for name in text.split():      
        rowcount,lyid = LyID(name)
        if rowcount == 1 and lyid:
            districtDetail = GetdistrictDetail(sourcetext2,eleDistrict,name)
            LyDetail(lyid[0],eleDistrict,district,party,districtDetail)
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
    m = re.search(u'([\W]{2})(縣|市)',eleDistrict)
    if m:
        district = m.group(1)+m.group(2)
    else:
        district = eleDistrict
    #print district
    LiterateProposer(singleSessionText,eleDistrict,district,party,sourcetext2)
    sourcetext = sourcetext[endP+1:]
    endP = GetSessionEndPoint(sourcetext)

def GetCommitteeEndPoint(text):
    return re.search(u'([\S]{2,9})委員會',text), re.search(u'委員名單',text)
def LyDetail(lyid,committee):
    c.execute('''UPDATE legislator_legislator SET committee = %s WHERE id = %s''',(committee,lyid))
def LiterateLY(text,committee):
    for name in text.split():      
        rowcount,lyid = LyID(name)
        if rowcount == 1 and lyid:
            LyDetail(lyid[0],committee)
        else:
            break         

text = codecs.open(u"0803立委委員會.txt", "r", "utf-8").read()
title , start = GetCommitteeEndPoint(text)
while(title and start):
    committee = title.group(1)
    text = text[start.end():]
    LiterateLY(text,committee)
    title , start = GetCommitteeEndPoint(text)

def Lyfb(url,lyid):
    c.execute('''UPDATE legislator_legislator SET facebook = %s WHERE id = %s''',(url,lyid))
def Lywiki(url,lyid):
    c.execute('''UPDATE legislator_legislator SET wiki = %s WHERE id = %s''',(url,lyid))
def Lyofficialsite(url,lyid):
    c.execute('''UPDATE legislator_legislator SET officialsite = %s WHERE id = %s''',(url,lyid))

sourcetext = codecs.open(u"08立委個資.txt", "r", "utf-8").read()
endP = GetSessionEndPoint(sourcetext)
while(endP != -1):
    singleSessionText = sourcetext[:endP]
    mfb = re.search(u"FB",singleSessionText)
    mwiki = re.search(u"wiki",singleSessionText)
    mofficialsite = re.search(u"立院官網",singleSessionText)
    ly = singleSessionText[:mfb.start()].rstrip().split()[-1]
    rowcount,lyid = LyID(ly)
    if rowcount == 1 and lyid:    
        facebook = singleSessionText[mfb.end():].rstrip().split()[0]
        if facebook != u"wiki":
            Lyfb(facebook,lyid[0])
        wiki = singleSessionText[mwiki.end():].rstrip().split()[0]
        if wiki != u"立院官網":
            Lywiki(wiki,lyid[0])    
        officialsite = singleSessionText[mofficialsite.end():].rstrip().split()[0]
        Lyofficialsite(officialsite,lyid[0])     
    #print ly,facebook,wiki,officialsite
    sourcetext = sourcetext[endP+1:]
    endP = GetSessionEndPoint(sourcetext)
conn.commit()
print 'Succeed'



