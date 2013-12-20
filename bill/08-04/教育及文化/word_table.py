#! /usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2,win32com,glob,os,re,difflib
from win32com.client import Dispatch, constants

def AddBill(billid,proposalid,law,title,motivation,description,committee,sessionPrd):
    c.execute('''INSERT into bill_bill(billid,proposalid,law,title,motivation,description,committee,"sessionPrd",hits)
        SELECT %s,%s,%s,%s,%s,%s,%s,%s,0
        WHERE NOT EXISTS (SELECT billid,proposalid,law,title,motivation,description,committee,"sessionPrd",hits FROM bill_bill WHERE billid = %s AND proposalid = %s) RETURNING id''',(billid,proposalid,law,title,motivation,description,committee,sessionPrd,billid,proposalid))
    r = c.fetchone()
    if r:
        return r[0]
def LyID(name):
    c.execute('''SELECT id FROM legislator_legislator WHERE name = %s''',[name])
    return c.rowcount,c.fetchone()
def MakeBillRelation(legislator_id,bill_id,priproposer):
    c.execute('''INSERT into bill_legislator_bill(legislator_id,bill_id,priproposer)
        SELECT %s,%s,%s
        WHERE NOT EXISTS (SELECT legislator_id,bill_id,priproposer FROM bill_legislator_bill WHERE legislator_id = %s AND bill_id = %s)''',(legislator_id,bill_id,priproposer,legislator_id,bill_id))
def AddBillDetail(bill_id,article,before,after,description):
    c.execute('''INSERT into bill_billdetail(bill_id,article,before,after,description)
        SELECT %s,%s,%s,%s,%s
        WHERE NOT EXISTS (SELECT bill_id,article,before,after,description FROM bill_billdetail WHERE bill_id = %s AND article = %s)''',(bill_id,article,before,after,description,bill_id,article))
def LiterateProposer(text,bill_id):
    firstName,priproposer = '',True
    for name in text.split():
        if re.search(u'[）)。】」]$',name):   #立委名字後有標點符號
            name = name[:-1]
        #兩個字的立委中文名字中間有空白
        if len(name)<2 and firstName=='':
            firstName = name
            continue
        if len(name)<2 and firstName!='':
            name = firstName + name
            firstName = ''
        if len(name)>4: #立委名字相連或名字後加英文
            name = name[:3]
        rowcount,lyid = LyID(name)
        if rowcount == 1 and lyid:
            MakeBillRelation(lyid[0],bill_id,priproposer)
            priproposer = False

conn = psycopg2.connect(dbname='ly', host='localhost', user='postgres' , password='P@ssw0rd')
c = conn.cursor()
msword = Dispatch('Word.Application')
msword.Visible = 0

files = [f for f in glob.glob('./*.doc')]
committee = os.getcwdu().split('\\')[-1]
print committee
for f in files:
    doc = msword.Documents.Open(os.getcwd()+f,False,False,False)
    ### Extract id
    id_match = re.search(u'院總第',doc.Tables[0].Rows(1).Range.Text)
    if id_match:
        id_string_list = re.sub(u'[\D]',' ',doc.Tables[0].Rows(1).Range.Text).split()
        billid, proposalid = id_string_list[0], id_string_list[1]
    else:
        print f + u'找不到院總第'
    ### Extract title, delete other table in doc
    for table in doc.Tables:
        match = re.search(u'(對照表|草案|增訂條文|修正條文|條文)[\r\n]',table.Cell(1, 1).Range.Text)
        if not match:
            table.Delete()
            continue
        else:
            title = re.sub(u'對照表[\r\n]','',table.Cell(1, 1).Range.Text).strip()
            title_match = re.search(table.Cell(1, 1).Range.Text,doc.Content.Text)
            print title
            break
    ### Extract motivation,description
    motivation_match,description_match,proposer_match,cosign_match = re.search(u'案由：',doc.Content.Text),re.search(u'說明：',doc.Content.Text),re.search(u'提案人：',doc.Content.Text),re.search(u'連署人：',doc.Content.Text)
    if motivation_match and description_match:
        motivation = doc.Content.Text[motivation_match.end():description_match.start()].strip()
    elif motivation_match and proposer_match:
        motivation = doc.Content.Text[motivation_match.end():proposer_match.start()].strip()
    else:
        print f + u'找不到案由起訖'
    if description_match and proposer_match:
        description = '\n\n'.join(doc.Content.Text[description_match.end():proposer_match.start()].split())
    elif motivation_match and proposer_match:
        description = None
    else:
        print f + u'找不到說明起訖'
    if proposer_match:
        if cosign_match:
            proposer_text = doc.Content.Text[proposer_match.end():cosign_match.start()]
        elif title_match:
            proposer_text = doc.Content.Text[proposer_match.end():title_match.start()]
        else:
            print f + u'找不到提案人起訖'
    re_title = False
    if re.match(u'(增訂|修正)?條文[\r\n]',title):
        title_rematch = re.search(u'[\S]*草案',doc.Content.Text[proposer_match.end():])
        title = title_rematch.group()
        re_title = True
        print 're title:' + title
    law_match = re.search(u'(法|條例)',title[1:])
    law = re.sub(u'[「(（｛]','',title[:law_match.end()+1])
    bill_id = AddBill(billid,proposalid,law,title,motivation,description,committee,'8')
    if bill_id:
        LiterateProposer(proposer_text,bill_id)
        ### Extract bill detail
        if re_title:
            startrow = 2
        else:
            startrow = 3
        for row in range(startrow , doc.Tables[0].Rows.Count+1):
            if doc.Tables[0].Columns.Count == 3:
                before ,after ='', ''
                a, b = doc.Tables[0].Cell(row, 2).Range.Text.split(), doc.Tables[0].Cell(row, 1).Range.Text.split()
                d = difflib.Differ()
                for line in d.compare(a, b):
                    if line[0] == ' ':
                        before += line + '\n'
                        after += line + '\n'
                    elif line[0] == '-':
                        before += '<font style="background-color: #FFFF66;">' + line[2:] + '</font>\n'
                    elif line[0] == '+':
                        after += '<font style="background-color: #CCFF99;">' + line[2:] + '</font>\n'
                article = doc.Tables[0].Cell(row, 1).Range.Text.split()[0]
                AddBillDetail(bill_id, article, before, after, doc.Tables[0].Cell(row, 3).Range.Text)
            elif doc.Tables[0].Columns.Count == 2:
                article = doc.Tables[0].Cell(row, 1).Range.Text.split()[0]
                AddBillDetail(bill_id, article, None, doc.Tables[0].Cell(row, 1).Range.Text, doc.Tables[0].Cell(row, 2).Range.Text)
            else:
                print 'table column count not 2 or 3'

    ### SaveAs and close
    #doc.SaveAs(os.getcwd() + '\\remove\\' + f )
    doc.Close(False)
msword.Application.Quit(-1)
conn.commit()
print 'finish'
