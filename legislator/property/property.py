#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../../')
import os
import re
import codecs
import json
import glob
from operator import itemgetter
from pandas import *
import pandas as pd
from numpy import nan
import numpy as np
import db_ly
import ly_common


def get_type(df):
    rows = df[df.columns[0]]
    for row in rows:
        if pd.notnull(row) and isinstance(row, basestring):
            match = re.search(u'(?:公職人員)?(\S*(申報|通知)表)', row)
            if match:
                return match.group(1)

def get_date(df):
    for column in [1, 0]:
        rows = df[df.columns[column]]
        for row in rows:
            if pd.notnull(row) and isinstance(row, basestring):
                date = ly_common.GetDate(row)
                if date:
                    return date

def get_name(df):
    rowcount = 0
    rows = df[df.columns[0]]
    for row in rows:
        if pd.notnull(row) and isinstance(row, basestring):
            if re.search(u'申報人姓名', row):
                potential_list = [df[df.columns[1]][rowcount], df[df.columns[2]][rowcount]]
                return [name for name in potential_list if pd.notnull(name)][0]
        rowcount += 1
    for row in reversed(df[df.columns[0]]):
        if pd.notnull(row) and isinstance(row, basestring):
            match = re.search(u'申報人\S(?P<name>\S*)', row)
            if match:
                return match.group('name')

def get_table_range(rows, targets):
    bookmarks = []
    for target in targets:
        for i in range(0, len(rows)):
            if pd.notnull(rows[i]) and isinstance(rows[i], basestring):
                if re.search(target, rows[i]):
                    bookmarks.append({"name": target, "position": i})
                    break
        #print '%s not found!' % target
    return sorted(bookmarks, key=itemgetter('position'))

conn = db_ly.con()
c = conn.cursor()
files = [f for f in glob.glob('data/*.xlsx')]
categories = [u'土地', u'建物', u'船舶', u'汽車', u'航空器', u'現金', u'存款', u'有價證券', u'股票', u'債券', u'基金受益憑證', u'其他有價證券', u'具有相當價值之財產', u'保險', u'債權', u'債務', u'事業投資', u'備註', u'此致']
models = {
    u"股票": {
        "columns": ['name', 'owner', 'quantity', 'face_value', 'currency', 'total']
    }
}
for f in files:
    print f
    df_orgi = pd.read_excel(f, 0, header=None, encoding='utf-8')
    df_orgi.replace(to_replace=u'監察院公報\S*', regex=True, value=nan, inplace=True)
    bookmarks = get_table_range(df_orgi[df_orgi.columns[0]], categories)
    title = get_type(df_orgi)
    date = get_date(df_orgi)
    name = get_name(df_orgi)
    if title == u'財產申報表':
        legislator_id = ly_common.GetLegislatorId(c, name)
        writer = pd.ExcelWriter('output/normal/%s_%s_%s_%s.xlsx' % (name, date, title, os.path.splitext(os.path.basename(f))[0]), engine='xlsxwriter')
        for i in range(0, len(bookmarks) - 1):
            df = df_orgi[bookmarks[i]['position'] + 1 : bookmarks[i+1]['position']]
            df.dropna(inplace=True, how='any', subset=[0, 1]) # Drop row if column 0 or 1 empty
            df.dropna(inplace=True, how='all', axis=1, thresh=2) # Drop column if non-nan cell not more than one
            if not df.empty:
                df[1:] = df[ df[0] != df.iloc[0][0] ]
                df.dropna(inplace=True, how='any', subset=[0, 1]) # Drop if column 0 or 1 empty
                if bookmarks[i]['name'].strip() == u"股票" or bookmarks[i]['name'].strip() == u"有價證券":
                    df.columns = models[u"股票"]["columns"]
                    df['date'] = date
                    df['legislator_name'] = name
                    df['legislator_id'] = legislator_id
                   #print df[1:].to_json(orient='records')
                   #output_file = codecs.open('output/property.json', 'w', encoding='utf-8')
                   #dict_list = json.loads(df[1:].to_json(orient='records'))
                   #dump_data = json.dumps(dict_list, sort_keys=True, indent=4, ensure_ascii=False)
                   #output_file.write(dump_data)
                   #output_file.close()
                else:
                    df.columns = map(lambda x: x.replace(' ', '') if isinstance(x, basestring) else x, df.iloc[0].replace(nan, ''))
                df[1:].to_excel(writer, sheet_name=bookmarks[i]['name'])
        writer.save()
    elif title == u'變動財產申報表':
        writer = pd.ExcelWriter('output/change/%s_%s_%s_%s.xlsx' % (name, date, title, os.path.splitext(os.path.basename(f))[0]), engine='xlsxwriter')
    elif title == u'信託財產申報表':
        writer = pd.ExcelWriter('output/trust/%s_%s_%s_%s.xlsx' % (name, date, title, os.path.splitext(os.path.basename(f))[0]), engine='xlsxwriter')
    else:
        writer = pd.ExcelWriter('output/others/%s_%s_%s_%s.xlsx' % (name, date, title, os.path.splitext(os.path.basename(f))[0]), engine='xlsxwriter')
