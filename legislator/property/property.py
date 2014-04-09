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
import ly_common


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
                return df[df.columns[1]][rowcount]
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
    return sorted(bookmarks, key=itemgetter('position'))

files = [f for f in glob.glob('data/*.xlsx')]
categories = [u'土地', u'建物', u'船舶', u'汽車', u'航空器', u'現金', u'存款', u'股票', u'債券', u'基金受益憑證', u'其他有價證券', u'其他具有相當價值之財產', u'保險', u'債權', u'債務', u'事業投資', u'備註', u'備往']
for f in files:
    df_orgi = pd.read_excel(f, 0, header=None, encoding='utf-8')
    df_orgi.replace(to_replace=u'監察院公報\S*', regex=True, value=nan, inplace=True)
    first_column = df_orgi[df_orgi.columns[0]]
    bookmarks = get_table_range(first_column, categories)
    date = get_date(df_orgi)
    name = get_name(df_orgi)
    writer = pd.ExcelWriter('output/%s-%s-%s-%s.xlsx' % (os.path.splitext(os.path.basename(f))[0], name, date, df_orgi[0][0]), engine='xlsxwriter')
    for i in range(0, len(bookmarks) - 1):
        df = df_orgi[bookmarks[i]['position']:bookmarks[i+1]['position']]
        df.dropna(inplace=True, axis=1, how='all', thresh=2)
        df = df_orgi[bookmarks[i]['position']:bookmarks[i+1]['position']].dropna(how='any', subset=[0, 1])
        if not df.empty:
            df.columns = map(lambda x: x.replace(' ', '') if isinstance(x, basestring) else x, df.iloc[0].replace(nan, ''))
        df.to_excel(writer, sheet_name=bookmarks[i]['name'])
    writer.save()
#output_pretty_file = codecs.open('./data(pretty_format)/merged.json', 'w', encoding='utf-8')
#dump_data = json.dumps(npl_dict_list, sort_keys=True, indent=4, ensure_ascii=False)
#output_pretty_file.write(dump_data)
#output_pretty_file.close()
