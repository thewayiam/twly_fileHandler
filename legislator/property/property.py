#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../../')
import re
import codecs
import json
from operator import itemgetter
from pandas import *
import pandas as pd
from numpy import nan
import numpy as np
import ly_common


def get_date(rows):
    for row in rows:
        if pd.notnull(row):
            date = ly_common.GetDate(row)
            if date:
                return date

def get_name(rows):
    bookmarks = []
    for row in reversed(rows):
        if pd.notnull(row):
            match = re.search(u'申報人\S(?P<name>\S*)', row)
            if match:
                return match.group('name')

def get_table_range(rows, targets):
    bookmarks = []
    for target in targets:
        for i in range(0, len(rows)):
            if pd.notnull(rows[i]):
                if re.search(target, rows[i]):
                    bookmarks.append({"name": target, "position": i})
                    break
    return sorted(bookmarks, key=itemgetter('position'))

df_orgi = pd.read_excel('data/tmped051.xlsx', 0, header=None)
df_orgi.replace(to_replace=u'監察院公報\S*', regex=True, value=nan, inplace=True)
first_column = df_orgi[df_orgi.columns[0]]
categories = [u'土地', u'建物', u'船舶', u'汽車', u'航空器', u'現金', u'存款', u'股票', u'債券', u'基金受益憑證', u'其他有價證券', u'其他具有相當價值之財產', u'保險', u'債權', u'債務', u'事業投資', u'備註']
bookmarks = get_table_range(first_column, categories)
date = get_date(df_orgi[df_orgi.columns[1]])
name = get_name(df_orgi[df_orgi.columns[0]])
writer = pd.ExcelWriter('output/%s-%s-%s.xlsx' % (name, date, df_orgi[0][0]), engine='xlsxwriter')
for i in range(0, len(bookmarks) - 1):
    df = df_orgi[bookmarks[i]['position']:bookmarks[i+1]['position']]
    df.dropna(inplace=True, axis=1, how='all', thresh=2)
    df = df_orgi[bookmarks[i]['position']:bookmarks[i+1]['position']].dropna(how='any', subset=[0, 1])
    if not df.empty:
        df.columns = [x.replace(' ', '') for x in df.iloc[0].replace(nan, '')]
    df.to_excel(writer, sheet_name=bookmarks[i]['name'])
    #print df.to_json(orient='records')
writer.save()
#output_pretty_file = codecs.open('./data(pretty_format)/merged.json', 'w', encoding='utf-8')
#dump_data = json.dumps(npl_dict_list, sort_keys=True, indent=4, ensure_ascii=False)
#output_pretty_file.write(dump_data)
#output_pretty_file.close()
