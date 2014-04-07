#! /usr/bin/env python
# -*- coding: utf-8 -*-
import re
import codecs
import json
from pandas import *
import pandas as pd
from numpy import nan
import numpy as np


def get_table_range(rows, target):
    start_end = {}
    for i in range(0, len(rows)):
        if start_end and i - start_end["start"] > 1 and pd.isnull(rows[i]):
            start_end["end"] = i
            return start_end
        if pd.notnull(rows[i]):
            if re.search(target, rows[i]):
                start_end["start"] = i
        elif start_end.has_key("start"):
            start_end["start"] = i
    print rows, target

def clean_page_mark(df):
    rows = df_orgi[df_orgi.columns[0]]
    for i in range(0, len(rows)):
        if pd.notnull(rows[i]):
            if re.search(u'監察院公報', rows[i]):
                return df.drop(df.index[i-1:i+2])

df_orgi = pd.read_excel('data/tmp660b1.xlsx', 0)
#df_orgi = clean_page_mark(df_orgi)
first_column = df_orgi[df_orgi.columns[0]]
categories = [u'土地', u'建物', u'船舶', u'汽車', u'航空器', u'現金', u'存款', u'股票', u'債券', u'基金受益憑證', u'其他有價證券', u'珠寶、古董、字畫', u'保險', u'債權', u'債務', u'事業投資', u'備註']
writer = pd.ExcelWriter('data.xlsx', engine='xlsxwriter')
for category in categories:
    roi = get_table_range(first_column, category)
    df_orgi[roi['start']+1:roi['end']].to_excel(writer, sheet_name=category)
writer.save()
