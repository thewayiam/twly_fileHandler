#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../../')
import os
import re
import codecs
import glob
from operator import itemgetter
import json
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
    return sorted(bookmarks, key=itemgetter('position'))

def get_portion(value):
    match = re.search(u'(?P<divider>\d+)\D+(?P<divide>\d+)', value)
    if match:
        return float(match.group('divide')) / float(match.group('divider'))
    if re.search(u'全部', value):
        return 1.0
    print value

def upsert_property_stock(dataset):
    c.executemany('''
        UPDATE property_stock
        SET legislator_id = %(legislator_id)s, date = %(date)s, category = %(category)s, name = %(name)s, owner = %(owner)s, quantity = %(quantity)s, face_value = %(face_value)s, currency = %(currency)s, total = %(total)s
        WHERE index = %(index)s and source_file = %(source_file)s
    ''', dataset)
    c.executemany('''
        INSERT INTO property_stock(legislator_id, date, category, name, owner, quantity, face_value, currency, total, source_file, index)
        SELECT %(legislator_id)s, %(date)s, %(category)s, %(name)s, %(owner)s, %(quantity)s, %(face_value)s, %(currency)s, %(total)s, %(source_file)s, %(index)s
        WHERE NOT EXISTS (SELECT 1 FROM property_stock WHERE index = %(index)s and source_file = %(source_file)s)
    ''', dataset)

def upsert_property_land(dataset):
    c.executemany('''
        UPDATE property_land
        SET legislator_id = %(legislator_id)s, date = %(date)s, category = %(category)s, name = %(name)s, area = %(area)s, share_portion = %(share_portion)s, portion = %(portion)s, owner = %(owner)s, register_date = %(register_date)s, register_reason = %(register_reason)s, acquire_value = %(acquire_value)s, total = %(total)s
        WHERE index = %(index)s and source_file = %(source_file)s
    ''', dataset)
    c.executemany('''
        INSERT INTO property_land(legislator_id, date, category, name, area, share_portion, portion, owner, register_date, register_reason, acquire_value, total, source_file, index)
        SELECT %(legislator_id)s, %(date)s, %(category)s, %(name)s, %(area)s, %(share_portion)s, %(portion)s, %(owner)s, %(register_date)s, %(register_reason)s, %(acquire_value)s, %(total)s, %(source_file)s, %(index)s
        WHERE NOT EXISTS (SELECT 1 FROM property_land WHERE index = %(index)s and source_file = %(source_file)s)
    ''', dataset)

def upsert_property_building(dataset):
    c.executemany('''
        UPDATE property_building
        SET legislator_id = %(legislator_id)s, date = %(date)s, category = %(category)s, name = %(name)s, area = %(area)s, share_portion = %(share_portion)s, portion = %(portion)s, owner = %(owner)s, register_date = %(register_date)s, register_reason = %(register_reason)s, acquire_value = %(acquire_value)s, total = %(total)s
        WHERE index = %(index)s and source_file = %(source_file)s
    ''', dataset)
    c.executemany('''
        INSERT INTO property_building(legislator_id, date, category, name, area, share_portion, portion, owner, register_date, register_reason, acquire_value, total, source_file, index)
        SELECT %(legislator_id)s, %(date)s, %(category)s, %(name)s, %(area)s, %(share_portion)s, %(portion)s, %(owner)s, %(register_date)s, %(register_reason)s, %(acquire_value)s, %(total)s, %(source_file)s, %(index)s
        WHERE NOT EXISTS (SELECT 1 FROM property_building WHERE index = %(index)s and source_file = %(source_file)s)
    ''', dataset)

def upsert_property_boat(dataset):
    c.executemany('''
        UPDATE property_boat
        SET legislator_id = %(legislator_id)s, date = %(date)s, category = %(category)s, name = %(name)s, tonnage = %(tonnage)s, homeport = %(homeport)s, owner = %(owner)s, register_date = %(register_date)s, register_reason = %(register_reason)s, acquire_value = %(acquire_value)s
        WHERE index = %(index)s and source_file = %(source_file)s
    ''', dataset)
    c.executemany('''
        INSERT INTO property_boat(legislator_id, date, category, name, tonnage, homeport, owner, register_date, register_reason, acquire_value, source_file, index)
        SELECT %(legislator_id)s, %(date)s, %(category)s, %(name)s, %(tonnage)s, %(homeport)s, %(owner)s, %(register_date)s, %(register_reason)s, %(acquire_value)s, %(source_file)s, %(index)s
        WHERE NOT EXISTS (SELECT 1 FROM property_boat WHERE index = %(index)s and source_file = %(source_file)s)
    ''', dataset)

def upsert_property_car(dataset):
    c.executemany('''
        UPDATE property_car
        SET legislator_id = %(legislator_id)s, date = %(date)s, category = %(category)s, name = %(name)s, capacity = %(capacity)s, owner = %(owner)s, register_date = %(register_date)s, register_reason = %(register_reason)s, acquire_value = %(acquire_value)s
        WHERE index = %(index)s and source_file = %(source_file)s
    ''', dataset)
    c.executemany('''
        INSERT INTO property_car(legislator_id, date, category, name, capacity, owner, register_date, register_reason, acquire_value, source_file, index)
        SELECT %(legislator_id)s, %(date)s, %(category)s, %(name)s, %(capacity)s, %(owner)s, %(register_date)s, %(register_reason)s, %(acquire_value)s, %(source_file)s, %(index)s
        WHERE NOT EXISTS (SELECT 1 FROM property_car WHERE index = %(index)s and source_file = %(source_file)s)
    ''', dataset)

def upsert_property_aircraft(dataset):
    c.executemany('''
        UPDATE property_aircraft
        SET legislator_id = %(legislator_id)s, date = %(date)s, category = %(category)s, name = %(name)s, maker = %(maker)s, number = %(number)s, owner = %(owner)s, register_date = %(register_date)s, register_reason = %(register_reason)s, acquire_value = %(acquire_value)s
        WHERE index = %(index)s and source_file = %(source_file)s
    ''', dataset)
    c.executemany('''
        INSERT INTO property_aircraft(legislator_id, date, category, name, maker, number, owner, register_date, register_reason, acquire_value, source_file, index)
        SELECT %(legislator_id)s, %(date)s, %(category)s, %(name)s, %(maker)s, %(number)s, %(owner)s, %(register_date)s, %(register_reason)s, %(acquire_value)s, %(source_file)s, %(index)s
        WHERE NOT EXISTS (SELECT 1 FROM property_aircraft WHERE index = %(index)s and source_file = %(source_file)s)
    ''', dataset)

def upsert_property_cash(dataset):
    c.executemany('''
        UPDATE property_cash
        SET legislator_id = %(legislator_id)s, date = %(date)s, category = %(category)s, currency = %(currency)s, owner = %(owner)s, total = %(total)s
        WHERE index = %(index)s and source_file = %(source_file)s
    ''', dataset)
    c.executemany('''
        INSERT INTO property_cash(legislator_id, date, category, currency, owner, total, source_file, index)
        SELECT %(legislator_id)s, %(date)s, %(category)s, %(currency)s, %(owner)s, %(total)s, %(source_file)s, %(index)s
        WHERE NOT EXISTS (SELECT 1 FROM property_cash WHERE index = %(index)s and source_file = %(source_file)s)
    ''', dataset)

def upsert_property_deposit(dataset):
    c.executemany('''
        UPDATE property_deposit
        SET legislator_id = %(legislator_id)s, date = %(date)s, category = %(category)s, bank = %(bank)s, deposit_type = %(deposit_type)s, currency = %(currency)s, owner = %(owner)s, total = %(total)s
        WHERE index = %(index)s and source_file = %(source_file)s
    ''', dataset)
    c.executemany('''
        INSERT INTO property_deposit(legislator_id, date, category, bank, deposit_type, currency, owner, total, source_file, index)
        SELECT %(legislator_id)s, %(date)s, %(category)s, %(bank)s, %(deposit_type)s, %(currency)s, %(owner)s, %(total)s, %(source_file)s, %(index)s
        WHERE NOT EXISTS (SELECT 1 FROM property_deposit WHERE index = %(index)s and source_file = %(source_file)s)
    ''', dataset)

conn = db_ly.con()
c = conn.cursor()
files = [f for f in glob.glob('data/*.xlsx')]
categories = [u'土地', u'建物', u'船舶', u'汽車', u'航空器', u'現金', u'存款', u'有價證券', u'股票', u'債券', u'基金受益憑證', u'其他有價證券', u'具有相當價值之財產', u'保險', u'債權', u'債務', u'事業投資', u'備註', u'此致']
models = {
    u"股票": {
        "columns": ['name', 'owner', 'quantity', 'face_value', 'currency', 'total']
    },
    u"土地": {
        "columns": ['name', 'area', 'share_portion', 'owner', 'register_date', 'register_reason', 'acquire_value']
    },
    u"船舶": {
        "columns": ['name', 'tonnage', 'homeport', 'owner', 'register_date', 'register_reason', 'acquire_value']
    },
    u"汽車": {
        "columns": ['name', 'capacity', 'owner', 'register_date', 'register_reason', 'acquire_value']
    },
    u"船舶": {
        "columns": ['name', 'tonnage', 'homeport', 'owner', 'register_date', 'register_reason', 'acquire_value']
    },
    u"航空器": {
        "columns": ['name', 'maker', 'number', 'owner', 'register_date', 'register_reason', 'acquire_value']
    },
    u"現金": {
        "columns": ['currency', 'owner', 'total']
    },
    u"存款": {
        "columns": ['bank', 'deposit_type', 'currency', 'owner', 'total']
    }
}
output_file = codecs.open('./output/property.json', 'w', encoding='utf-8')
output_list = []
for f in files:
    print f
    filename = os.path.splitext(os.path.basename(f))[0]
    df_orgi = pd.read_excel(f, 0, header=None, encoding='utf-8')
    df_orgi.replace(to_replace=u'監察院公報\S*', regex=True, value=nan, inplace=True)
    bookmarks = get_table_range(df_orgi[df_orgi.columns[0]], categories)
    title = get_type(df_orgi)
    date = get_date(df_orgi)
    name = get_name(df_orgi)
    if title == u'財產申報表':
        legislator_id = ly_common.GetLegislatorId(c, name)
        writer = pd.ExcelWriter('output/normal/%s_%s_%s_%s.xlsx' % (name, date, title, filename), engine='xlsxwriter')
        for i in range(0, len(bookmarks) - 1):
            df = df_orgi[bookmarks[i]['position'] + 1 : bookmarks[i+1]['position']]
            df.dropna(inplace=True, how='any', subset=[0, 1]) # Drop rows who's column 0 or 1 are empty
            df.dropna(inplace=True, how='all', axis=1, thresh=2) # Drop columns who's non-nan cell not more than one
            if not df.empty:
                df[1:] = df[ df[0] != df.iloc[0][0] ] # Remove rows who's first column equal to index fisrt column
                df = df[1:]
                df.dropna(inplace=True, how='any', subset=[0, 1]) # Drop if column 0 or 1 empty
                df.replace(to_replace=u'[\s，,’^《•★；;、_/\'-]', value='', inplace=True, regex=True)
                if bookmarks[i]['name'].strip() == u"股票" or bookmarks[i]['name'].strip() == u"有價證券":
                    df.columns = models[u"股票"]["columns"]
                    df['property_category'] = 'stock'
                    df['category'] = 'normal'
                    df['date'] = date
                    df['legislator_name'] = name
                    df['legislator_id'] = legislator_id
                    df['source_file'] = filename
                    df['index'] = df.index
                    df['quantity'].replace(to_replace=u'[\D]', value='', inplace=True, regex=True)
                    try:
                        dict_list = json.loads(df.to_json(orient='records'))
                        upsert_property_stock(dict_list)
                    except:
                        print df
                        raise
                    conn.commit()
                    output_list.extend(dict_list)
                elif bookmarks[i]['name'].strip() == u"土地":
                    df.columns = models[u"土地"]["columns"]
                    df['property_category'] = 'land'
                    df['category'] = 'normal'
                    df['date'] = date
                    df['legislator_name'] = name
                    df['legislator_id'] = legislator_id
                    df['source_file'] = filename
                    df['index'] = df.index
                    df['portion'] = map(lambda x: get_portion(x), df['share_portion'])
                    df['area'].replace(to_replace=u'[^\d.]', value='', inplace=True, regex=True)
                    df['area'].replace(to_replace=u'^\.', value='', inplace=True, regex=True)
                    df['area'] = df['area'].astype(float)
                    df['total'] = df['area'] * df['portion']
                    try:
                        dict_list = json.loads(df.to_json(orient='records'))
                        upsert_property_land(dict_list)
                    except:
                        print df
                        raise
                    conn.commit()
                    output_list.extend(dict_list)
                elif bookmarks[i]['name'].strip() == u"建物":
                    df.columns = models[u"土地"]["columns"]
                    df['property_category'] = 'building'
                    df['category'] = 'normal'
                    df['date'] = date
                    df['legislator_name'] = name
                    df['legislator_id'] = legislator_id
                    df['source_file'] = filename
                    df['index'] = df.index
                    df['portion'] = map(lambda x: get_portion(x), df['share_portion'])
                    df['area'].replace(to_replace=u'[^\d.]', value='', inplace=True, regex=True)
                    df['area'].replace(to_replace=u'^\.', value='', inplace=True, regex=True)
                    df['area'] = df['area'].astype(float)
                    df['total'] = df['area'] * df['portion']
                    try:
                        dict_list = json.loads(df.to_json(orient='records'))
                        upsert_property_building(dict_list)
                    except:
                        print df
                        raise
                    conn.commit()
                    output_list.extend(dict_list)
                elif bookmarks[i]['name'].strip() == u"船舶":
                    df.columns = models[u"船舶"]["columns"]
                    df['property_category'] = 'boat'
                    df['category'] = 'normal'
                    df['date'] = date
                    df['legislator_name'] = name
                    df['legislator_id'] = legislator_id
                    df['source_file'] = filename
                    df['index'] = df.index
                    df['tonnage'].replace(to_replace=u'[^\d.]', value='', inplace=True, regex=True)
                    df['tonnage'].replace(to_replace=u'^\.', value='', inplace=True, regex=True)
                    df['tonnage'] = df['tonnage'].astype(float)
                    try:
                        dict_list = json.loads(df.to_json(orient='records'))
                        upsert_property_boat(dict_list)
                    except:
                        print df
                        raise
                    conn.commit()
                    output_list.extend(dict_list)
                elif bookmarks[i]['name'].strip() == u"汽車":
                    df.columns = models[u"汽車"]["columns"]
                    df['property_category'] = 'car'
                    df['category'] = 'normal'
                    df['date'] = date
                    df['legislator_name'] = name
                    df['legislator_id'] = legislator_id
                    df['source_file'] = filename
                    df['index'] = df.index
                    df['capacity'].replace(to_replace=u'[^\d.]', value='', inplace=True, regex=True)
                    df['capacity'].replace(to_replace=u'^\.', value='', inplace=True, regex=True)
                    df['capacity'] = df['capacity'].astype(float)
                    try:
                        dict_list = json.loads(df.to_json(orient='records'))
                        upsert_property_car(dict_list)
                    except:
                        print df
                        raise
                    conn.commit()
                    output_list.extend(dict_list)
                elif bookmarks[i]['name'].strip() == u"航空器":
                    df.columns = models[u"航空器"]["columns"]
                    df['property_category'] = 'aircraft'
                    df['category'] = 'normal'
                    df['date'] = date
                    df['legislator_name'] = name
                    df['legislator_id'] = legislator_id
                    df['source_file'] = filename
                    df['index'] = df.index
                    try:
                        dict_list = json.loads(df.to_json(orient='records'))
                        upsert_property_aircraft(dict_list)
                    except:
                        print df
                        raise
                    conn.commit()
                    output_list.extend(dict_list)
                elif bookmarks[i]['name'].strip() == u"現金":
                    if 2 in df.columns:
                        df.drop(2, axis=1, inplace=True)
                    df.columns = models[u"現金"]["columns"]
                    df['property_category'] = 'cash'
                    df['category'] = 'normal'
                    df['date'] = date
                    df['legislator_name'] = name
                    df['legislator_id'] = legislator_id
                    df['source_file'] = filename
                    df['index'] = df.index
                    df['total'].replace(to_replace=u'[^\d.]', value='', inplace=True, regex=True)
                    df['total'].replace(to_replace=u'^\.', value='', inplace=True, regex=True)
                    df['total'] = df['total'].astype(float)
                    try:
                        dict_list = json.loads(df.to_json(orient='records'))
                        upsert_property_cash(dict_list)
                    except:
                        print df
                        raise
                    conn.commit()
                    output_list.extend(dict_list)
                elif bookmarks[i]['name'].strip() == u"存款":
                    if 4 in df.columns:
                        df.drop(4, axis=1, inplace=True)
                    df.columns = models[u"存款"]["columns"]
                    df['property_category'] = 'deposit'
                    df['category'] = 'normal'
                    df['date'] = date
                    df['legislator_name'] = name
                    df['legislator_id'] = legislator_id
                    df['source_file'] = filename
                    df['index'] = df.index
                    df['total'].replace(to_replace=u'[^\d.]', value='', inplace=True, regex=True)
                    df['total'].replace(to_replace=u'^\.', value='', inplace=True, regex=True)
                    df['total'] = df['total'].astype(float)
                    try:
                        dict_list = json.loads(df.to_json(orient='records'))
                        upsert_property_deposit(dict_list)
                    except:
                        print df
                        raise
                    conn.commit()
                    output_list.extend(dict_list)
                else:
                    df.columns = map(lambda x: x.replace(' ', '') if isinstance(x, basestring) else x, df.iloc[0].replace(nan, ''))
                df.to_excel(writer, sheet_name=bookmarks[i]['name'])
        writer.save()
    elif title == u'變動財產申報表':
        writer = pd.ExcelWriter('output/change/%s_%s_%s_%s.xlsx' % (name, date, title, os.path.splitext(os.path.basename(f))[0]), engine='xlsxwriter')
    elif title == u'信託財產申報表':
        writer = pd.ExcelWriter('output/trust/%s_%s_%s_%s.xlsx' % (name, date, title, os.path.splitext(os.path.basename(f))[0]), engine='xlsxwriter')
    else:
        writer = pd.ExcelWriter('output/others/%s_%s_%s_%s.xlsx' % (name, date, title, os.path.splitext(os.path.basename(f))[0]), engine='xlsxwriter')
dump_data = json.dumps(output_list, sort_keys=True, indent=4, ensure_ascii=False)
output_file.write(dump_data)
output_file.close()
