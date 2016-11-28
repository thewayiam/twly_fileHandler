#! /usr/bin/env python
# -*- coding: utf-8 -*-
import re
import uuid
import codecs
import psycopg2
from psycopg2.extras import Json
import json

from common import ly_common
from common import db_settings


def normalize_constituency(constituency):
    match = re.search(u'第(?P<num>.+)選(?:舉)?區', constituency)
    if not match:
        return 1
    try:
        return int(match.group('num'))
    except:
        print match.group('num')
    ref = {u'一': 1, u'二': 2, u'三': 3, u'四': 4, u'五': 5, u'六': 6, u'七': 7, u'八': 8, u'九': 9}
    if re.search(u'^\s*十\s*$', match.group('num')):
        return 10
    num = re.sub(u'^\s*十', u'一', match.group('num'))
    num = re.sub(u'十', '', num)
    digits = re.findall(u'(一|二|三|四|五|六|七|八|九)', num)
    total, dec = 0, 1
    for i in reversed(range(0, len(digits))):
        total = total + int(ref.get(digits[i], 0)) * dec
        dec = dec * 10
    return total

def get_or_create_uid(legislator):
    identifiers = {legislator['name'], re.sub(u'[\w‧]', '', legislator['name']), re.sub(u'\W', '', legislator['name']).lower(), } - {''}
    c.execute('''
        SELECT ld.legislator_id
        FROM legislator_legislator l, legislator_legislatordetail ld
        WHERE l.identifiers ?| array[%s]
    ''' % ','.join(["'%s'" % x for x in identifiers]) + ' and l.uid = ld.legislator_id and ld.ad = %(ad)s', legislator)
    r = c.fetchone()
    if r:
        return r[0]
    c.execute('''
        select max(uid)+1
        from legislator_legislator
    ''', legislator)
    return c.fetchone()[0]

def Legislator(legislator):
    legislator['identifiers'] = list((set(legislator['former_names']) | {legislator['name'], re.sub(u'[\w‧]', '', legislator['name']), re.sub(u'\W', '', legislator['name']).lower(), }) - {''})
    legislator['former_names'] = '\n'.join(legislator['former_names']) if legislator.has_key('former_names') else ''
    c.execute('''
        INSERT INTO legislator_legislator(uid, name, former_names, identifiers)
        VALUES (%(uid)s, %(name)s, %(former_names)s, %(identifiers)s)
        ON CONFLICT (uid)
        DO UPDATE
        SET name = %(name)s, former_names = %(former_names)s, identifiers = %(identifiers)s
        returning uid
    ''', legislator)
    return c.fetchone()[0]

def LegislatorDetail(uid, term, ideal_term_end_year):
    for key in ['education', 'experience', 'remark']:
        if term.has_key(key):
            term[key] = '\n'.join(term[key])
    term.pop('county', None)
    if term.has_key('district'):
        term['district'] = u'，'.join([x for x in term['district']])
    term['term_end'] = term.get('term_end') or {"date": '%04d-01-31' % int(ideal_term_end_year)}
    complement = {"uid": uid, "gender": '', "party": '', "caucus": '', "contacts": None, "county": term['constituency'], "constituency": 0, "district": '', "term_start": None, "education": None, "experience": None, "remark": None, "image": '', "links": None}
    match = re.search(u'(?P<county>[\W]{1,2}(縣|市))', term['constituency'])
    if match:
        complement.update({"county": re.sub(u'台', u'臺', match.group('county'))})
    if term.get('constituency'):
        term['constituency'] = normalize_constituency(term['constituency'])
    complement.update(term)
    complement['party'] = [{"name": complement['party'], "end_at": complement['term_end']['date'], "start_at": complement['term_start']}]
    c.execute('''
        INSERT into legislator_legislatordetail(legislator_id, ad, name, gender, title, party, elected_party, caucus, constituency, county, district, in_office, contacts, term_start, term_end, education, experience, remark, image, links)
        VALUES (%(uid)s, %(ad)s, %(name)s, %(gender)s, %(title)s, %(party)s, %(elected_party)s, %(caucus)s, %(constituency)s, %(county)s, %(district)s, %(in_office)s, %(contacts)s, %(term_start)s, %(term_end)s, %(education)s, %(experience)s, %(remark)s, %(image)s, %(links)s)
        ON CONFLICT (ad, legislator_id)
        DO UPDATE
        SET name = %(name)s, gender = %(gender)s, title = %(title)s, party = %(party)s, elected_party = %(elected_party)s, caucus = %(caucus)s, constituency = %(constituency)s, in_office = %(in_office)s, contacts = %(contacts)s, county = %(county)s, district = %(district)s, term_start = %(term_start)s, term_end = %(term_end)s, education = %(education)s, experience = %(experience)s, remark = %(remark)s, image = %(image)s, links = %(links)s
        returning id
    ''', complement)
    # because we only have cec candidates data after ad > 7
    if complement['ad'] > 7:
        complement['legislator_id'] = c.fetchone()[0]
        c.execute('''
            UPDATE candidates_terms
            SET legislator_id = %(legislator_id)s
            WHERE name = %(name)s AND ad = %(ad)s AND county = %(county)s AND constituency = %(constituency)s
        ''', complement)

def Committees(committees):
    c.executemany('''
        INSERT INTO committees_committees(name)
        SELECT %(name)s
        WHERE NOT EXISTS (SELECT 1 FROM committees_committees WHERE name = %(name)s)
    ''', committees)

def Legislator_Committees(legislator_id, committee):
    complement = {"legislator_id": legislator_id}
    complement.update(committee)
    c.execute('''
        INSERT INTO committees_legislator_committees(legislator_id, committee_id, ad, session, chair)
        VALUES (%(legislator_id)s, %(name)s, %(ad)s, %(session)s, %(chair)s)
        ON CONFLICT (ad, legislator_id, committee_id, session)
        DO UPDATE
        SET chair = %(chair)s
    ''', complement)

conn = db_settings.con()
c = conn.cursor()

f = codecs.open('legislator/no_committees.txt', 'w', encoding='utf-8')
dict_list = json.load(open('data/twly_crawler/data/9/merged.json'))
ideal_term_end_year = {'1': 1993, '2': 1996, '3': 1999, '4': 2002, '5': 2005, '6': 2008, '7': 2012, '8': 2016, '9': 2020}
for legislator in dict_list:
    legislator = ly_common.normalize_person(legislator)
    legislator['uid'] = get_or_create_uid(legislator)
    legislator['elected_party'] = legislator.get('elected_party', legislator['party'])
    uid = Legislator(legislator)
    legislator['uid'] = uid
    LegislatorDetail(legislator['uid'], legislator, ideal_term_end_year[str(legislator['ad'])])
    legislator_id = ly_common.GetLegislatorDetailId(c, legislator['uid'], legislator['ad'])
    if legislator.has_key('committees'):
        Committees(legislator['committees'])
        for committee in legislator['committees']:
            Legislator_Committees(legislator_id, committee)
    else:
        f.write('no committees!!, uid: %s, name: %s, ad: %s\n' % (legislator["uid"], legislator["name"], legislator["ad"]))
f.close()
conn.commit()
party_change = json.load(open('legislator/party_change.json'))
for instance in party_change:
    c.execute('''
        UPDATE legislator_legislatordetail
        SET party = %(party)s
        WHERE ad = %(ad)s AND name = %(name)s
    ''', instance)
conn.commit()

with codecs.open('merged_uid_by_ourself.json', 'w', encoding='utf-8') as outfile:
    json.dump(dict_list, outfile, ensure_ascii=False)
