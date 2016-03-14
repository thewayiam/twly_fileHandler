#! /usr/bin/env python
# -*- coding: utf-8 -*-
import re
import json
import glob
import codecs
import requests
import difflib

from common import ly_common
from common import db_settings


conn = db_settings.con()
c = conn.cursor()
url = 'http://data.ly.gov.tw/odw/openDatasetJson.action?id=19&selectTerm=all&page='

# api pages -> page.json -> laws -> bills(with lines)

#i = 0
#r = requests.get('%s%d' % (url, i))
#while r.json()['jsonList']:
#    json.dump(r.json(), codecs.open('data/laws/pages/%d.json' % i, 'w', encoding='utf-8'), indent=2, ensure_ascii=False)
#    print 'Page %d' % i
#    i += 1
#    r = requests.get('%s%d' % (url, i))

laws = {}
for f in sorted(glob.glob('data/laws/pages/*.json'), key=lambda x : int(x.split('/')[-1].rstrip('.json'))):
    page = json.load(open(f))
    for fragment in page['jsonList']:
        if not laws.get(fragment['billNo']):
            laws[fragment['billNo']] = {
                'meetings': {},
                'term': fragment['term'],
                'docUrl': fragment['docUrl'],
                'docNo': fragment['docNo'],
                'lawCompareTitle': fragment['lawCompareTitle']
            }
        meeting = fragment['selectTerm'] + fragment['sessionTimes'] + ('T%s' % fragment['meetingTimes'] if fragment['meetingTimes'] != 'null' else '')
        if not laws[fragment['billNo']]['meetings'].get(meeting):
            laws[fragment['billNo']]['meetings'][meeting] = []
        laws[fragment['billNo']]['meetings'][meeting].append(
            {x: fragment[x] for x in ['description', 'activeLaw', 'reviseLaw']}
        )
bills = []
for billNo, bill in laws.items():
    #--> check law are the same in each meetings
    meeting_0_bill = bill['meetings'][bill['meetings'].keys()[0]]
    if len(bill['meetings'].keys()) > 1:
        reviseLaws = {line['reviseLaw'] for line in meeting_0_bill}
        activeLaws = {line['activeLaw'] for line in meeting_0_bill}
        descriptions = {line['description'] for line in meeting_0_bill}
        for no in bill['meetings'].keys()[1:]:
            if reviseLaws != {line['reviseLaw'] for line in bill['meetings'][no]}:
                raise billNo + no + 'reviseLaw unmatch'
            if activeLaws != {line['activeLaw'] for line in bill['meetings'][no]}:
                raise billNo + no + 'activeLaw unmatch'
            if descriptions != {line['description'] for line in bill['meetings'][no]}:
                raise billNo + no + 'description unmatch'
    #<--
    lines = []
    for line in meeting_0_bill:
        for key in ['activeLaw', 'reviseLaw', 'description', ]:
            line[key] = line[key] or ''
        s = difflib.SequenceMatcher(None, line['activeLaw'], line['reviseLaw'])
        lines.append(
            {
                'fragment': [{'tag': tag, 'from': line['activeLaw'][i1:i2], 'to': line['reviseLaw'][j1:j2]} for tag, i1, i2, j1, j2 in s.get_opcodes()],
                'description': line['description']
            }
        )
    bill_lines = {
        'no': billNo,
        'title': bill['lawCompareTitle'],
        'ad': bill['term'],
        'links': {'doc': bill['docUrl']},
        'ref': bill['docNo'],
        'meetings': bill['meetings'].keys(),
        'lines': lines
    }
    bills.append(bill_lines)
    match = re.search(u'院總第(\d+)號委員提案第(\d+)號', bill['docNo'])
    bill_id = None
    if match:
        c.execute('''
            select uid
            from bill_bill
            WHERE data->>'提案編號' = %s
        ''', [u'%s委%s' % match.groups()])
        r = c.fetchone()
        if r:
            bill_id = r[0]
    c.execute('''
        update bill_law
        set ad = %s, data = %s, bill_id = %s
        where uid = %s
    ''', [bill_lines['ad'], bill_lines, bill_id, bill_lines['no'], ])
    c.execute('''
        INSERT into bill_law(uid, ad, data, bill_id)
        SELECT %s, %s, %s, %s
        WHERE NOT EXISTS (SELECT 1 FROM bill_law WHERE uid = %s)
    ''', [bill_lines['no'], bill_lines['ad'], bill_lines, bill_id, bill_lines['no'], ])
#json.dump(bills, codecs.open('data/laws/lines.json', 'w', encoding='utf-8'), indent=2, ensure_ascii=False)
conn.commit()
