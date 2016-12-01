#! /usr/bin/env python
# -*- coding: utf-8 -*-
import re
import json
import glob
import codecs
import requests
import difflib
import ast
from sys import argv

from common import ly_common
from common import db_settings


conn = db_settings.con()
c = conn.cursor()
url = 'http://data.ly.gov.tw/odw/openDatasetJson.action?id=19&selectTerm=all&page='

# api pages -> page.json -> laws -> bills(with lines)

try:
    latest_pages_num = ast.literal_eval(argv[1])['pages']
except:
    latest_pages_num = 0
try:
    i = int(sorted(glob.glob('data/laws/pages/*.json'), key=lambda x : int(x.split('/')[-1].rstrip('.json')), reverse=True)[latest_pages_num].split('/')[-1].rstrip('.json'))
except IndexError:
    i = 1
print 'start from page: %d' % i
r = requests.get('%s%d' % (url, i), timeout=60)
if r.status_code == 200:
    try:
        while r.json()['jsonList']:
            json.dump(r.json(), codecs.open('data/laws/pages/%d.json' % i, 'w', encoding='utf-8'), indent=2, ensure_ascii=False)
            i += 1
            r = requests.get('%s%d' % (url, i), timeout=60)
    except ValueError:
        print 'ValueError at page: %d' % i
        pass
    except Exception, e:
        print 'response content:'
        print r.content
        print 'Exception: '
        print e
else:
    print 'response status_code: ' + r.status_code

print 'law process start'
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
        meeting_0_laws = {x: {line[x] for line in meeting_0_bill} for x in ['description', 'activeLaw', 'reviseLaw']}
        for no in bill['meetings'].keys()[1:]:
            for k, v in meeting_0_laws.items():
                if v != {line[k] for line in bill['meetings'][no]}:
                    raise billNo + no + '%s unmatch' % k
    #<--
    lines = []
    for line in meeting_0_bill:
        for key in ['activeLaw', 'reviseLaw', 'description', ]:
            if line[key] is None:
                line[key] = ''
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
print 'law process end'
