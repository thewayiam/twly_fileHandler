#!/usr/bin/env python
#coding:UTF-8
import sys
sys.path.append('../')
import psycopg2
import unicodedata
import requests
import db_ly
import local_settings


def update_id(legislator):
    c.execute('''
        UPDATE legislator_legislator
        SET id = %(id)s
        WHERE uid = %(legislator_id)s
    ''', legislator)

def persons():
    c.execute('''
        SELECT id, uid as legislator_id, name
        FROM legislator_legislator
    ''')
    return c.fetchall()

def each_terms():
    c.execute('''
        SELECT a.id as person_id, b.legislator_id, b.ad, b.name, b.gender, b.party, '立法委員' as role, b.constituency, b.county, b.district, b.in_office, b.contacts, b.term_start as start_date, cast(b.term_end::json->>'date' as date) as end_date, b.education, b.experience, b.remark, b.image
        FROM legislator_legislator a, legislator_legislatordetail b
        WHERE a.uid = b.legislator_id
    ''')
    return c.fetchall()

conn = db_ly.con()
c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
site_name = 'taiwan'

# Get the id of city council if exist, if not, create it
organization = '立法院'
r = requests.get('http://%s.popit.mysociety.org/api/v0.1/search/organizations?q=name:"%s"' % (site_name, organization))
print r.text
if r.status_code == 200:
    if r.json()['result']:
        organization_id = r.json()['result'][0]['id']
    else:
        r = requests.post('http://%s.popit.mysociety.org/api/v0.1/organizations/' % site_name, data={'name': organization}, auth=(local_settings.EMAIL, local_settings.PASSWORD))
        organization_id = r.json()['result']['id']
        print r.text

# Create person if not exist
for person in persons():
    r = requests.get('http://%s.popit.mysociety.org/api/v0.1/search/persons?q=name:"%s"' % (site_name, person['name']))
    if r.status_code == 200:
        for match in r.json()['result']:
            if match['name'].encode('utf8') == person['name']:
                print 'in\n\n'
                person['id'] = match['id']
                if match.get('legislator_id') is None or int(match.get('legislator_id')) == person['legislator_id']:
                    update_id(person)
                    conn.commit()
    r = requests.get('http://%s.popit.mysociety.org/api/v0.1/search/persons?q=id:"%s"' % (site_name, person['id']))
    print r.text
    if r.status_code == 200:
        if not r.json()['result']:
            r = requests.post('http://%s.popit.mysociety.org/api/v0.1/persons/' % site_name, data=person, auth=(local_settings.EMAIL, local_settings.PASSWORD))
            print r.text
#       else:
#           r = requests.put('http://%s.popit.mysociety.org/api/v0.1/persons/%s' % (site_name, r.json()['result'][0]['id']), data=person, auth=(local_settings.EMAIL, local_settings.PASSWORD))
#           print r.text

# Create the memberships if not exist, else update it
for term in each_terms():
    if term['end_date']:
        print term
        r = requests.get('http://%s.popit.mysociety.org/api/v0.1/search/memberships?q=person_id:"%s" AND organization_id:"%s" AND end_date:"%s"' % (site_name, term['person_id'], organization_id, term['end_date']))
        term['organization_id'] = organization_id
        print r.text
        if r.status_code == 200:
            if not r.json()['result']:
                r = requests.post('http://%s.popit.mysociety.org/api/v0.1/memberships/' % site_name, data=term, auth=(local_settings.EMAIL, local_settings.PASSWORD))
                print r.text
#           else:
#               r = requests.put('http://%s.popit.mysociety.org/api/v0.1/memberships/%s' % (site_name, r.json()['result'][0]['id']), data=term, auth=(local_settings.EMAIL, local_settings.PASSWORD))
#               print r.text
