#!/usr/bin/env python
#coding:UTF-8
import sys
sys.path.append('../')
import json
import requests
import psycopg2
import unicodedata

from django.core.serializers.json import DjangoJSONEncoder

import db_settings
import local_settings


def update_id(legislator):
    c.execute('''
        UPDATE legislator_legislator
        SET id = %(id)s
        WHERE uid = %(legislator_id)s
    ''', legislator)

def persons():
    c.execute('''
        SELECT l.uid as legislator_id, l.name,
        (
            SELECT json_agg(rs)
            FROM (
                SELECT image as url
                FROM legislator_legislatordetail ld
                WHERE l.uid = ld.legislator_id
                ORDER BY ld.ad DESC
            ) rs
        ) AS images
        FROM legislator_legislator l
    ''')
    return c.fetchall()

def each_terms(person):
    c.execute('''
        SELECT %(id)s as person_id, legislator_id, ad, name, gender, party, '立法委員' as role, constituency, county, district, in_office, contacts, term_start as start_date, cast(term_end::json->>'date' as date) as end_date, education, experience, remark, image
        FROM legislator_legislatordetail
        WHERE legislator_id = %(legislator_id)s
    ''', person)
    return c.fetchall()

UPDATE = False
conn = db_settings.con()
c = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
site_name = 'taiwan'
headers = {'Apikey': '%s' % local_settings.API_KEY, 'Accept': 'application/json', 'Content-Type': 'application/json'}

# Get the id of city council if exist, if not, create it
organization = '立法院'
r = requests.get('http://%s.popit.mysociety.org/api/v0.1/search/organizations?q=name:"%s"' % (site_name, organization))
if r.status_code == 200:
    if r.json()['result']:
        organization_id = r.json()['result'][0]['id']
    else:
        r = requests.post('http://%s.popit.mysociety.org/api/v0.1/organizations/' % site_name, data={'name': organization}, auth=(local_settings.EMAIL, local_settings.PASSWORD))
        organization_id = r.json()['result']['id']

# Create person if not exist, else update it
for person in persons():
    r = requests.get('http://%s.popit.mysociety.org/api/v0.1/search/persons?q=name:"%s"' % (site_name, person['name']))
    if r.status_code == 200:
        for match in r.json()['result']:
            if match['name'].encode('utf8') == person['name']:
                person['id'] = match['id']
                break
#               if match.get('legislator_id') is None or int(match.get('legislator_id')) == person['legislator_id']:
#                   update_id(person)
#                   conn.commit()
    print person
    data = json.dumps(person)
    if not person.get('id'): # insert
        r = requests.post('http://%s.popit.mysociety.org/api/v0.1/persons/' % site_name, headers=headers, data=data)
    elif UPDATE: # update
        r = requests.put('http://%s.popit.mysociety.org/api/v0.1/persons/%s' % (site_name, person['id']), headers=headers, data=data)

    # Create the memberships if not exist, else update it
    for term in each_terms(person):
        if term['end_date']:
            print term
            r = requests.get('http://%s.popit.mysociety.org/api/v0.1/search/memberships?q=person_id:"%s" AND organization_id:"%s" AND end_date:"%s"' % (site_name, term['person_id'], organization_id, term['end_date']))
            term['organization_id'] = organization_id
            if r.status_code == 200:
                data = json.dumps(term, cls=DjangoJSONEncoder)
                if not r.json()['result']: # insert
                    r = requests.post('http://%s.popit.mysociety.org/api/v0.1/memberships/' % site_name, headers=headers, data=data)
                    print r.text
                elif UPDATE: # update
                    r = requests.put('http://%s.popit.mysociety.org/api/v0.1/memberships/%s' % (site_name, r.json()['result'][0]['id']), data=data)
                    print r.text
