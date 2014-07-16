#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../')
from pandas import *
from numpy import nan
import pandas.io.sql as psql
import pandas as pd
import numpy as np
import db_ly


conn = db_ly.con()
#df_vote = psql.frame_query("SELECT * FROM vote_vote order by uid desc", conn)
df_detail = psql.frame_query('''
    select
        vote_vote.uid as vote_id,
        sittings_sittings.date as vote_date,
        legislator_legislatordetail.legislator_id as mp_id,
        legislator_legislatordetail.name as mp_name,
        legislator_legislatordetail.party as mp_party,
        case vote_legislator_vote.decision
        when 1 then 'agree'
        when 0 then 'abstain'
        when -1 then 'disagree'
        else 'not_voting'
        end
        as mp_vote
    from vote_legislator_vote
    inner join vote_vote
        on vote_vote.uid = vote_legislator_vote.vote_id
    inner join legislator_legislatordetail
        on legislator_legislatordetail.id = vote_legislator_vote.legislator_id
    inner join sittings_sittings
        on sittings_sittings.uid = vote_vote.sitting_id
    order by vote_id
''', conn)
df_vote = psql.frame_query('''
    select
        vote_vote.uid as vote_id,
        vote_vote.content as vote_content
    from vote_vote
    order by vote_id
''', conn)
#df_vote.to_csv('output/votes.csv', cols=['uid', 'sitting_id', 'vote_seq', 'content', 'results', 'result'], header=[u'表決ID', u'會議ID', u'該會議第幾個表決', u'表決內容', u'票數', u'結果'], encoding='utf-8')
df_detail.to_csv('output/votes_detail.csv', encoding='utf-8')
df_vote.to_csv('output/votes.csv', encoding='utf-8')
