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
df_vote = psql.frame_query("SELECT * FROM vote_vote order by uid desc", conn)
df_vote.to_csv('output/votes.csv', cols=['uid', 'sitting_id', 'vote_seq', 'content', 'results', 'result'], header=[u'表決ID', u'會議ID', u'該會議第幾個表決', u'表決內容', u'票數', u'結果'], encoding='utf-8')
