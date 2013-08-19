#! /usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2

def con():
    conn = psycopg2.connect(dbname='twly', host='localhost', user='xxx' , password='xxx')         
    return conn



