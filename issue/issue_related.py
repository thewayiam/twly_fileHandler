#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys # 正常模組
sys.path.append('../')
import psycopg2
import db_ly

def Add_issue_vote(issue_id,vote_id):
    c.execute('''INSERT into legislator_issue_vote(issue_id,vote_id,weights)
        SELECT %s,%s,%s
        WHERE NOT EXISTS (SELECT issue_id,vote_id,weights FROM legislator_issue_vote WHERE issue_id = %s and vote_id = %s) RETURNING id''',(issue_id,vote_id,1,issue_id,vote_id))
def Add_issue_proposal(issue_id,proposal_id):
    c.execute('''INSERT into legislator_issue_proposal(issue_id,proposal_id,weights)
        SELECT %s,%s,%s
        WHERE NOT EXISTS (SELECT issue_id,proposal_id,weights FROM legislator_issue_proposal WHERE issue_id = %s and proposal_id = %s) RETURNING id''',(issue_id,proposal_id,1,issue_id,proposal_id))
def Add_issue_bill(issue_id,bill_id):
    c.execute('''INSERT into legislator_issue_bill(issue_id,bill_id,weights)
        SELECT %s,%s,%s
        WHERE NOT EXISTS (SELECT issue_id,bill_id,weights FROM legislator_issue_bill WHERE issue_id = %s and bill_id = %s) RETURNING id''',(issue_id,bill_id,1,issue_id,bill_id))
def Get_Bill_id(billid,proposalid):
    c.execute('''SELECT id FROM legislator_bill WHERE billid = %s and proposalid = %s''',(billid,proposalid))
    return c.rowcount,c.fetchone()
      
conn = db_ly.con()
c = conn.cursor()
l = [
     {'issue':1,
      'proposal':[6759,6760,6761,6762,6763],
      'vote':[258,],      
      'bill':[{'billid':335,'proposalid':15351},
              {'billid':335,'proposalid':15352},
              {'billid':335,'proposalid':15353},
              ]
      },
     {'issue':2,
      'proposal':[927,926,829,2916,2440],
      'bill':[{'billid':666,'proposalid':14092},
              ]      
      },
     {'issue':3,
      'proposal':[752,1084,1085,3876,5296,5309,5351,6088,6655,6656,6774,6803],
      'vote':[257,232,],      
      },      
     {'issue':11,
      'proposal':[6738,6724,6721,6720,6719,5901,5900,6712,5777,5751,6680,1660,1654,5395,5394,5391,5184,3724,3718,3717,3716,4652,4651,4649,4648,4646,4645,4644,4643,4641],
      },
     {'issue':13,
      'proposal':[5184,5212,5179,5213,5587,6678,6677],
      'bill':[{'billid':1749,'proposalid':13217},
              {'billid':1749,'proposalid':13032},
              ]      
      },
     {'issue':5,
      'vote':[236,226,83,235,222,221,87,229,240,223,244,220,216,242,215,217],
      'proposal':[3401
,4971
,5066
,5590
,4856
,3243
,4968
,4985
,3251
,4950
,3246
,3241
,4983
,6613
,5578
,6334
,2476
,4984
,4857
,2837
,3367
,4951
,4966
,6094
,5592
,5966
,478
,5736
,2589
,5597
,1033
,1079
,1601
,1475
,3118
,2468
,2471
,2606
,2473
,2477
,2478
,2580
,2582
,2583
,2598
,2593
,2599
,2600
,2623
,2835
,2787
,2834
,2833
,2836
,2838
,3233
,3116
,3117
,3232
,3230
,3231
,3234
,3237
,3239
,3240
,3244
,3245
,3248
,3250
,3252
,3255
,3399
,3400
,3477
,3646
,3694
,3682
,4963
,4964
,4965
,4967
,4969
,4970
,4988
,4981
,4982
,5076
,5067
,5068
,5069
,5070
,5071
,5072
,5073
,5074
,5075
,5077
,5078
,5964
,5962
,5965
,5968
,5969
,5970
,5971
,5972
,5973
,5974
,5975
,5985
,6333
,6442
,6612
,6675
,6679
],
      'bill':[{'billid':1767,'proposalid':13690},
              {'billid':1640,'proposalid':13067},
              {'billid':1640,'proposalid':12889},
              ]      
      },  
     ]
for e in l:
    if 'vote' in e:
        for p in e['vote']:
            Add_issue_vote(e['issue'],p)    
    if 'proposal' in e:
        for p in e['proposal']:
            Add_issue_proposal(e['issue'],p)
    if 'bill' in e:
        for b in e['bill']:
            rowcount,bill = Get_Bill_id(b['billid'],b['proposalid'])
            if rowcount == 1 and bill:
                Add_issue_bill(e['issue'],bill[0])        
            else:
                print b + ':bill repeated!!'  
conn.commit()
print 'Succeed'



