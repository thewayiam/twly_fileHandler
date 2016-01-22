# -*- coding: utf-8 -*-

# --> conscience vote
def party_Decision_List(c, party, ad):
    c.execute('''
        select vote_id, avg(decision)
        from vote_legislator_vote
        where decision is not null and legislator_id in (select id from legislator_legislatordetail where party = %s and ad = %s)
        group by vote_id
    ''', (party, ad))
    return c.fetchall()

def personal_Decision_List(c, party, vote_id, ad):
    c.execute('''
        select legislator_id, decision
        from vote_legislator_vote
        where decision is not null and vote_id = %s and legislator_id in (select id from legislator_legislatordetail where party = %s and ad = %s)
    ''', (vote_id, party, ad))
    return c.fetchall()

def party_List(c, ad):
    c.execute('''
        select distinct(party)
        from legislator_legislatordetail
        where ad = %s
    ''', (ad,))
    return c.fetchall()

def conflict_vote(c, conflict, vote_id):
    c.execute('''
        update vote_vote
        set conflict = %s
        where uid = %s
    ''', (conflict, vote_id))

def conflict_legislator_vote(c, conflict, legislator_id, vote_id):
    c.execute('''
        update vote_legislator_vote
        set conflict = %s
        where legislator_id = %s and vote_id = %s
    ''', (conflict, legislator_id, vote_id))

def conscience_vote(c, ad):
    for party in party_List(c, ad):
        if party != u'無黨籍':
            for vote_id, avg_decision in party_Decision_List(c, party, ad):
                # 黨的decision平均值如不為整數，表示該表決有人脫黨投票
                if int(avg_decision) != avg_decision:
                    conflict_vote(c, True, vote_id)
                    # 同黨各立委的decision與黨的decision平均值相乘如小於(相反票)等於(棄權票)零，表示脫黨投票
                    for legislator_id, personal_decision in personal_Decision_List(c, party, vote_id, ad):
                        if personal_decision*avg_decision <= 0:
                            conflict_legislator_vote(c, True, legislator_id, vote_id)
# <-- conscience vote

# --> not voting & vote results
def vote_list(c):
    c.execute('''
        select vote.uid, sitting.ad, sitting.date
        from vote_vote vote, sittings_sittings sitting
        where vote.sitting_id = sitting.uid
    ''')
    return c.fetchall()

def not_voting_legislator_list(c, vote_id, vote_ad, vote_date):
    c.execute('''
        select id
        from legislator_legislatordetail
        where ad = %s and term_start <= %s and cast(term_end::json->>'date' as date) > %s and id not in (select legislator_id from vote_legislator_vote where vote_id = %s)
    ''', (vote_ad, vote_date, vote_date, vote_id))
    return c.fetchall()

def insert_not_voting_record(c, legislator_id, vote_id):
    c.execute('''
        INSERT INTO vote_legislator_vote(legislator_id, vote_id)
        SELECT %s, %s
        WHERE NOT EXISTS (SELECT legislator_id, vote_id FROM vote_legislator_vote WHERE legislator_id = %s AND vote_id = %s)
    ''', (legislator_id, vote_id, legislator_id, vote_id))

def get_vote_results(c, vote_id):
    c.execute('''
        select
            count(*) total,
            sum(case when decision isnull then 1 else 0 end) not_voting,
            sum(case when decision = 1 then 1 else 0 end) agree,
            sum(case when decision = 0 then 1 else 0 end) abstain,
            sum(case when decision = -1 then 1 else 0 end) disagree
        from vote_legislator_vote
        where vote_id = %s
    ''', (vote_id,))
    return [desc[0] for desc in c.description], c.fetchone() # return column name and value

def update_vote_results(c, uid, results):
    c.execute('''
        select json_agg(row)
        from (
            select decision, json_agg(party_list) as party_list, sum(count)
            from (
                select decision, json_build_object('party', party, 'legislators', legislators, 'count', json_array_length(legislators)) as party_list, json_array_length(legislators) as count
                from (
                    select decision, party, json_agg(detail) as legislators
                    from (
                        select decision, party, json_build_object('name', name, 'legislator_id', legislator_id) as detail
                        from (
                            select
                                case
                                    when vl.decision = 1 then '贊成'
                                    when vl.decision = -1 then '反對'
                                    when vl.decision = 0 then '棄權'
                                    when vl.decision isnull then '沒投票'
                                end as decision,
                                d.name as party,
                                l.name,
                                l.legislator_id
                            from legislator_legislatordetail l, jsonb_to_recordset(l.party) d(name text, start_at date, end_at date), vote_vote v , vote_legislator_vote vl, sittings_sittings s
                            where v.uid = %s and v.uid = vl.vote_id and vl.legislator_id = l.id and v.sitting_id = s.uid and d.start_at < s.date and d.end_at > s.date
                        ) _
                    ) __
                group by decision, party
                order by decision, party
                ) ___
            ) ____
        group by decision
        order by sum desc
        ) row
    ''', [uid])
    decisions = c.fetchone()[0]
    if results['agree'] > results['disagree']:
        result = 'Passed'
    else:
        result = 'Not Passed'
    c.execute('''
        UPDATE vote_vote
        SET result = %s, results = %s
        WHERE uid = %s
    ''', (result, decisions, uid))

def not_voting_and_results(c):
    for vote_id, vote_ad, vote_date in vote_list(c):
        for legislator_id in not_voting_legislator_list(c, vote_id, vote_ad, vote_date):
            insert_not_voting_record(c, legislator_id, vote_id)
        key, value = get_vote_results(c, vote_id)
        update_vote_results(c, vote_id, dict(zip(key, value)))
# <-- not voting & vote results end

def vote_param(c):
    c.execute('''
        SELECT
            legislator_id,
            COUNT(*) total,
            SUM(CASE WHEN conflict = True THEN 1 ELSE 0 END) conflict,
            SUM(CASE WHEN decision isnull THEN 1 ELSE 0 END) not_voting,
            SUM(CASE WHEN decision = 1 THEN 1 ELSE 0 END) agree,
            SUM(CASE WHEN decision = 0 THEN 1 ELSE 0 END) abstain,
            SUM(CASE WHEN decision = -1 THEN 1 ELSE 0 END) disagree
        FROM vote_legislator_vote
        GROUP BY legislator_id
    ''')
    response = c.fetchall()
    for r in response:
        param = dict(zip(['total', 'conflict', 'not_voting', 'agree', 'abstain', 'disagree'], r[1:]))
        c.execute('''
            UPDATE legislator_legislatordetail
            SET vote_param = %s
            WHERE id = %s
        ''', (param, r[0]))

def attendance_param(c):
    c.execute('''
        SELECT
            legislator_id,
            COUNT(*) total,
            SUM(CASE WHEN category = 'YS' AND status = 'absent' THEN 1 ELSE 0 END) absent
        FROM legislator_attendance
        GROUP BY legislator_id
    ''')
    response = c.fetchall()
    for r in response:
        param = dict(zip(['total', 'absent'], r[1:]))
        c.execute('''
            UPDATE legislator_legislatordetail
            SET attendance_param = %s
            WHERE id = %s
        ''', (param, r[0]))

def sittingIdsInAd(c, ad):
    c.execute('''
        SELECT uid
        FROM sittings_sittings
        WHERE ad = %s AND name != ''
    ''', (ad, ))
    return [x[0] for x in c.fetchall()]
