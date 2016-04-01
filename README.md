twly_fileHandler
==========

File handler of [立委投票指南](http://vote.ly.g0v.tw/)     

需求
======
postgresql-9.5

使用方法
======
(1) [建立資料庫](https://github.com/g0v/twly-voter-guide#restore-data-into-database)       
(2) common/db_settings.py：資料庫 config 請自行設定		
(3) update git submodule
```
$ git submodule init
$ git submodule update
```
(4) 建立/更新立委資料
```
$ python -m legislator.legislator
```

## 法條修正草案
Pass ad(屆期) to crawler, if output file already exist please remove it first manually, ad=9 for example:
```
bill/crawler$ rm -f bills_9.json	
bill/crawler$ scrapy crawl lis_by_ad -a ad=9 -o bills_9.json -t json	
$ python -m bill.parser_lis	'{"ad": 9}'	
$ python -m bill.law
```

## 立院表決紀錄，立院出缺席紀錄
vote_9 的9是立法院屆期

```
vote$ rm minutes.json
vote$ scrapy runspider meeting_minutes_crawler.py -o minutes.json
$ python -m vote.vote_9
$ python -m vote.vote_8
$ python -m vote.vote_7
$ python -m vote.vote_6
```

## 候選人和政治獻金
candidates_8 的8是立法院屆期
```
$ python -m candidates.candidates_8
$ python -m candidates.cec_api
$ python -m candidates.candidates_cross_with_councilor
candidates/political_contribution$ python political_contribution.py
```

## 政見
only for ad=8, ad>8 use cec api for platform data.
```
legislator/platform$ python platform.py
```

[資料來源](http://vote.ly.g0v.tw/reference/)
======

CC0 1.0 Universal
=================
CC0 1.0 Universal       
This work is published from Taiwan.      
[about](http://vote.ly.g0v.tw/about/)
