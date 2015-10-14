twly_fileHandler
==========

File handler of [立委投票指南](http://vote.ly.g0v.tw/)     

使用方法
======
(1) [建立資料庫](https://github.com/g0v/twly-voter-guide#restore-data-into-database)       
(2) ./db_ly.py：資料庫config請自行設定		
(3) update git submodule
```
cd data/twly_crawler        
git pull        
```
(4) 建立立委主表
```
./data/twly_crawler$ git pull origin master
./legislator$ python legislator_and_committee.py：
```

## 法條修正草案
```
./bill$ python crawler.py		
./bill$ python parser.py		
```

## 立院表決紀錄，立院出缺席紀錄
vote_8.py 的8是立法院屆期

```
./vote$ rm minutes.json
./vote$ scrapy runspider meeting_minutes_crawler.py -o minutes.json
./vote$ python vote_8.py		
./vote$ python vote_7.py		
./vote$ python vote_6.py		
```

## 候選人和政治獻金
```
./candidates$ python candidates.py
./candidates/platform$ python political_contribution.py
```

## 政見
```
./legislator/platform$ python platform.py
```

[資料來源](http://vote.ly.g0v.tw/reference/)
======

CC0 1.0 Universal
=================
CC0 1.0 Universal       
This work is published from Taiwan.      
[about](http://vote.ly.g0v.tw/about/)
