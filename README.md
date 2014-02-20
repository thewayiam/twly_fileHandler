twly_fileHandler
==========

File handler of [立委投票指南](http://vote.ly.g0v.tw/)     

使用方法
======
(1) Use db.dump 建立資料庫[Example Procedure](https://github.com/g0v/twly-voter-guide#restore-data-into-database)
(2) ./db_ly.py：資料庫config請自行設定		
(3) 建立立委主表
```
./legislator$ python legislator_and_committee.py：
./legislator$ python distrcit_detail.py：
```

## 法條修正草案
```
	./bill$ python crawler.py		
	./bill$ python parser.py		
```

## 立院表決紀錄，立院出缺席紀錄

```
	./vote$ python vote.py		
```

## 立委在委員會的臨時提案、附帶決議紀錄，出缺席紀錄
```
    ./proposal$ python proposal.py
```

## 政見
```
    ./platform$ python platform.py
```

[資料來源](http://vote.ly.g0v.tw/reference/)
======

CC0 1.0 Universal
=================
CC0 1.0 Universal       
This work is published from Taiwan.      
[about](http://vote.ly.g0v.tw/about/)
