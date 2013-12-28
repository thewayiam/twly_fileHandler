twly_fileHandler
==========

File handler of http://twly.herokuapp.com/

使用方法
======
(1)./db.dump：建立資料庫	
(2)./db_ly.py：資料庫帳密位址請設定		
(3)建立立委主表
```
    ./legislator$ python legislator_and_committee.py：
    ./legislator$ python distrcit_detail.py：
```

	建立主表後：		
    ●法條修正草案		

```
	./bill$ python crawler.py		
	./bill$ python parser.py		
```

    ●立院表決紀錄，立院出缺席紀錄		

```
	./vote$ python vote.py		
```

	●立委在委員會的臨時提案、附帶決議紀錄		

```
    ./proposal$ python proposal.py
```

	●政見		

```
    ./platform$ python platform.py
```

資料來源
======
http://twly.herokuapp.com/reference/

CC0 1.0 Universal
=================
CC0 1.0 Universal       
This work is published from Taiwan.      
http://twly.herokuapp.com/about/
