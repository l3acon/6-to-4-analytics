# Installation/Running

* Install depends

`pip install scrapy elasticsearch`

* Run ELK
`docker-compose up -d`

* Run scraper
`python data/spider.py`

* Set ES passwords
`docker exec -it craigslistanalytics_elasticsearch_1 /bin/bash `

`./bin/x-pack/setup-passwords interactive`
(type password a million times)


Then you have to go to `localhost:5601` and login with credentials `username: elasticsearch`, `password: supersecret` and add the kibana index: 
` Discover -> cl-*`

And then you can look at data!




