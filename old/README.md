README
======
This directory contains both synchronous and asynchronous implementations of the zone data importer.



##Usage: Help
```
python nsone_async_importer.py -h

```

## Usage: Importing data from a properly formatted csv
```
python nsone_async_importer.py -f ZoneData.csv -a YmZB3gnt2MxolyCCKMOR

```

## Usage: Deleting Zone data for convenience
```
python nsone_async_importer.py -f ZoneData.csv -a YmZB3gnt2MxolyCCKMOR -d

```
###The following examples are run on this csv file
```
Name,Zone,Type,TTL,Data
@,example.test,A,300,1.2.3.4
@,example.test,MX,86400,5 mail1.mailer.com
@,example.test,MX,86400,10 mail2.mailer.com
www,example.test,CNAME,60,example.test
ftp,example.test,A,3600,5.6.7.8
@,example.test,TXT,86400,v=spf1 mx a -all
db,example.test,A,60,7.8.9.10
web1,example.test,A,60,7.8.9.11
@,other.test,A,3600,5.5.5.5
www,other.test,A,1800,5.5.5.5
www,other.test,A,1800,6.6.6.6
www,other.test,A,1800,7.7.7.7
db,other.test,CNAME,60,www
```

#Async execution demo

![alt text][logo]

[logo]: ./ns1_async.gif


#sync execution demo

![alt text][logo]

[logo]: ./ns1_sync.gif

