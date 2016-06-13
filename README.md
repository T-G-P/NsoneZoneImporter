README
======
This directory contains the asynchronous version of the zone data importer. In the 'old' directory
you will find a simple synchronous implementation as well as an asynchronous implementation. The
difference between that async implementation and this one is that the script is broken into modules
and is object oriented.



##Usage: Help
Possible arguments -f, --file -a, --apikey, -d, --delete
```
python run.py -h

```

## Usage: Importing data from a properly formatted csv
```
python run.py -f ZoneData.csv -a YmZB3gnt2MxolyCCKMOR

```

## Usage: Deleting Zone data for convenience
```
python run.py -f ZoneData.csv -a YmZB3gnt2MxolyCCKMOR -d

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

[logo]: ./static/run.gif
