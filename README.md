WoKDownload
===========

It's a python application to download the papers and citations involved in a Web Of Knowledge query. It requires register on Web Of Knowledge and have a public 

Programs:
--

 * start.py: Its the download application
 * cli.py: Utility to show stadistics about the download process, used in a crontab.

Sample execution
--
```
 python start.py -a "AU=(Singla J)"
```
Add "AU=(Singla J)" as a query and start download papers and citations

```
 python start.py -h
```
Get help documentation about download application


