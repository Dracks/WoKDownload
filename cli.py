__author__ = 'dracks'
import MySQLdb
import sys
import optparse
import config

def getPapersId(cursor):
    cursor.execute("select id from listPapers where year=2011")
    results=cursor.fetchall()
    return map(lambda e: e[0], results)

def showJournalStatistics(cursor , titleSource):
    cursor.execute("Select id from listSources where title= %s limit 1", titleSource)

    titleId=cursor.fetchone()[0]

    print "Items"

    cursor.execute("select count(*), year from listPapers where source=%s group by year order by year desc limit 10", (titleId ,) )

    results = cursor.fetchall()

    for row in results:
        print row[1], row[0]


    #cursor.execute("select count(*), p2.year from listPapers as p1, paperPaper_aso as aso where p1.id = aso.srcId and aso.dstId in (select id from listPapers as p2 where source = %s ) group by p2.year order by p2.year desc limit 10")

    print "Cites"

    papersTen=getPapersId(cursor)
    print len(papersTen)
    cursor.execute("select count(*), year from listPapers as p1, paperPaper_aso as aso where p1.id=aso.srcId and p1.source=%s and aso.dstId in ('%s') group by year order by year desc limit 10" % ("%s","','".join(papersTen)), (titleId, ));

    #cursor.execute("select count(*), year from ")
    results = cursor.fetchall()

    for row in results:
        print row[1], row[0]


def showFirstStepDownload(cursor):
    cursor.execute("select * from queryList")
    results = cursor.fetchall()
    for row in results:
        print "query:"+row[1]+"; progress:"+row[2]+"/"+row[3]

def showSecondStepDownload(cursor):
    cursor.execute("select count(*),cite, download, error, now() from papersCitesDownload group by cite, download, error")

    results = cursor.fetchall()
    for row in results:
        print "\t"+row[1]+":\t"+"count:"+str(row[0])+" - "+str(row[2])+" / "+str(row[3])


if __name__=='__main__':
    db=MySQLdb.Connect(config.host, config.user, config.pswd, config.db)
    cursor=db.cursor()

    usage="Usage: cli.py [options]"
    parser = optparse.OptionParser(usage)
    parser.add_option("-j", "--journal", action="store", dest="journalTitle", default=None,
                      help="Show journal statistics")

    parser.add_option("-1", "--control_first", action="store_true", dest="first", default=False,
                      help="Control the first step of the download")

    parser.add_option("-2", "--control_second", action="store_true", dest="second", default=False,
                      help="Control the second step of the download")

    (options, args) = parser.parse_args()

    if len(args) > 1:
        parser.error("wrong number of arguments")

    if options.journalTitle is not None:
        showJournalStatistics(cursor, options.journalTitle)

    else:
        if options.first:
            showFirstStepDownload(cursor, )
        if options.second:
            showSecondStepDownload(cursor, )





