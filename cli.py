__author__ = 'dracks'
import MySQLdb
import sys

def getPapersId(cursor):
    cursor.execute("select id from listPapers where year=2011")
    results=cursor.fetchall()
    return map(lambda e: e[0], results)

if __name__=='__main__':
    db=MySQLdb.Connect("localhost", "webofscience", "climaIC3", "webofscience")
    cursor=db.cursor()

    titleSource=sys.argv[1]

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

