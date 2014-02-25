import optparse

__author__ = 'dracks'
#from xml.dom.minidom import parseString

import WoKService
import WoKDb
import time
import suds
import pprint
import urllib2
import config


import sys
import traceback

def getData(record):
    """

    @param record:
    @type record: xml.dom.minidom.Element
    @return:
    @rtype: dict
    """

    summary=record.getElementsByTagName('summary')[0]
    dynamic_data=record.getElementsByTagName('dynamic_data')[0]
    citation_related=dynamic_data.getElementsByTagName('citation_related')[0]
    tc_list=citation_related.getElementsByTagName('tc_list')
    assert(len(tc_list)==1)
    tc_list=tc_list[0]

    ret={'title':'Not title specified, sorry'}

    uid=record.getElementsByTagName('UID')[0].firstChild.wholeText
    #print uid
    ret['database']=uid.split(':')[0]
    ret['id']=uid.split(':')[1]

    list_title=summary.getElementsByTagName('titles')[0].getElementsByTagName('title')
    source={'title':None, 'abbreviation':None}
    for title in list_title:
        type=title.getAttribute('type')
        if type=='source':
            source['title']=title.firstChild.wholeText
        elif type=='abbrev_iso':
            source['abbreviation']=title.firstChild.wholeText
        elif type=='item':
            ret['title']=title.firstChild.wholeText

    ret['source']=source

    authors=[]
    listAuthors=summary.getElementsByTagName('names')[0].getElementsByTagName('name')
    for author in listAuthors:
        if author.getAttribute('role')=='author':
            data={}
            full_name=author.getElementsByTagName('full_name')[0].firstChild;
            if full_name is not None:
                data['name']=full_name.wholeText
            else:
                data['name']=''
            #data['first_name']=author.getElementsByTagName('first_name')[0].firstChild.wholeText
            #data['last_name']=author.getElementsByTagName('last_name')[0].firstChild.wholeText
            wos_standard=author.getElementsByTagName('wos_standard')[0].firstChild
            if wos_standard is not None:
                data['wos']=wos_standard.wholeText
                if data['name']=='':
                    data['name']=data['wos']
            else:
                data['wos']=''
            assert(data['name']!='')
            authors.append(data)

    ret['authors']=authors

    ret['pubdate']=summary.getElementsByTagName('pub_info')[0].getAttribute('sortdate')

    silo_tc_list=tc_list.getElementsByTagName('silo_tc')
    assert(len(silo_tc_list)==1)
    ret['cites']=silo_tc_list[0].getAttribute('local_count')

    ret['doi']=None
    for identifier in dynamic_data.getElementsByTagName('identifiers')[0].getElementsByTagName('identifier'):
        if identifier.getAttribute('type').__contains__('doi'):
            ret['doi']=identifier.getAttribute('value')
            break


    #print ret
    return ret

def insertRow(db, e):
    """

    @param db: The database connection to insert
    @type db: WoKDb.MySQL
    @param e:
    @type e: dict
    @return:
    @rtype: None
    """
    paperId=e['id']
    ret=db.getPaper(paperId, ['id'])
    if ret is None:
        #print ret
        date=e['pubdate'].split('-')
        sourceId=db.getSourceId(e['source']['abbreviation'], e['source']['title'])

        db.insertPaper(paperId, e['doi'], e['title'],sourceId,date[0],date[1])

        listAuthors=map(lambda author: db.getAuthorId(author['wos'], author['name']), e['authors'])

        listAuthorsFiltered=[]
        [listAuthorsFiltered.append(authorId) for authorId in listAuthors if not listAuthorsFiltered.__contains__(authorId)]

        db.linkAuthors(paperId, listAuthorsFiltered)
        #[db.linkAuthors(paperId, authorId) for authorId in listAuthors]

def runDownloadQuery(soap, db):
    """

    @param soap: Soap Instance to call API soaps
    @type soap: WoKService.WoKSoap
    @param db: MySQL Instance to call and use database
    @type db: WoKDb.MySQL
    """

    data_query=db.getQuery()

    while data_query is not None:
        begin=data_query[2]
        end=data_query[3]
        print data_query
        response=soap.search(data_query[1], begin)
        #print response.toString()
        while begin <= end:
            #print "inside while"
            #pprint.pprint(response)
            for element in response.getData():
                insertRow(db, element)
                #print element['cites']
                if db.getPaperToDownload(element['id'], 'out') is None:
                    db.insertPaperToDownload(element['id'], 'out')

                if int(element['cites'])>0:
                    #print "inside 1 ", element['id']
                    if db.getPaperToDownload(element['id'], 'in') is None:
                        #print "inside 2"
                        db.insertPaperToDownload(element['id'], 'in')
                begin+=1
            db.updateQuery(data_query[0], begin, response.getNumber())
            db.commit()
            response.nextPart()

        data_query=db.getQuery()

def runDownloadInputCites(soap, db):
    """
    Get input cites from papers downloaded in query step

    @param soap: Soap Instance to call API soaps
    @type soap: WoKService.WoKSoap
    @param db: MySQL Instance to call and use database
    @type db: WoKDb.MySQL
    """
    data_query=db.getPaperToDownload(cite='in')
    while data_query is not None:
        paperId=data_query[0]
        print "in", paperId
        listInserted=[]
        response=soap.citingArticles(paperId)

        t0=time.time()

        pending=True

        print response.getNumber()

        while pending:
            #print "tractant"
            for element in response.getData():
                elementId=element['id']
                #print listInserted
                #print elementId
                if not listInserted.__contains__(elementId):
                    listInserted.append(elementId)
                    insertRow(db, element)
                    #pprint.pprint(element)
                    #print element['database'], element['id']
                    #q=db.getLinkPaper(paperId, element['id'])
		    #if q is not None:
                    db.linkPapers(elementId, paperId)

            #print "Next part"

            pending=response.nextPart()

        db.updatePaperToDownload(paperId, 'in')
        db.commit()

        data_query=db.getPaperToDownload(cite='in')
        elapsed=time.time()-t0
        if elapsed<1:
            time.sleep(1-elapsed)


def runDownloadOutputCites(soap, db):
    """
    Get output citations from papers downloaded in query step

    @param soap: Soap Instance to call API soaps
    @type soap: WoKService.WoKSoap
    @param db: MySQL Instance to call and use database
    @type db: WoKDb.MySQL
    """
    assert(False, "Not implemented")


def runDownload(db, s1, s2, s3):
    """
    @type db: WoKDb.MySQL
    """
    soap=WoKService.WoKSoap(getData)


    repeat=True
    lastUrlError=time.time()
    while repeat:
        try:
            if s1:
                print "Download query"
                runDownloadQuery(soap, db)
            if s2:
                print "Download input cites"
                runDownloadInputCites(soap, db)
            if s3:
                print "Download output cites"
                runDownloadOutputCites(soap, db)
            repeat=False
        except suds.WebFault, error:
            db.rollback()
            if hasattr(error.fault, 'faultstring'):
                faultstring=error.fault.faultstring
                print faultstring
                if faultstring.__contains__("(ISE0002)"):
                    soap.resetSession()
                elif faultstring.__contains__("(SEE0003)"):
                    soap.resetSession()
                elif faultstring.__contains__("(WSE0002)"):
                    time.sleep(30)
                elif faultstring.__contains__("(WSE0024)"):
                    time.sleep(30)
                elif faultstring.__contains__("Session not found: SID="):
                    soap.resetSession()
                else:
                    raise error
            else:
                raise error
        except  urllib2.URLError, error:
            db.rollback()
            now=time.time()
            if (now-lastUrlError)<310.0:
                raise error
            print "urllib2 Error"
            time.sleep(60)


    #soap.close()



if __name__=='__main__':
    db=WoKDb.MySQL(config.host, config.user, config.pswd, config.db)

    usage="Usage: %prog [options]"
    description="Download papers and cites from Web of Knowledge API database, using Webservice v3.0"
    parser = optparse.OptionParser(usage, description=description)

    parser.add_option("-c", "--create", action="store_true", dest="create", default=False, help="Create database tables")

    parser.add_option("-a", "--add", action="store", dest="add", default=None, help="Add Wok query to database")

    parser.add_option("-s", "--skip", action="store_true", dest="skip", default=False, help="Skip download process")

    parser.add_option("-q", "--download-query", action="store_true", dest="download_query", default=False, help="Download query papers only")

    parser.add_option("-i", "--download-input", action="store_true", dest="download_input", default=False, help="Download input citation papers only")

    parser.add_option("-o", "--download-output", action="store_true", dest="download_output", default=False, help="Download output citation papers only")

    (options, args) = parser.parse_args()

    if options.create:
        db.createTables()

    if options.add is not None:
        db.addQuery(options.add)

    if not options.skip:

        if not (options.download_query or options.download_input or options.download_output):
            options.download_query=True
            options.download_input=True
            options.download_output=True

        try:
            runDownload(db, options.download_query, options.download_input, options.download_output)
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            ErrorList=traceback.format_exception(exc_type, exc_value,exc_traceback)
            print "".join(ErrorList)
