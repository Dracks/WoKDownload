__author__ = 'dracks'
from xml.dom.minidom import parseString

import WoKService
import WoKDb

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

    ret={}

    uid=record.getElementsByTagName('UID')[0].firstChild.wholeText
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
            data['name']=author.getElementsByTagName('full_name')[0].firstChild.wholeText
            #data['first_name']=author.getElementsByTagName('first_name')[0].firstChild.wholeText
            #data['last_name']=author.getElementsByTagName('last_name')[0].firstChild.wholeText
            data['wos']=author.getElementsByTagName('wos_standard')[0].firstChild.wholeText
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
        date=e['pubdate'].split('-')
        sourceId=db.getSourceId(e['source']['abbreviation'], e['source']['title'])

        db.insertPaper(paperId, e['doi'], e['title'],sourceId,date[0],date[1])

        listAuthors=map(lambda author: db.getAuthorId(author['wos'], author['name']), e['authors'])

        listAuthorsFiltered=[]
        [listAuthorsFiltered.append(authorId) for authorId in listAuthors if not listAuthorsFiltered.__contains__(authorId)]

        db.linkAuthors(paperId, listAuthorsFiltered)
        #[db.linkAuthors(paperId, authorId) for authorId in listAuthors]

def runDownloadQuery(soap, db):

    data_query=db.getQuery()

    while data_query is not None:
        begin=data_query[2]
        end=data_query[3]
        print data_query
        response=soap.search(data_query[1], begin)
        while begin <= end:
            for element in response.getData():
                insertRow(db, element)
                db.insertPaperToDownload(element['id'], 'out')
                if int(element['cites'])>0:
                    db.insertPaperToDownload(element['id'], 'in')
                begin+=1
            db.updateQuery(data_query[0], begin, response.getNumber())
            db.commit()
            response.nextPart()

        data_query=db.getQuery()

def runDownloadInputCites(soap, db):
    """

    @param soap:
    @type soap: WoKService.WoKSoap
    @param db:
    @type db: WoKDb.MySQL
    @return:
    @rtype:
    """

    data_query=db.getPaperToDownload(cite='in')
    while data_query is not None:
        paperId=data_query[0]
        response=soap.citingArticles(paperId)

        pending=True

        while pending:
            for element in response.getData():
                insertRow(db, element)
                db.linkPapers(paperId, element['id'])

            pending=response.nextPart()

        db.updatePaperToDownload(paperId, 'in')
        db.commit()

        data_query=db.getPaperToDownload(cite='in')




def runDownloadOutputCites(soap, db):
    pass


def runDownload(create):
    soap=WoKService.WoKSoap(getData)
    db=WoKDb.MySQL('localhost', 'webofscience','climaIC3','webofscience')

    if create:
        db.createTables()

    runDownloadQuery(soap, db)
    runDownloadInputCites(soap, db)
    runDownloadOutputCites(soap, db)

    #soap.close()



if __name__=='__main__':
    #rawData=open("SampleResponses/Sample1.xml", "r").read()
    #dom=parseString(rawData.replace('\n',''))
    #map(getData, dom.getElementsByTagName('REC'))

    runDownload(False)