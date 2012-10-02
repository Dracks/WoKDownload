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
    citation_related=dynamic_data.getElementsByTagName('citation_related')
    tc_list=citation_related.getElementsByTagName('tc_list')

    ret={}

    uid=record.getElementsByTagName('UID')[0].firstChild.wholeText
    ret['database']=uid.split(':')[0]
    ret['id']=uid.split(':')[1]

    list_title=summary.getElementsByTagName('titles')[0].getElementsByTagName('title')
    source={}
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


    print ret
    return ret

def insertRow(db, e):
    """

    @param db: The database connection to insert
    @type db: WoKService.WoKSoap
    @param e:
    @type e: dict
    @return:
    @rtype: None
    """


    pass

def runDownloadQuery(soap, db):

    data_query=db.getQuery()

    while data_query is not None:
        begin=data_query[2]
        end=data_query[3]
        response=soap.search(data_query[1], begin)
        while begin < end:
            for element in response.getData():
                insertRow(db, element)
                db.insertPaperToDownload(element['id'], 'out')
                if int(element['cites'])>0:
                    db.insertPaperToDownload(element['id'], 'in')
                begin+=1
            db.updateQuery(data_query[1], begin, response.getNumber())
            db.commit()
            response.nextPart()

def runDownloadInputCites(soap, db):
    pass

def runDownloadOutputCites(soap, db):
    pass


def runDownload(create):
    soap=WoKService.WoKSoap(getData)
    db=WoKDb.MySQL('localhost', 'webofscience','pwd','isi_1')

    if create:
        db.createTables()

    runDownloadQuery(soap, db)
    runDownloadInputCites(soap, db)
    runDownloadOutputCites(soap, db)



if __name__=='__main__':
    #rawData=open("SampleResponses/Sample1.xml", "r").read()
    #dom=parseString(rawData.replace('\n',''))
    #map(getData, dom.getElementsByTagName('REC'))



