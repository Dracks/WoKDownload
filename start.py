__author__ = 'dracks'
from xml.dom.minidom import parseString

def getData(record):
    """

    @param record:
    @type record: xml.dom.minidom.Element
    @return:
    @rtype: dict
    """

    summary=record.getElementsByTagName('summary')[0]
    dynamic_data=record.getElementsByTagName('dynamic_data')[0]

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

    ret['doi']=None
    for identifier in dynamic_data.getElementsByTagName('identifiers')[0].getElementsByTagName('identifier'):
        if identifier.getAttribute('type').__contains__('doi'):
            ret['doi']=identifier.getAttribute('value')
            break;


    print ret
    return ret

if __name__=='__main__':
    rawData=open("SampleResponses/Sample1.xml", "r").read()
    dom=parseString(rawData.replace('\n',''))
    map(getData, dom.getElementsByTagName('REC'))