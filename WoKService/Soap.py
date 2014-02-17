__author__ = 'dracks'

import suds
import cookielib
import os
import Debug
#import pprint

from xml.dom.minidom import parseString

class WoKSoap:
    """
        Map and manage the basic soap functions
    """
    QUERY_DICT={'databaseId':'WOS','userQuery':None, 'queryLanguage':'en'}
    RETRIEVE_DICT={'firstRecord':None, 'count':10}

    class Session:
        def __init__(self):
            try:
                self.sid=open("sessionId","r").read()
                self.opened=True
            except:
                self.opened=False
                self.sid=None

        def setSession(self, sid):
            if not self.opened:
                self.sid=sid
                self.opened=True
                open("sessionId", "w").write(sid)
            else:
                raise Exception("session already Opened")

        def closeSession(self):
            if self.opened:
                os.remove("sessionId")
                self.opened=False
                self.sid=None

        def getCookieSession(self):
            if self.opened:
                return cookielib.Cookie(
                    version=0,
                    name="SID",
                    value=self.sid,
                    port=None,
                    port_specified=False,
                    domain="search.webofknowledge.com",
                    domain_specified=True,
                    domain_initial_dot=False,
                    path="/esti/wokmws/ws",
                    path_specified=True,
                    secure=False,
                    expires=None,
                    discard=False,
                    comment=None,
                    comment_url=None,
                    rest=None
                )

    class WoKResponse:
        """
        The query Response are packated inside
        """
        def __init__(self, client, mapFunction, begin, response):
            self.client=client
            self.mapFunction=mapFunction

            self.dataResponse=[]
            self.nextPos=begin
            self.queryId=response.queryId
            self.number=response.recordsFound

        def __chargeResponse(self, response):
            #open("DebugResponse.xml", "w").write(response.records)
            #Debug.debugWrite(response.records.replace('>','>\n'))
            #print response.records
            #dom=parseString(response.records.replace('\n','').replace(u'\u674e', '').replace(u'\u6db5',''))
            dom=parseString(response.records.replace('\n','').encode('ascii', 'xmlcharrefreplace'))

            self.dataResponse=map(self.mapFunction,dom.getElementsByTagName('REC'))
            self.nextPos+=len(self.dataResponse)
            #print len(self.dataResponse)

        def getData(self):
            return self.dataResponse

        def getNumber(self):
            return self.number

        def getPos(self):
            return self.nextPos

        def nextPart(self):
            """
            Download the next 100 items for the query
            """
            #print "Next pos", self.nextPos, "Number:", self.number
            if self.nextPos<=self.number:
                self.dataResponse=None
                soap_retrieve=WoKSoap.RETRIEVE_DICT.copy()
                soap_retrieve['firstRecord']=self.nextPos
                #print self.queryId, self.nextPos, self.number
                response=self.client.service.retrieve(self.queryId, soap_retrieve)
                #pprint(response)
                self.__chargeResponse(response)
                return True
            else:
                return False

        def reset(self):
            self.pos=0

        #def toString(self):
        #    return "nextPos:"+self.nextPos+"\nnumber:"+self.number+"\ncountDownloaded:"+len(self.dataResponse)

    def __init__(self, mapFunction):
        self.mapFunction=mapFunction
        self.authorize=suds.client.Client('http://search.webofknowledge.com/esti/wokmws/ws/WOKMWSAuthenticate?wsdl')
        self.query=suds.client.Client("http://search.webofknowledge.com/esti/wokmws/ws/WokSearch?wsdl", timeout=300)
        self.session=WoKSoap.Session()
        if not self.session.opened:
            self.__authorize()
        else:
            self.authorize.options.transport.cookiejar.set_cookie(self.session.getCookieSession())
        self.query.options.transport.cookiejar.set_cookie(self.session.getCookieSession())

    def __authorize(self):
        sid=self.authorize.service.authenticate()
        self.session.setSession(sid)

    def resetSession(self):
        self.close()
        self.__authorize()
        self.query.options.transport.cookiejar.set_cookie(self.session.getCookieSession())

    def close(self):
        try:
            self.authorize.service.closeSession()
        except suds.WebFault, error:
            if hasattr(error.fault, 'faultstring'):
                faultstring=error.fault.faultstring
                if not faultstring.__contains__("Session not found: SID=") and not faultstring.__contains__("No matches returned for SessionID"):
                    raise error
        self.session.closeSession()


    def search(self, query, begin=1):
        """
            Call to the search api and parse XML
            @rtype WoKSoap.WoKResponse
        """
        soap_query=WoKSoap.QUERY_DICT.copy()
        soap_retrieve=WoKSoap.RETRIEVE_DICT.copy()

        soap_query['userQuery']=query
        soap_retrieve['firstRecord']=begin

        #pprint.pprint(soap_query)
        #pprint.pprint(soap_retrieve)

        r=self.query.service.search(soap_query, soap_retrieve)

        return WoKSoap.WoKResponse(self.query, self.mapFunction, begin, r)

    def citingArticles(self, idArticle, begin=1):
        """
        Get the citing Articles from the soap
        @param id: Paper Identifier
        @type id: string
        @param begin: Number to begin the download
        @type begin: int
        @return:
        @rtype: WoKSoap.WoKResponse
        """
        soap_retrieve=WoKSoap.RETRIEVE_DICT.copy()
        soap_retrieve['firstRecord']=begin

        r=self.query.service.citingArticles('WOS', idArticle, [], None,  'en', soap_retrieve)

        return WoKSoap.WoKResponse(self.query, self.mapFunction, begin, r)



