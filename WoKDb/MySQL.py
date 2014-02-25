'''
Created on 17/02/2012

@author: eickot
'''
import MySQLdb

class MySQL(object):
    '''
    classdocs
    '''


    def __init__(self,host, user, pswd, database='isi'):
        '''
        Constructor
        @param host: name of the host
        @param user: user to connect to MySQL
        @param pswd: password to connecto to MySQL   
        '''
        self.db=MySQLdb.Connect(host, user, pswd, database)
        #self.db.begin();
        self.cursor=self.db.cursor()


    def createTables(self):
        '''
            Create tables for insert the data;
        '''
        try:
            self.cursor.execute("Create table listSources (id int(9) primary key auto_increment, title text, abr char(255)) ENGINE=InnoDB;")

            self.cursor.execute("""Create table listPapers 
                                (id char(20) primary key,
                                 doi char(255), 
                                 title TINYTEXT,
                                 source int(9),
                                 year int(4),
                                 month int(2),
                                 foreign key (source) references listSources(id))
                                 ENGINE=InnoDB;""")

            #self.cursor.execute("Create table listKeywords (id int(9) primary key auto_increment, name char(255)) ENGINE=InnoDB");

            #self.cursor.execute("Create table listCountries (id int(9) primary key auto_increment, name char(255))ENGINE=InnoDB");

            self.cursor.execute("Create table listAuthors (id int(9) primary key auto_increment, abr char(255), name TINYTEXT, key (abr))ENGINE=InnoDB;")

            self.cursor.execute("Create table paperPaper_aso (srcId char(20), dstId char(20), primary key(srcId, dstId), constraint foreign key (srcId) references listPapers(id)  on update cascade on delete restrict, constraint foreign key (dstId) references listPapers(id)  on update cascade on delete restrict)ENGINE=InnoDB;")

            self.cursor.execute("Create table paperAuthors_aso (paperId char(20), authorId int(9), primary key (paperId, authorId), constraint foreign key (paperId) references listPapers(id)  on update cascade on delete restrict, constraint foreign key (authorId) references listAuthors(id)  on update cascade on delete restrict) ENGINE=InnoDB;")

            #self.cursor.execute("Create table paperKeywords_aso (paperId int(9), keywordId int(9), primary key(paperId, keywordId), foreign key (paperId) references listPapers(id), foreign key (keywordId) references listKeywords(id))ENGINE=InnoDB;");

            # Download temporary tables
            self.cursor.execute("Create table queryList (id int(9) primary key auto_increment, query text, imported int(9), totals int(9))ENGINE=InnoDB")

            self.cursor.execute("""Create table papersCitesDownload (
                    paperId char(20),
                    cite enum('in','out'),
                    download enum('true', 'false', 'progress'),
                    error enum('true','false'),
                    errorText text,
                    primary key(paperId, cite),
                    constraint foreign key(paperId) references listPapers(id)  on update cascade on delete restrict
                    ) ENGINE=InnoDB""")

            self.commit()
        except:
            self.db.rollback()
            raise


    def getSourceId(self, abbrev, title):
        '''
            search in the database for the id from source, and if don't exist insert new row
        '''
        if title is not None and len(title)>0:
            self.cursor.execute("Select id from listSources where title= %s limit 1", (title,))
        else:
            self.cursor.execute("Select id from listSources where abr= %s limit 1", (abbrev,))
        if self.cursor.rowcount:
            r=self.cursor.fetchone()[0]
        else:
            self.cursor.execute("Insert into listSources (title, abr) values (%s, %s)", (title, abbrev))
            r=self.cursor.lastrowid
        return r


    def getAuthorId(self, abbrev, name):
        self.cursor.execute("Select id from listAuthors where abr=%s and name =%s limit 1", ( abbrev, name))
        if self.cursor.rowcount:
            r=self.cursor.fetchone()[0]
        else:
            self.cursor.execute("Insert into listAuthors (abr, name) values (%s, %s)", (abbrev, name))
            r=self.cursor.lastrowid
        return r

    """def getKeywordId(self, keyword):
        self.cursor.execute("Select id from listKeywords where name= %s limit 1", (keyword, ))
        if self.cursor.rowcount!=0:
            r=self.cursor.fetchone()[0]
        else:
            self.cursor.execute("Insert into listKeywords (name ) values (%s)", (keyword, ))
            r=self.cursor.lastrowid
        return r

    def getCountryId(self, country):
        self.cursor.execute("Select id from listCountries where name= %s limit 1", (country, ))
        if self.cursor.rowcount!=0:
            r=self.cursor.fetchone()[0]
        else:
            self.cursor.execute("Insert into listCountries (name ) values (%s)", (country, ))
            r=self.cursor.lastrowid
        return r"""

    def linkPapers(self, source, destination):
        self.cursor.execute("insert into paperPaper_aso (srcId, dstId) values (%s, %s)", (source, destination))

    def linkAuthors(self, paper, listAuthors):
        def link(e):
            self.cursor.execute("insert into paperAuthors_aso (paperId, authorId) values (%s, %s)", (paper, e))
        [link(e) for e in listAuthors]

    """def linkCountries(self, paper, listCountries):
        def link(e):
            self.cursor.execute("insert into paperCountries_aso (paperId, countryId) values (%s, %s)", (paper, e))
        [link(e) for e in listCountries]

    def linkKeywords(self, paper, listKeywords):
        def link(e):
            self.cursor.execute("insert into paperKeywords_aso (paperId, keywordId) values (%s, %s)", (paper, e))
        [link(e) for e in listKeywords]"""

    def getPaper(self, recId, data=None):
        """

        @param recId: Paper identifier
        @type recId: string
        @param data: Requiered fields, None to all fields
        @type data: list
        @return:
        @rtype: list
        """
        reqFields='*'
        if data!=None:
            reqFields="`"+"`,`".join(data)+"`"
        #print reqFields, recId
        self.cursor.execute("select %s from listPapers where id=%s limit 1" % (reqFields, '%s'),(recId, ))
        return self.cursor.fetchone()

    def getLinkPaper(self, source=None, destination=None):
        assert(source is not None or destination is not None)
        where=[]
        where_values=[]
        if source is not None:
            where.append("srcId = %s")
            where_values.append(source)

        if destination is not None:
            where.append("dstId = %s")
            where_values.append(source)

        self.cursor.execute("select srcId, dstId from paperPaper_aso where "+" and ".join(where), where_values)
        return self.cursor.fetchall()



    def updatePaper(self, recId, doi, title, source, volum, Page, year, month, language, cites):
        self.cursor.execute(""" update listPapers  set 
            doi=%s, title=%s, source=%s, year=%s, month=%s
            where id=%s limit 1""", (doi, title, source, year, month, recId))

    def insertEmptyPaper(self, recId):
        self.cursor.execute("insert into listPapers (id) values (%s)", (recId, ))

    def insertPaper(self, recId, doi, title, source, year, month):
        #print recId, doi, title, source, year, month
        self.cursor.execute("""
            insert into listPapers 
            (id, doi, title, source, year, month)
            values 
            (%s, %s, %s, %s, %s, %s)""", (recId, doi, title, source, year, month,))

    def getQuery(self):
        self.cursor.execute("select id, query, imported, totals from queryList where imported<=totals limit 1")
        return self.cursor.fetchone()

    def addQuery(self, query):
        self.cursor.execute("insert into queryList (query, imported, totals) values (%s, 1, 1)", query)
        self.commit()

    def updateQuery(self, queryId, imported, totals):
        self.cursor.execute("update queryList set imported=%s, totals=%s where id=%s limit 1", (imported, totals, queryId))

    def getReferenceToEmptyPaper(self, skip=0):
        self.cursor.execute("select srcId from paperPaper_aso where dstId = (select id from listPapers where year is null limit %s,1) limit 1", (skip, ))

        return self.cursor.fetchone()

    def getPaperToDownload(self, paperId=None, cite=None):
        appendAnd=''
        if cite is not None:
            appendAnd='and cite="'+cite+'"'

        if paperId is None:
            self.cursor.execute("select paperId, cite from papersCitesDownload where download='false' and error='false' %s limit 1" % appendAnd)
        else:
            self.cursor.execute("select paperId, cite from papersCitesDownload where paperId=%s %s limit 1" % ("%s", appendAnd), (paperId, ))
        return  self.cursor.fetchone()

    def updatePaperToDownload(self, recId, cite, download=True, error=False, errorText=''):
        self.cursor.execute("update papersCitesDownload set download= %s, error=%s, errorText=%s where paperId=%s and cite=%s", (str(download).lower(), str(error).lower(), errorText, str(recId), cite))

    def insertPaperToDownload(self, recId, cite):
        #print recId, typeDownload;
        cite=cite.lower()
        if cite!='in' and cite!='out':
            raise Exception("Problem with type (%s) of paper to download " % (cite, ))

        self.cursor.execute("insert into papersCitesDownload (paperId, cite, download, error, errorText) values (%s, %s, 'false', 'false', '')", (recId, cite))

    def changePaperKey(self, orig, dst):
        """
            Change a paper Id, changing all tables that are afected.
        """
        assert(int(orig)!=int(dst))
        exist=self.getPaper(dst,['id']) is not None
        if not exist:
            self.cursor.execute("""insert into listPapers 
            (id, doi, title, source, year, month)
            select '%s' , doi, title, source, year, month
            from listPapers where id=%s""" % (dst,'%s'), (orig, ))

        self.cursor.execute("update paperPaper_aso set dstId=%s where dstId=%s", (dst, orig))

        self.cursor.execute("update paperPaper_aso set srcId=%s where srcId=%s", (dst, orig))

        self.cursor.execute("update paperAuthors_aso set paperId=%s where paperId=%s", (dst, orig))

        self.cursor.execute("update paperKeywords_aso set paperId=%s where paperId=%s", (dst, orig))

        self.cursor.execute("delete from listPapers where id=%s limit 1", (orig, ))

    def commit(self):
        self.db.commit()
        #self.db.begin();
        self.cursor=self.db.cursor()

    def rollback(self):
        self.db.rollback()
        #self.db.begin();
        self.cursor=self.db.cursor()
        

