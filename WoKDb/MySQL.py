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
        self.db=MySQLdb.Connect(host, user, pswd, database);
        #self.db.begin();
        self.cursor=self.db.cursor();


    def createTables(self):
        '''
            Create tables for insert the data;
        '''
        """ 
            Data i Mes
            Doi
            Titol
            Autors
            Keywords
            Source i abrev.
            bib Pages (inici, final, quantitat) & issue & Year
            Language
            Research country
        """
        try:
            self.cursor.execute("Create table listSources (id int(9) primary key auto_increment, title text, abr char(255)) ENGINE=InnoDB;")

            self.cursor.execute("""Create table listPapers 
                                (id int(9) primary key, 
                                 doi char(255), 
                                 title text, 
                                 source int(9),
                                 volum char(25),
                                 pageStart char(9),
                                 pageEnd char(9),
                                 numberPages char(9),
                                 year int(4),
                                 month int(2),
                                 language char(50),
                                 timescited int(9), 
                                 foreign key (source) references listSources(id)) ENGINE=InnoDB;""")

            self.cursor.execute("Create table listKeywords (id int(9) primary key auto_increment, name char(255)) ENGINE=InnoDB");

            #self.cursor.execute("Create table listCountries (id int(9) primary key auto_increment, name char(255))ENGINE=InnoDB");

            self.cursor.execute("Create table listAuthors (id int(9) primary key auto_increment, abr char(255), firstName varchar(255), secondName varchar(255), key (abr))ENGINE=InnoDB;")

            self.cursor.execute("Create table paperPaper_aso (srcId int(9), dstId int(9), primary key(srcId, dstId), foreign key (srcId) references listPapers(id), foreign key (dstId) references listPapers(id))ENGINE=InnoDB;");

            self.cursor.execute("Create table paperAuthors_aso (paperId int(9), authorId int(9), primary key (paperId, authorId), foreign key (paperId) references listPapers(id), foreign key (authorId) references listAuthors(id))ENGINE=InnoDB;");

            self.cursor.execute("Create table paperKeywords_aso (paperId int(9), keywordId int(9), primary key(paperId, keywordId), foreign key (paperId) references listPapers(id), foreign key (keywordId) references listKeywords(id))ENGINE=InnoDB;");

            # Download temporary tables
            self.cursor.execute("Create table queryList (id int(9) primary key auto_increment, query text, imported int(9), totals int(9))ENGINE=InnoDB");

            self.cursor.execute("Create table papersCitesDownload (paperId int(9), typeDownload enum('paper','cites'), download enum('true', 'false'), error enum('true','false'), errorText text, primary key(paperId, typeDownload))ENGINE=InnoDB");

            self.commit();
        except:
            self.db.rollback();
            raise


    def getSourceId(self, title, abrev):
        '''
            search in the database for the id from source, and if don't exist insert new row
        '''
        if title!=None and len(title)>0:
            self.cursor.execute("Select id from listSources where title= %s limit 1", (title,))
        else:
            self.cursor.execute("Select id from listSources where abr= %s limit 1", (abrev,))
        if self.cursor.rowcount!=0:
            r=self.cursor.fetchone()[0];
        else:
            self.cursor.execute("Insert into listSources (title, abr) values (%s, %s)", (title, abrev));
            r=self.cursor.lastrowid;
        return r;


    def getAuthorId(self, abrev, firstName, secondName):
        self.cursor.execute("Select id from listAuthors where abr=%s and firstName= %s and secondName= %s limit 1", ( abrev, firstName, secondName))
        if self.cursor.rowcount!=0:
            r=self.cursor.fetchone()[0];
        else:
            self.cursor.execute("Insert into listAuthors (abr, firstName, secondName) values (%s, %s, %s)", (abrev,firstName, secondName));
            r=self.cursor.lastrowid;
        return r;

    def getKeywordId(self, keyword):
        self.cursor.execute("Select id from listKeywords where name= %s limit 1", (keyword, ))
        if self.cursor.rowcount!=0:
            r=self.cursor.fetchone()[0];
        else:
            self.cursor.execute("Insert into listKeywords (name ) values (%s)", (keyword, ));
            r=self.cursor.lastrowid;
        return r;

    def getCountryId(self, country):
        self.cursor.execute("Select id from listCountries where name= %s limit 1", (country, ))
        if self.cursor.rowcount!=0:
            r=self.cursor.fetchone()[0];
        else:
            self.cursor.execute("Insert into listCountries (name ) values (%s)", (country, ));
            r=self.cursor.lastrowid;
        return r;

    def linkPapers(self, source, destination):
        self.cursor.execute("insert into paperPaper_aso (srcId, dstId) values (%s, %s)", (source, destination));

    def linkAuthors(self, paper, listAuthors):
        def link(e):
            self.cursor.execute("insert into paperAuthors_aso (paperId, authorId) values (%s, %s)", (paper, e));
        [link(e) for e in listAuthors];

    def linkCountries(self, paper, listCountries):
        def link(e):
            self.cursor.execute("insert into paperCountries_aso (paperId, countryId) values (%s, %s)", (paper, e));
        [link(e) for e in listCountries];

    def linkKeywords(self, paper, listKeywords):
        def link(e):
            self.cursor.execute("insert into paperKeywords_aso (paperId, keywordId) values (%s, %s)", (paper, e));
        [link(e) for e in listKeywords];

    def getPaper(self, recId, data=None):
        reqFields='*';
        if data!=None:
            reqFields="`"+"`,`".join(data)+"`";
        self.cursor.execute("select %s from listPapers where id=%s limit 1" % (reqFields, '%s'),(recId, ));
        return self.cursor.fetchone();

    def updatePaper(self, recId, doi, title, source, volum, Page, year, month, language, cites):
        self.cursor.execute(""" update listPapers  set 
            doi=%s, title=%s, source=%s, volum=%s, pageStart=%s, pageEnd=%s, numberPages=%s, year=%s, month=%s, language=%s, timescited=%s 
            where id=%s limit 1""", (doi, title, source, volum, Page[0], Page[1], Page[2], year, month, language, cites, recId));

    def insertEmptyPaper(self, recId):
        self.cursor.execute("insert into listPapers (id) values (%s)", (recId, ));

    def insertPaper(self, recId, doi, title, source, volum, Page, year, month, language, cites):
        self.cursor.execute("""
            insert into listPapers 
            (id, doi, title, source, volum, pageStart, pageEnd, numberPages, year, month, language, timescited) 
            values 
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (recId, doi, title, source, volum, Page[0], Page[1], Page[2], year, month, language, cites));

    def getQuery(self):
        self.cursor.execute("select id, query, imported, totals from queryList where imported<=totals limit 1");
        return self.cursor.fetchone();

    def updateQuery(self, queryId, imported, totals):
        self.cursor.execute("update queryList set imported=%s, totals=%s where id=%s limit 1", (imported, totals, queryId));

    def getReferenceToEmptyPaper(self, skip=0):
        self.cursor.execute("select srcId from paperPaper_aso where dstId = (select id from listPapers where year is null limit %s,1) limit 1", (skip, ));

        return self.cursor.fetchone();

    def getPaperToDownload(self, paperId=None, typeDownload=None):
        appendAnd='';
        if (typeDownload!=None):
            appendAnd='and typeDownload="'+typeDownload+'"'

        if paperId==None:
            self.cursor.execute("select paperId, typeDownload from papersCitesDownload where download='false' and error='false' {extraAnd} limit 1".format(extraAnd=appendAnd));
        else:
            self.cursor.execute("select paperId, typeDownload from papersCitesDownload where paperId=%s limit 1", (paperId, ));
        return  self.cursor.fetchone();

    def updatePaperToDownload(self, recId, typeDownload, download=True, error=False, errorText=''):
        self.cursor.execute("update papersCitesDownload set download= %s, error=%s, errorText=%s where paperId=%s and typeDownload=%s", (str(download).lower(), str(error).lower(), errorText, str(recId), typeDownload));

    def insertPaperToDownload(self, recId, typeDownload):
        #print recId, typeDownload;
        typeDownload=typeDownload.lower();
        if typeDownload!='paper' and typeDownload!='cites':
            raise Exception("Problem with type (%s) of paper to download " % (typeDownload, ));

        self.cursor.execute("insert into papersCitesDownload (paperId, typeDownload, download, error, errorText) values (%s, %s, 'false', 'false', '')", (recId, typeDownload));

    def changePaperKey(self, orig, dst):
        """
            Change a paper Id, changing all tables that are afected.
        """
        assert(int(orig)!=int(dst))
        exist=self.getPaper(dst,['id'])!=None;
        if not exist:
            self.cursor.execute("""insert into listPapers 
            (id, doi, title, source, volum, pageStart, pageEnd, numberPages, year, month, language, timescited) 
            select '%s' , doi, title, source, volum, pageStart, pageEnd, numberPages, year, month, language, timescited
            from listPapers where id=%s""" % (dst,'%s'), (orig, ));

        self.cursor.execute("update paperPaper_aso set dstId=%s where dstId=%s", (dst, orig));

        self.cursor.execute("update paperPaper_aso set srcId=%s where srcId=%s", (dst, orig));

        self.cursor.execute("update paperAuthors_aso set paperId=%s where paperId=%s", (dst, orig));

        self.cursor.execute("update paperKeywords_aso set paperId=%s where paperId=%s", (dst, orig));

        self.cursor.execute("delete from listPapers where id=%s limit 1", (orig, ));

    def commit(self):
        self.db.commit();
        #self.db.begin();
        self.cursor=self.db.cursor();

    def rollback(self):
        self.db.rollback();
        #self.db.begin();
        self.cursor=self.db.cursor();
        

