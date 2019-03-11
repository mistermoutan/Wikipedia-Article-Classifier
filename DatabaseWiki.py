import sqlite3
import pickle
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
import re
import math
import codecs
from MapReduce import return_output

class databaseWiki:
    """
     \author Biasini Mirko s181753, Carmignani Vittorio s181755, Joao Alemao s182312
     \date nov 2018
     \version 1.0
     \brief Library to manage the database of pages
     \details The class contains all the functions to access/modify/save data in the database
    """

    ##Path in file are located
    PATH = '/Wikipedia/'
    ##Name of the database
    DB_NAME = 'database.db'
    ##Name of the dictionary of documents: keys = documents title, values = freqDist of the document.
    DICT_NAME = 'documents.pickle'
    ##Name of the inverted index
    INVERTED_NAME = 'inverted.pickle'
    ##English stopwords
    STOPWORDS = set(stopwords.words('english'))
    ##Porter stemmer
    stemmer = nltk.stem.PorterStemmer()

    """
    \brief Class to access to the database
    """
    def __init__(self):
        """
        \brief Default constructor, it initializes db with the file "database" if exists, otherwise, it creates it.      
        """
        ##type:database sqlite3 = access to the database file
        self.db = sqlite3.connect(self.PATH+self.DB_NAME)

        
        ##type:dict = keys = word, values = [dict = keys = page title, value = TF] .
        self.invertedIndex = dict()
        try:
            with open(self.PATH+self.INVERTED_NAME, 'rb') as handle:
                self.invertedIndex = pickle.load(handle)
        except IOError as e:
            pass

        ##type:dict = keys = documents title, values = freqDist of the document.
        self.documents = dict()
        if(len(self.invertedIndex))==0:
            try:
                with open(self.PATH+self.DICT_NAME, 'rb') as handle:
                    self.documents = pickle.load(handle)
            except IOError as e:
                pass

        ##type: int = Number of documents to be analyzed by the mapreducer if it is used
        self.tempDocuments = 0

    def close(self):
        """
        \brief Close the connection with the database. And save the #documents and #invertedIndex into pickles files
        """
        self.db.close()
        with open(self.PATH+self.DICT_NAME, 'wb') as handle:
            pickle.dump(self.documents, handle, protocol=pickle.HIGHEST_PROTOCOL)
        with open(self.PATH+self.INVERTED_NAME, 'wb') as handle:
            pickle.dump(self.invertedIndex, handle, protocol=pickle.HIGHEST_PROTOCOL)


    def createDatabase(self):
        """
        \brief The function delete all the exisisting tables (if there are) and creates the new ones.
        """
        c = self.db.cursor()
        c.executescript('''
            DROP TABLE IF EXISTS pages;
            DROP TABLE IF EXISTS categories;
            DROP TABLE IF EXISTS catpage;
            DROP TABLE IF EXISTS catsub;
            CREATE TABLE pages(title TEXT PRIMARY KEY);
            CREATE TABLE categories(name TEXT PRIMARY KEY);
            CREATE TABLE catpage(
                cat_name text, 
                pag_title text, 
                PRIMARY KEY (cat_name,pag_title), 
                FOREIGN KEY (pag_title) REFERENCES pages(title)
                    ON UPDATE CASCADE
                    ON DELETE NO ACTION,
                FOREIGN KEY (cat_name) REFERENCES categories(name)
                    ON UPDATE CASCADE
                    ON DELETE NO ACTION
            );
            CREATE TABLE catsub(
                cat_name text, 
                cat_name_sub text, 
                PRIMARY KEY (cat_name,cat_name_sub), 
                FOREIGN KEY (cat_name) REFERENCES categories(name)
                    ON UPDATE CASCADE
                    ON DELETE NO ACTION,
                FOREIGN KEY (cat_name_sub) REFERENCES categories(name)
                    ON UPDATE CASCADE
                    ON DELETE NO ACTION,
                CHECK (cat_name!=cat_name_sub)
            );
        ''')
        self.db.commit()
        self.documents = dict()
        self.invertedIndex = dict()
        self.tempDocuments = 0
        print("Database created")
    
    @staticmethod
    def printResults(title="",columns=[],rows=[]):
        """
        \brief The function print the results given the rows resulted by the "fetchall"
        \param title :str = Title of the print
        \param columns :str = columns' titles of the rows
        \param rows :list = rows resulted by the "fetchall"
        """
        print(title.upper())
        s = "Index"
        for c in columns:
            s+="%50s" % c
        print(s)
        for i,row in enumerate(rows):
            s = "%3d) " % i
            for v in row:
                s+="%50s " % str(v)
            print(s)

    def getPages(self,nrows=None,offset=0):
        """
        \brief The function returns rows of the content of pages's table.
        \param nrows :int (default=None): number of rows to be printed.
        \param offset :int (default=0): number of initial rows to be skipped.
        \return list of pages' title
        """
        c = self.db.cursor()
        if(nrows is None):
            c.execute("SELECT title FROM pages;")
        else:
            c.execute("SELECT title FROM pages LIMIT ? OFFSET ?;", [nrows,offset])
        return [r[0] for r in c.fetchall()]

    def viewPages(self,nrows=None,offset=0):
        """
        \brief The function print the content of pages's table.
        \param nrows :int (default=None): number of rows to be printed.
        \param offset :int (default=0): number of initial rows to be skipped.
        """
        rows = self.getPages(nrows=nrows,offset=offset)
        self.printResults(title="Pages",columns=["Name"],rows=rows)

    def getCategories(self,nrows=None,offset=0):
        """
        \brief The function returns rows of the content of categories's table.
        \param nrows :int (default=None): number of rows to be printed.
        \param offset :int (default=0): number of initial rows to be skipped.
        \return list of categories' name.
        """
        c = self.db.cursor()
        if(nrows is None):
            c.execute("SELECT name FROM categories;")
        else:
            c.execute("SELECT name FROM categories LIMIT ? OFFSET ?;", [nrows,offset])
        return [r[0] for r in c.fetchall()]

    def viewCategories(self,nrows=None,offset=0):
        """
        \brief The function print the content of categories's table.
        \param nrows :int (default=None): number of rows to be printed.
        \param offset :int (default=0): number of initial rows to be skipped.
        """
        rows = self.getCategories(nrows=nrows,offset=offset)
        self.printResults(title="Categories",columns=["Name"],rows=rows)

    def getCatPag(self, nrows=None,offset=0):
        """
        \brief The function returns rows of the content of catpage's table.
        \param nrows :int (default=None): number of rows to be printed.
        \param offset :int (default=0): number of initial rows to be skipped.
        \return :list = rows of the content of catpage's table.
        """
        c = self.db.cursor()
        if(nrows is None):
            c.execute('''SELECT * FROM catpage''')
        else:
            c.execute('''SELECT * FROM catpage ORDER BY pag_title LIMIT ? OFFSET ? ;''', [nrows,offset])
        return c.fetchall()

    def viewCatPag(self,nrows=None,offset=0):
        """
        \brief The function print the content of catpage's table.
        \param nrows :int (default=None): number of rows to be printed.
        \param offset :int (default=0): number of initial rows to be skipped.
        """
        rows = self.getCatPag(nrows=nrows,offset=offset)
        self.printResults(title="Categories & Pages",columns=["Category name","Page title"],rows=rows)
    
    def getCatSub(self,nrows=None,offset=0):
        """
        \brief The function returns rows of the content of catsub's table.
        \param nrows :int (default=None): number of rows to be printed.
        \param offset :int (default=0): number of initial rows to be skipped.
        \return :list = rows of the content of catsub's table.
        """
        c = self.db.cursor()
        if(nrows is None):
            c.execute('''SELECT * FROM catsub ORDER BY cat_name_sub''')
        else:
            c.execute('''SELECT * FROM catsub ORDER BY cat_name_sub LIMIT ? OFFSET ?;''', [nrows,offset])
        return c.fetchall()

    def viewCatSub(self,nrows=None,offset=0):
        """
        \brief The function print the content of catsub's table.
        \param nrows :int (default=None): number of rows to be printed.
        \param offset :int (default=0): number of initial rows to be skipped.
        """
        rows = self.getCatSub(nrows=nrows,offset=offset)
        self.printResults(title="Categories & Sub categories",columns=["Category name","Sub Category name"],rows=rows)
     
    def viewNCat(self):
        """
        \brief The function prints the number of rows in categories's table.
        """
        print(self.getNCat())

    def getNCat(self):
        """
        \brief The function returns the number of rows in categories's table.
        \return :int = number of categories saved.
        """
        c = self.db.cursor()
        c.execute('SELECT count(*) FROM categories')
        return int(c.fetchone()[0])

    def viewNPag(self):
        """
        \brief The function returns the number of rows in pages's table.
        """
        print(self.getNPag())

    def getNPag(self):
        """
        \brief The function returns the number of rows in pages's table.
        \return :int = number of pages saved.
        """
        c = self.db.cursor()
        c.execute('SELECT count(*) FROM pages')
        return int(c.fetchone()[0])

    def insertPage(self,page):
        """
        \brief The function insert a page in the relative table.
        \param page :str = page's title.
        """
        c = self.db.cursor()
        c.execute('INSERT INTO pages(title) VALUES (?)',[page])
        self.db.commit()

    def insertCategory(self,cat):
        """
        \brief The function insert a category's in the relative table.
        \param cat :str = category's name.
        """
        c = self.db.cursor()
        c.execute('INSERT INTO categories(name) VALUES (?);',[cat])
        self.db.commit()

    def insertCatPag(self,cat,page):
        """
        \brief The function insert a category's name cat if not present. Moreover, it 
        inserts the pair (cat,page) in the relative table catpage.
        \param cat :str = category's name. 
        \param page :str = title of the page.
        """
        c = self.db.cursor()
        try:
            self.insertCategory(cat)
        except sqlite3.IntegrityError:
            pass
        c.execute('INSERT OR IGNORE INTO catpage(cat_name,pag_title) VALUES (?,?)',[cat,page])
        self.db.commit()

    def inserCatPagList(self,listCatPag=set(),listCat=set(),listPag=set(), listCatSub=set()):
        """
        \brief This function should be used to boost the insert operations in the database.
        \details The function starts a transaction. It executes (or ignore if there are exceptions) all the insert operations taking every 
        value contained in the passed parameters. it commits the operations.
        \param listCatPag :set or list (default empty set) = set of pair (category's name,page's title)
        \param listCat :set or list (default empty set) = set of string with categories' title.
        \param listPag :set or list (default empty set) = set of string with pages' title.
        \param listCatSub :set or list (default empty set) = set of pair (category's name,sub_category's name)
        """
        c = self.db.cursor()
        c.execute("BEGIN TRANSACTION")
        for pag in listPag:
            c.execute("INSERT OR IGNORE INTO pages(title) VALUES (?);",[pag])
        for cat in listCat:
            c.execute("INSERT OR IGNORE INTO categories(name) VALUES (?);",[cat])
        for cat,pag in listCatPag:
            c.execute("INSERT OR IGNORE INTO catpage(cat_name,pag_title) VALUES (?,?);",[cat,pag])
        for cat,sub in listCatSub:
            c.execute("INSERT OR IGNORE INTO catsub(cat_name,cat_name_sub) VALUES (?,?);",[cat,sub])
        self.db.commit()
            
    def insertCatSub(self,cat,cat_sub):
        """
        \brief The function insert :category's name cat and car_sub if not present. Moreover, it 
        inserts the pair (cat,cat_sub) in the relative table catsub.
        \param cat :str = category's name. 
        \param cat_sub :str = sub category's name. 
        """
        c = self.db.cursor()
        try:
            self.insertCategory(cat)
        except sqlite3.IntegrityError:
            pass
        try:
            self.insertCategory(cat_sub)
        except sqlite3.IntegrityError:
            pass
        c.execute('INSERT INTO catsub VALUES (?,?)',[cat,cat_sub])
        self.db.commit()

    def getTopCategories(self, nrows = None, offset = 0):
        """
        \brief The function print all the categories which not compare in the catsub's column cat_name_sub.
        \param nrows :int (default=None): number of rows to be printed.
        \param offset :int (default=0): number of initial rows to be skipped.
        \return :list = rows resulted by the "fetchall"
        """
        c = self.db.cursor()
        if(nrows is None):
            c.execute('''SELECT DISTINCT(cat_name) FROM catsub WHERE cat_name NOT IN (SELECT DISTINCT(cat_name_sub) FROM catsub)''')
        else:
            c.execute('''SELECT DISTINCT(cat_name) FROM catsub WHERE cat_name NOT IN (SELECT DISTINCT(cat_name_sub) FROM catsub) LIMIT ? OFFSET ?;''', [nrows,offset])
        return c.fetchall()

    def viewTopCategories(self, nrows = None, offset = 0):
        """
        \brief The function print all the categories which not compare in the catsub's column cat_name_sub.
        \param nrows :int (default=None): number of rows to be printed.
        \param offset :int (default=0): number of initial rows to be skipped.
        """
        rows = self.getTopCategories(nrows=nrows,offset = 0)
        self.printResults(title="Top categories",columns=["Category name"],rows=rows)

    def isInPage(self,title):
        """
        \brief The function returns true if the title is in pages's table.
        \param title :str = page's table to be checked
        \return bool = True if title is in pages's table, False otherwise
        """
        c = self.db.cursor()
        c.execute("SELECT COUNT(title) FROM pages where title=?",[title])
        return bool(c.fetchone()[0])

    def getBiggestCaregories(self,inferior_limit=500):
        """
        \brief The function return the categories which contain at least the inferior_limit pages.
        The category in which appear "death" or "birth" are not taken into consideration.
        \param inferior_limit :int (default 500)= min number of pages
        \return :list = rows resulted by the "fetchall"
        """
        c = self.db.cursor()
        c.execute("""
                        SELECT cat_name, count(*) as c
                        FROM catpage
                        WHERE cat_name NOT LIKE '%birth%' AND cat_name NOT LIKE '%death%' AND NOT cat_name IN (
                            SELECT DISTINCT(cat_name)
                            FROM catsub
                        )
                        GROUP BY cat_name
                        HAVING c >= ?
                        ORDER BY c DESC
                    """,[inferior_limit])
        return c.fetchall()

   
    def getAverageCateForPage(self):
        """
        \brief The function returns the average number of category per page
        \return float = average
        """
        c = self.db.cursor()
        c.execute('SELECT AVG(S.C) FROM (SELECT COUNT(*) as C FROM catpage GROUP BY pag_title) as S')
        return float(c.fetchone()[0])

    def viewBiggestCategories(self,inferior_limit=500):
        """
        \brief The function print the categories in which compare the largest number of pages.
        The category in which appear "death" or "birth" are not printed.
        \param inferior_limit : min number of pages so that categories are taken into account
        """
        rows = self.getBiggestCaregories(inferior_limit)
        self.printResults(title="Biggest categories",columns=["Category name","Page number"],rows=rows)

    def getPagesGivenCategory(self,category):
        """
        \brief The function returns the pages' title contained into the category passed as parameter.
        \param category :str = name of the category
        \return :list = list of pages' title
        """
        c = self.db.cursor()
        c.execute("""SELECT pag_title FROM catpage WHERE cat_name=?""",[category])
        return [r[0] for r in c.fetchall()]

    def getCategoriesGivenPage(self,page):
        """
        \brief The function returns the categories'name of the page passed as parameter.
        \param page :str = title of the page
        \return :list = list of categories' name
        """
        c = self.db.cursor()
        c.execute("""SELECT cat_name FROM catpage WHERE pag_title=?""",[page])
        return [r[0] for r in c.fetchall()]

    def getCaregoriesNPages(self,inferior_limit=1):
        """
        \brief The function return the all the categories and the relative number of pages associated.
        \return :list = rows resulted by the "fetchall"
        """
        c = self.db.cursor()
        c.execute("""
                        SELECT cat_name, count(*) as c
                        FROM catpage
                        GROUP BY cat_name
                        HAVING COUNT(*) >= ?
                        ORDER BY cat_name
                    """,[inferior_limit])
        return c.fetchall()

    def getAllCategoriesGivenAllPages(self, inferior_limit):
        """
        \brief This function returns a dictionary where the keys are the pages and the values are: list of the categories associated.
        The relative SQL function should be: SELECT pag_title,GROUP_CONCAT(cat_name,SEPARATOR) FROM catpage GROUP BY pag_title HAVING COUNT(*)>= inferior_limit. The problem
        with SQLLIte3 is that does not allow to choose the separator but must be the comma. Because of that, and since there are many categories in
        which the comma is appear, we had to structure a more complicated SQL query.
        \param inferior_limit :int = number of minimum pages so that a category appear in the returned dictionary.
        \return dict = keys: pages values: list of categories associated
        """
        c = self.db.cursor()
        c.execute("""   SELECT pag_title,GROUP_CONCAT(cat_name) 
                        FROM catpage as princ
                        WHERE NOT cat_name LIKE '%,%' AND pag_title IN 
                            (SELECT pag_title
                            FROM catpage
                            WHERE cat_name IN 
                                (
                                SELECT cat_name
                                FROM catpage
                                GROUP BY cat_name
                                HAVING COUNT(*)>=?
                                )
                            )
                        GROUP BY pag_title """,[inferior_limit])
        pagesCat = {}
        results = c.fetchall()
        for page,concat in results:
            pagesCat[page]=concat.split(',')
        c.execute("""SELECT pag_title,
                     GROUP_CONCAT(REPLACE(cat_name,',','|')) 
                     FROM catpage 
                     WHERE cat_name LIKE '%,%' AND pag_title IN 
                            (SELECT pag_title
                            FROM catpage
                            WHERE cat_name IN 
                                (
                                SELECT cat_name
                                FROM catpage
                                GROUP BY cat_name
                                HAVING COUNT(*)>=?
                                )
                            )
                     GROUP BY pag_title""",[inferior_limit])
        results = c.fetchall()
        for page,concat in results:
            try:
                pagesCat[page]+=[p.replace("|",u"\u002C") for p in concat.split(',')]
            except:
                pagesCat[page]=[p.replace("|",u"\u002C") for p in concat.split(',')]
        return pagesCat

    def saveDocument(self,text="",title="",mapreduce=False):
        """
        \brief The function execute the different transformation on text and save the "freqDist" in the dictionary #documents.
        The key will be the title, the value the freqDist.
        \param text :str = text to be transformed and saved
        \param title :str = title of the document
        \param mapreduce :bool (default:False) = If it is true, the document text is written a folder as txt
        if 
        """
        if(not mapreduce):
            self.documents[title] = self.transformDocument(text)
        else:
            with codecs.open("TempDoc/%s.txt" % title,"w",encoding="utf-8") as file:
                file.write(text)
            self.tempDocuments+=1
            if(self.tempDocuments>500):
                results = return_output("TempDoc/*.txt")
                for r in results:
                    self.documents[r[0]] =r[1]
                self.tempDocuments=0
    
    def transformDocument(self,text):
        """
        \brief The function execute the different transformation on text and returns its "freqDist"
        \param text :str = text to be transformer
        \return dict = keys: words values: frequencies
        """
        listWords = nltk.word_tokenize(text)
        res = list()
        for word in listWords:
            word = word.lower()
            if(not bool(re.search(r"[^A-Za-z]", word)) and word not in self.STOPWORDS):
                res.append(self.stemmer.stem(word))
        return nltk.FreqDist(res)

    def createInvertedIndex(self):
        """
        \brief The function create the inverted index: keys = word, values = [dict: keys = page title, value = TF].
        """
        self.invertedIndex = dict()
        for doc,freqDist in self.documents.items():
            tot_lenght = float(sum(freqDist.values()))
            for word,tf in freqDist.items():
                try:
                    self.invertedIndex[word][1][doc] = tf/tot_lenght
                except KeyError as k:
                    self.invertedIndex[word] = [None,{doc:tf/tot_lenght}]        
        N_DOCS = float(len(self.documents))
        for word, pair in self.invertedIndex.items():
            pair[0] = math.log(N_DOCS/len(pair[1]))