import xml.etree.ElementTree as etree
import re
import pandas as pd
import numpy as np
from DatabaseWiki import databaseWiki
import mwparserfromhell as parse
from tqdm import tqdm

class ParseDumpWiki:
    """
     \author Biasini Mirko s181753, Carmignani Vittorio s181755, Joao Alemao s182312
     \date nov 2018
     \version 1.0
     \brief class to parse the dump wikipedia file
     \details The class contains functions in order to parse the dump wikipedia files.
     It uses the databaseWiki library to save information about category, titles etc.
    """

    ##Path in which the dump is located
    DUMP_PATH = 'Wikipedia/enwiki-latest-pages-articles.xml'
    ##type:int = number of tag=page parsed, which comprends every type of page
    totalCount = 0
    ##type:int = number of actual pages parsed
    pagesCount = 0

    def __init__(self):
        """
        \brief Default constructor, all parameters are initialized.
        """
        ##type:databaseWiki = access to the database
        self.db = databaseWiki()
        ##type:set = pairs (title of category, title of page) to be saved in #db
        self.listCatPag= set()
        ##type:set =  categories' title to be saved in #db
        self.listCat= set()
        ##type:set = pages' title to be saved in #db
        self.listPag = set()
        ##type:set = pairs (title of category, title of sub_category) to be saved in #db
        self.listCatSub = set()
    
    @staticmethod
    def strip_tag_name(t):
        """
        \brief Function to strip the namespaces from the tags.
        \param t :str =  raw tag name
        \return str = tag's name
        """
        idx = k = t.rfind("}")
        if idx != -1:
            t = t[idx + 1:]
        return t

    def insertCategoryPage(self,textCategories,title):
        """
        \brief The function parses the textCategories in order to find the actual categories. It add the page title
        in #listPag. It add all the categories and pairs (category, page title) in the respective #listCat and #listCatPag
        \param textCategories :str  = raw text which comprends the keywords [[Category:....|...]
        \param title :str  = title of the page 
        """
        res = re.findall(r"\[\[Category:(.*?)[\||\]\]]",textCategories)
        self.listPag.add(title)
        for r in res:
            ct = self.normName(r)
            self.listCat.add(ct)
            self.listCatPag.add((ct,title))
    
    @staticmethod
    def normName(name):
        """
        \brief Given the the string as parameter, the function removes duplicate or initial whitespaces and substute all
        whitespaces in _.
        \param name :str = name to be transformed
        \return str = normalized string
        """
        return " ".join(name.split()).translate(str.maketrans(" ", "_"))

    
    def insertCatSub(self,textCategories,sub):
        """
        \brief The function parses the textCategories in order to find the actual categories. It add the category sub
        in #listCat. It add all the categories and pairs (category, sub_category) in the respective #listCat and #listCatSub
        \param textCategories :str = raw text which comprends the keywords [[Category:....|...]
        \param sub :str = name of the sub category 
        """
        res = re.findall("\[\[Category:[^(.*?)[\||\]]+",textCategories)
        sub = self.normName(sub[9:])
        self.listCat.add(sub)
        for r in res:
            ct = self.normName(r[11:])
            self.listCat.add(ct)
            self.listCatSub.add((ct,sub))

    def saveData(self):
        """
        \brief This function call the function insertCatPagList() passing the following values:
        #listCatPag, #listCat, #listPag, #listCatSub (in order to save and insert the data in the database). Then,
        the variable are initialized to set().
        """
        self.db.inserCatPagList(self.listCatPag,self.listCat,self.listPag,self.listCatSub)
        self.listCatPag= set()
        self.listCat= set()
        self.listPag = set()
        self.listCatSub = set()

    def saveText(self,text,title):
        """
        \brief This function call the respective functions to trasform the text and obtain in frequency distribution of the words contained.
        \param text :str = raw text to be cleaned by the Wikipedia tags and saved.
        \param title :str = title of the page which contain text.
        """
        wiki = parse.parse(text)
        self.db.saveDocument(text=wiki.strip_code().strip(),title=title)

    def parse(self, maxNumberPages = 100000):
        """
        \brief The function parse the DUMP file and save the relative information. Each time the database is re-created.
        \details The function call the function createDatabase. Parsing the DUMP file, 
        are skipped all the pages which have: 'redirect' tag, number of template different from 14 (category) or 0 (page), no text, 
        no categories. For those pages aligned with the above requirements, the following functions are called: #insertCatSub, #normName, #insertCategoryPage 
        #saveText. The parse stops when it has analyzed #maxNumberPages.
        \param maxNumberPages :int = valid pages to be analyzed
        """
        self.db.createDatabase()

        title = None
        isCategoryPage = False
        isValid = True

        for event, elem in etree.iterparse(self.DUMP_PATH, events=('start', 'end')):
            tname = self.strip_tag_name(elem.tag)
            if event == 'start':
                if tname == 'page':
                    title = None
                    isCategoryPage = False
                    isValid = True
            else:
                if tname == 'title':
                    title = self.normName(elem.text)
                elif tname == 'ns':
                    if elem.text == "14":
                        isCategoryPage = True
                    elif elem.text != "0":
                        isValid = False
                elif isValid and tname == 'redirect':
                    isValid = False
                elif isValid and tname == 'text':
                    text = elem.text
                    if(text is not None):
                        c = text.find('[[Category:')
                        if(c!=-1):
                            if (not isCategoryPage):
                                self.saveText(text[:c],title)
                                self.insertCategoryPage(text[c:],title)
                            else:
                                self.insertCatSub(text[c:],title)
                elif tname == 'page':
                    self.totalCount += 1
                    if isValid:
                        self.pagesCount += 1
                    if self.pagesCount%10000 == 0:
                        self.saveData()
                        print(self.pagesCount)
                        if(self.pagesCount==maxNumberPages):
                            break
                elem.clear()

        self.saveData()
        self.db.close()

    def printStats(self):
        """
        \brief The function print the number of total tags page and actual pages scanned.
        """
        print("Total pages: {:,}".format(self.totalCount))
        print("Template pages: {:,}".format(self.pagesCount))
