import random
import csv
from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import scipy as sp
from DatabaseWiki import databaseWiki
from ParseDumpWiki import ParseDumpWiki
import pickle
import math
import time
from tqdm import tqdm
import wikipedia
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

class Categorization:
    """
    \author Biasini Mirko s181753, Carmignani Vittorio s181755, Joao Alemao s182312
    \date nov 2018
    \version 1.0
    \brief Library to execute categorization on the wikipedia pages
    \details The class contains all the functions to execute the wikipedia pages categorization.
    """
    ##Path in file are located
    PATH = 'Wikipedia/'

    def __init__(self):  
        """
        \brief Default constructor, it initializes the databaseWiki variable.
        """
        ##type:databaseWiki = access to the database
        self.db = databaseWiki()

    def writeFile(self, f, fname):
        """
        \brief The function write a pickle file storing the element given in input.
        \param f :object = Object which will be saved in a pickle file.
        \param fname :string = Name to be used to save the pickle file.
        """
        with open(self.PATH + fname, 'wb') as handle:
            pickle.dump(f, handle, protocol = pickle.HIGHEST_PROTOCOL)

    def readFile(self, fname):
        """
        \brief The function read a pickle file and return the related structure.
        \param fname :string = Name of the pickle file to be read.
        \return object = Object containing the structure stored by the pickle file.
        """
        res = None
        with open(self.PATH + fname, 'rb') as handle:
            res = pickle.load(handle)
        return res

    def getVectors(self):
        """
        \brief The function computes the vector representation for all the Wikipedia pages contained in the dataset.
        \return dict = Dictionary containing the vectors representaion of the pages.
        """
        vectors = dict()
        i = 0
        N = len(self.db.invertedIndex)
        for w, (idf, docs) in self.db.invertedIndex.items():
            for doc, tf in docs.items():
                try:
                    vectors[doc][i] = tf * idf
                except KeyError as k:
                    vectors[doc] = {i: tf * idf}
            i += 1
        i = 0;
        return vectors

    def cosin_sim_pairs(a, b):
        """
        \brief The function receives as input #a and #b which are the vector representions of two pages and computes the cosine 
         distance between them.
        \param a :dict = Dictionary containing the vector representation of the first page. 
        \param b :dict = Dictionary containing the vector representation of the second page. 
        \return float = Cosine distance of the two given pages.
        """
        wordsA = set(a.keys())
        wordsB = set(b.keys())
        inter = wordsA.intersection(wordsB)
        if(len(inter) == 0):
            return 0.0
        aa, bb, ab = 0, 0, 0
        for k in inter:
            aa += a[k] ** 2
            bb += b[k] ** 2
            ab += a[k] * b[k]
        for k in wordsA - inter:
            aa += a[k] ** 2
        for k in wordsB - inter:
            bb += b[k] ** 2
        return ab / float(math.sqrt(aa) * math.sqrt(bb))

    def getAllCentroids(self, inferior_limit = 5, withPrint = True, saveFile = True, test = []):
        """
        \brief The function create all the centroids of the categories with at least inferior_limit number of pages.
        \param inferior_limit :int (Default = 5): Minimum number of pages such that a page is computed.
        \param withPrint :bool (Default = True): True if the function has to print the initial line, false otherwise.
        \param saveFile :bool (Default = True): True if the function has to save the pickle file containing the centroids, false otherwise. 
        \param test :list (Default = []): List representing the test set.
        \return dict = Dictionary containing centroids vector for each category.
        """
        i = 0
        if(withPrint):
            print("I'm creating the page-categories dictionary")
        pageCat = self.db.getAllCategoriesGivenAllPages(inferior_limit)
        if(len(test) > 0):
            for p in test:
                pageCat.pop(p, None)
        if(withPrint):
            print("I'm creating the category-number of pages related dictionary")
        lenCategories = {c:float(n) for c, n in self.db.getCaregoriesNPages(inferior_limit)}
        if(withPrint):
            print("I'm creating the centroids")
        centroids = {c:{} for c in lenCategories}
        for w, (idf, docs) in self.db.invertedIndex.items():
            for doc, tf in docs.items():
                try:
                    for cat in pageCat[doc]:
                        centroids[cat][i] = centroids[cat].get(i, 0) + tf * idf / lenCategories[cat]
                except KeyError as k:
                    pass
            i += 1
        if(saveFile):
            self.writeFile(centroids, "centroids.pickle")
        return centroids

    def getCluster(self, eps = None, minPts = None):
        """
        \brief The function goes through the Wikipedia vectors and computes the clustering over them.
        \param eps :float = Maximum distance between two points on the same cluster.
        \param minPts :int = The number of samples (or total weight) in a neighborhood for a point to be considered as a core point. This includes the point itself.
        \return list = List containing cluster labels.
        """
        #D = getDistanceMatrix()
        #print("Distance matrix completed, clustering in process")
        clusters = DBSCAN(metric=Categorization.cosin_sim_pairs).fit_predict(np.arange(186696).reshape(-1, 1))
        print("Clustering completed, writing pickle file")
        self.writeFile(clusters, "clusters.pickle")
        return clusters
    
    def getDistanceMatrix(self):
        """
        \brief The function computes the matrix containing the distances between the vector representations of the wikipedia pages.
        \return matrix = Matrix containing the Wikipedia pages distances.
        """
        v = self.getVectors()
        vLis = v.keys()
        N = len(v.keys())
        D = np.zeros([N, N], dtype=np.float32)
        print(N)
        for i in range(N):
            print("%d/%d" %(i, N))
            D[i, i] = 1
            for j in range(i + 1, N):
                dist = self.cosin_sim_pairs(v[vLis[i]], v[vLis[j]])
                D[i, j] = dist
                D[j, i] = dist
        return D

    def getVector(self, p):
        """
        \brief The function receives as input #p which is a Wikipedia page name and it computes its vector representation.
        \param p :string = Name of the Wikipedia page to be computed. 
        \return dict = Dictionary containing the vector representation of the given page.
        """
        vector = {}
        i = 0
        tr = ParseDumpWiki.normName(p)
        if(self.db.isInPage(tr)):
            for w, (idf, docs) in self.db.invertedIndex.items():
                if (p in docs):
                    vector[i] = idf * docs[p]
                i += 1
        else:
            freqDist = self.db.transformDocument(wikipedia.page(p).content)
            indexesWords = list(self.db.invertedIndex.keys())
            commonWords = set(indexesWords).intersection(freqDist.keys())
            for w in commonWords:
                idf, docs = self.db.invertedIndex[w]
                vector[indexesWords.index(w)] = idf * freqDist[w]
        return vector

    def recommendCategory(self, page, randomWeb, centroids = None, nSugg = None, printRes = True):
        """
        \brief The function receives as input #page which is the name of the page to be recommended. Additionally,
        it returns the boolean, fractional and hierarchical measures.
        \param page :string = Name of the page to be recommended.
        \param randomWeb :bool = True if the page has to be randmly chosen from the web, false otherwise.
        \param centroids :dict (Default = None): Dictionary containing the centroid vectors.
        \param nSugg :int (Default = None): Number of categories to be recommmeded for the given page.
        \param printRes :bool (Default = True): True if the function has to print the initial sentence, false otherwise.
        \return (int, float, float) = (Boolean measure, Fractional measure, Hierarchical measure)
        """
        if(centroids is None):
            try:
                centroids = self.readFile("centroids.pickle")
            except:
                centroids = self.getAllCentroids()
        
        if(randomWeb):
            try:
                wikipedia.page(page)
            except Exception as e:
                print("We did not find the page on wikipedia, write it without '_'")
                return

        if(printRes):
            print("\nI'm categorizing the '%s' page.." % page)
        
        pageVector = self.getVector(page)
        
        res = {cat:Categorization.cosin_sim_pairs(pageVector, centre) for cat, centre in centroids.items()}

        if(randomWeb):
            actual = [ParseDumpWiki.normName(c) for c in wikipedia.page(page).categories]
        else:
            actual = self.db.getCategoriesGivenPage(page)

        if(nSugg is None):
            if(len(res) < len(actual)):
                nSugg = len(res)
            else:
                nSugg = len(actual)

        top = sorted(res.items(), key = lambda kv: kv[1], reverse=True)[:nSugg]
        m1, m2, m3 = self.measures(actual, top, nSugg)
        if(printRes):
            Categorization.printStats(m2, m3, actual, top)
        return m1, m2, m3

    def measures(self, actual, top, nSugg):
        """
        \brief The function receives as input #actual, #top and #nSugg which are the real categories, the suggested categories and the number of suggested categories.
         Then, it returns the boolean, fractional and hierarchical measures.
        \param actual :list = List containing the real categories.
        \param top :list = List containing the recommended categories.
        \param nSugg :int = Number of recommended categories.
        \return bool = Boolean measure.
        \return float = Fractional measure.
        \return float = Hierarchical measure.
        """
        m2 = 0.0
        m3 = 0.0
        for categorySug, count in top:
            if categorySug in actual:
                m2 += 1.0
            else:
                for cR in actual:
                    if self.getFatherSon(cR, categorySug) != None:
                        m3 += 0.5
                    elif self.getBrothers(cR, categorySug) != None:
                        m3 += 0.25
                m3 /= len(actual)
        m2 /= nSugg
        m3 = m2 + m3 / nSugg
        return 1 if m2 > 0 else 0, m2, m3

    @staticmethod
    def printStats(m2, m3, actual, top):
        """
        \brief The function receives as input #m2, #m3, #actual and #top which are the fractional measure, the hierarchical measure, the real categories and the recommended
         categories. Then, it prints the reccomendation results.
        \param m2 :float = Fractional measure.
        \param m3 :float = Hierarchical measure.
        \param actual :list = List containing the real categories.
        \param top :list = List containing the recommended categories.
        """
        print("\nThe actual categories for this page are: %s" % ", ".join(sorted(actual)))
        print("\nThe suggested categories for this page are: %s" % ", ".join(sorted([v for v, count in top])))
        print("\nBOOLEAN MEASURE = %s" %(m2 != 0))
        print("FRACTIONAL MEASURE = %0.2f" %(m2))
        print("HIERARCHICAL MEASURE = %0.2f\n" %(m3))
        print("*" * 150)
    
    def getBrothers(self, catR, catS):
        """
        \brief The function receives as input #catR and #catS which are the name of the real and suggested category respectively. 
         Then, it returns a list containing the brother categories.
        \param catR :string = Name of the real category.
        \param catS :string = Name of the suggested category.
        \return list = List containing the brother categories.
        """
        c = self.db.db.cursor()
        c.execute('SELECT * FROM catsub WHERE ? IN (SELECT cat_name FROM cat_name_sub WHERE cat_name_sub = ?)', [catS, catR])
        return c.fetchall()

    def getFatherSon(self, catR, catS):
        """
        \brief The function receives as input #catR and #catS which are the name of the real and suggested category respectively. 
         Then, it returns a list containing the father/son category.
        \param catR :string = Name of the real category.
        \param catS :string = Name of the suggested category.
        \return list = List containing the father/son category.
        """
        c = self.db.db.cursor()
        c.execute('SELECT * FROM catsub WHERE (cat_name_sub = ? AND cat_name = ?) OR (cat_name_sub = ? AND cat_name = ?)', [catS, catR, catR, catS])
        return c.fetchall()

    def evaluation(self, npages, centroids = None, randomWeb = False, pageWeb = None):
        """
        \brief The function receives as input #npages which is the number of Wikipedia pages to be evaluated and #pageWeb which is the page to be recommended. 
         It computes the boolean, fractional and hierarchical measures and prints them.
        \param npages :int = Number of pages used for the algorithm evaluation.
        \param centroids :dict (Default = None): Dictionary containing the centroid vectors.
        \param randomWeb :bool (Default = False): True if the #pageWeb is not None, false otherwise.
        \param pageWeb :string (Default = None): String containing the page to be recommended.
        """
        if(centroids is None):
            try:
                centroids = self.readFile("centroids.pickle")
            except:
                centroids = self.getAllCentroids(5)
        
        if(randomWeb):
            pages = [pageWeb]
        else:
            pages = random.sample(self.db.getPages(), npages)

        m1, m2, m3 = [], [], []
        for page in pages:
            m1p, m2p, m3p = self.recommendCategory(page=page,centroids=centroids,randomWeb=randomWeb)
            m1.append(m1p)
            m2.append(m2p)
            m3.append(m3p)

        avg1 = np.mean(m1)
        avg2 = np.mean(m2)
        avg3 = np.mean(m3)

        print("%s%s%s" %("#" * 75, "   OVERALL RESULTS   ", "#" * 75))
        print("Considering the %d pages" %(npages))
        print("The categorization succeeded %d out of %d times" %(avg1, npages))
        print("The fractional measure scored %0.2f" %(avg2))
        print("The hierarchical measure scored %0.2f" %(avg3))

    def measurements(self, minPag = 4, maxPag = 50, nPagesRacc = 100, percentageTest = 0.20):
        """
        \brief The function receives as input #nPageRacc and #percentageTest which are the number of pages to be recommended for each step and the dataset fraction
         to use as test set. It computes several test recommending every time #nPageRacc pages. For each step it considers a different number of categories. Specifically,
         it starts with the categories containing at least #minPag pages and iteratively considers the categories with a larger number of pages. In the final iteration, it 
         computes the categories with at least #maxPag pages. Finally, the function write the results obtained in a pickle called "avg.pickle" in which it is saved the list
         returned by the function.         
        \param minPag : int (default=4) : minimum number of pages for the first iteration (to construct the centroids).
        \param maxPag : int (default=4) : minimum number of pages for the last iteration (to construct the centroids).
        \param nPagesRacc :int (default=100)= Number of pages to be recommended.
        \param percentageTest :float (default =0.2)= Fraction of the dateset to use as test set.
        \return list = The list cointaned tuples. Each tuple contains: (nPage, len(centroids), elapsed_time, mean(Boolean measure), std(Boolean measure),mean(Fractional measure), 
         std(Fractional measure),mean(Hierarchical measure), std(Hierarchical measure))
        """
        allPages = self.db.getPages()
        test = random.sample(allPages, int(len(allPages) * percentageTest))
        avg = []
        centroids = self.getAllCentroids(inferior_limit = minPag, withPrint = False, saveFile = False, test = test)
        for nPage in tqdm(range(minPag, maxPag+1)):
            c = self.db.db.cursor()
            c.execute('SELECT cat_name FROM catpage GROUP BY cat_name HAVING COUNT(*)<?', [nPage]) 
            for ex in {c[0] for c in c.fetchall()}.intersection(centroids.keys()):
                centroids.pop(ex,None)
            m1, m2, m3 = [], [], []
            start_time = time.time()
            for page in random.sample(test, nPagesRacc):
                m1p, m2p, m3p = self.recommendCategory(page = page, centroids = centroids, randomWeb = False, printRes = False)
                m1.append(m1p)
                m2.append(m2p)
                m3.append(m3p)
            elapsed_time = time.time() - start_time
            tuple = (nPage, len(centroids), elapsed_time, np.mean(m1), np.std(m1), np.mean(m2), np.std(m2), np.mean(m3), np.std(m3))
            print("\n", tuple)
            avg.append(tuple)
        self.writeFile(avg, "avg.pickle")
        return avg

    def createGraph(self):
        """
        \brief The function reads a pickle file containing the results of a precomputed recommendation and creates a plot.
        """
        self.measurements(45,50,10)
        avg = self.readFile("avg.pickle")
        table = []
        for a in avg:
            table.append((a[0], a[1], a[2], a[3], a[4], "Boolean"))
            table.append((a[0], a[1], a[2], a[5], a[6], "Fractional"))
            table.append((a[0], a[1], a[2], a[7], a[8], "Hierarchical"))
        df = pd.DataFrame(table)
        df.columns = ["nPages", "nCentroids", "Time", "Mean", "Std", "Type"]
        print(df)
        sns.set(style = 'darkgrid')
        sns.lmplot(x = "nCentroids", y = "Mean",  col = "Type", hue="Type", data = df)
        #sns.lmplot(x = "nPages", y = "Mean",  col = "Type", hue="Type", data = df)
        #sns.scatterplot(x = "nCentroids", y = "Mean", size = "Time", hue = "Type", sizes = (20, 200), data = df)
        #sns.scatterplot(x = "nPages", y = "Mean", size = "Time", hue = "Type", sizes = (20, 200), data = df)
        plt.show()