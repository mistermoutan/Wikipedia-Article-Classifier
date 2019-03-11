from mrjob.job import MRJob
import re
import os
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import nltk
import string
from mrjob.step import MRStep
from nltk.stem.porter import *
import numpy as np
    
def return_output(filename):
    """
    \brief The function parsess the output of the MRWordFrequencyCount class by creating a dictionary which we can use for further processing.
    \param filename :str : the files to run the MapReduce process on
    \return a list with 2-dimensional elements with the structure [(Document(string), {words in document :count}), (Document2(string), {words in document2 :count}), ...]
    """
    list = ()
    mr_job =  MRWordFrequencyCount(args=[filename])
    results = []
    with mr_job.make_runner() as runner:
        runner.run()
        for doc,wordcount in mr_job.parse_output(runner.cat_output()):
            results.append((doc,{word:count for word, count in wordcount}))
    return results
                
class MRWordFrequencyCount(MRJob):
    """
    \author Biasini Mirko s181753, Carmignani Vittorio s181755, Joao Alemao s182312
    \date nov 2018
    \version 1.0
    \brief Library to perform the transformation of the text with mapreduce
    \details The function performs different steps in order to read more files and perform: tokenization, deletion of stopwords,
    stemming and indexing.
    """
    def steps(self):
        """
        \brief The function specify which are the spes to be performed
        """
        return [
            MRStep(mapper=self.mapper,
                reducer=self.reducer),
            MRStep(mapper=self.mapper_round2,
                reducer=self.reducer_round2)
        ]

    def mapper(self,_,line):
        """
        \brief 1st mapper: parsing of each line of documents and consider only relevant words.
        \param _ : 
        \param line: default argument for parsing of each line
        \return 2 dimensional elements with the strucutre ((word,filename), 1)
        """
        stop = set(stopwords.words('english'))
        punctuation = [s for s in string.punctuation]
        filename = os.environ['map_input_file']
        tokens = word_tokenize(line)
        stemmer = PorterStemmer()
        for word in tokens:
            word = word.lower()
            if not bool(re.search("[^A-Za-z]",word)) and word not in stop:

                yield ((stemmer.stem(word),filename), 1)

    def reducer(self, info, count):
        """
        \brief 1st reducer: obtaining the count of each word for each document the word is present in.
        \param info :tuple first element given by the mapper, corresponds to (word,filename)
        \param count: int count of the word for the 
        \return 2 dimensional elements with the strucutre ((word,filename), count of the word in the file )
        """
        yield(info, sum(count))

    def mapper_round2(self,info,count):
        """
        \brief 2nd mapper: takes the output of the first reducer and arranges it in order to match the format we are interested in 
        \param info: tuple first argument from the first reducer, corresponds to (word,filename)
        \param count: int second argument from the first reducer, corresponds to the number of times the word is present in the file
        \return 2 dimensional elements with the strucutre (document name, (word,count))
        """
        yield(info[1], (info[0],count))

    def reducer_round2(self,doc,info2):
        """
        \brief 2nd reducer: takes the output of the second mapper and condenses all combinations of (word,count) for each document in a list
        \param doc: str first argument from the mapper, corresponds to document name
        \param info2: tuple second argument from the mapper, corresponds to  (word,count)
        \return 2 dimensional elements with the strucutre (document name, list((word,count)), the list contains all the words which remained after the process of cleaning the text that are present in each document
        """
        yield(doc,list(info2))

if __name__ == '__main__':
    MRWordFrequencyCount.run()