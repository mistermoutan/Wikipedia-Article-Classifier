"""
 \author Biasini Mirko s181753, Carmignani Vittorio s181755, Joao Alemao s182312
 \date nov 2018
 \version 1.0
 \brief Demo to test out the final solution
 \details This demo test our algorithm by recommending categories for XX random pages. In order to evaluate the precision of our
  out solution, it computes the number of correct suggestions.
"""

from Categorization import Categorization
from DatabaseWiki import databaseWiki
import wikipedia
from ParseDumpWiki import ParseDumpWiki
from tkinter import *

c = Categorization()

def funcB1():
    c.db.viewNCat()

def funcB2():
    c.db.viewNPag()

def funcB3():
    c.db.viewCatPag(20)

def funcB4():
    c.db.viewCatSub(20)

def funcB5():
    c.db.evaluation(5)

def funcB6():
    c.db.createGraph()

root = Tk()

b1 = Button(root, text="Print Number of Categories", command=funcB1, justify=LEFT, width=50)
b1.pack()

b2 = Button(root, text="Print Number of Pages", command=funcB2, justify=LEFT,width=50)
b2.pack()

b3 = Button(root, text="Print sample of table PageCat", command=funcB3, justify=LEFT, width=50)
b3.pack()

b4 = Button(root, text="Print sample of table CatSub", command=funcB4, justify=LEFT, width=50)
b4.pack()

b5 = Button(root, text="Print random reccomandations", command=funcB5, justify=LEFT, width=50)
b5.pack()

b6 = Button(root, text="Print graph of raccomandations with centroids", command=funcB5, justify=LEFT, width=50)
b6.pack()

root.mainloop()
d.close()