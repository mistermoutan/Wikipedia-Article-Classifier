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

p = ParseDumpWiki()
p.parse()
d = databaseWiki()
d.createInvertedIndex()
c = Categorization()
c.evaluation(5)