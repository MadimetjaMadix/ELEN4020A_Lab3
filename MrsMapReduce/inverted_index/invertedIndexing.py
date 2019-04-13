import mrs
import string

import re

def getStopWords():
	return [ "a", "about", "above", "after", "again", 
	"against", "all", "am", "an", "and", "any", "are", 
	"as", "at", "be", "because", "been", "before", "being", 
	"below", "between", "both", "but", "by", "could", "did", 
	"do", "does", "doing", "down", "during", "each", "few", 
	"for", "from", "further", "had", "has", "have", "having", 
	"he", "he'd", "he'll", "he's", "her", "here", "here's", 
	"hers", "herself", "him", "himself", "his", "how", "how's", 
	"i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", 
	"it", "it's", "its", "itself", "let's", "me", "more", "most", 
	"my", "myself", "nor", "of", "on", "once", "only", "or", 
	"other", "ought", "our", "ours", "ourselves", "out", "over", 
	"own", "same", "she", "she'd", "she'll", "she's", "should", 
	"so", "some", "such", "than", "that", "that's", "the", 
	"their", "theirs", "them", "themselves", "then", "there", 
	"there's", "these", "they", "they'd", "they'll", "they're", 
	"they've", "this", "those", "through", "to", "too", "under", 
	"until", "up", "very", "was", "we", "we'd", "we'll", "we're", 
	"we've", "were", "what", "what's", "when", "when's", "where", 
	"where's", "which", "while", "who", "who's", "whom", "why", 
	"why's", "with", "would", "you", "you'd", "you'll", "you're", 
	"you've", "your", "yours", "yourself", "yourselves" ]
	
WORD_RE     = re.compile(r"[\w']+") #looks for words such as "word", word's "word's", word
STOP_WORDS  = getStopWords()

class MRSInvertedIndex(mrs.MapReduce):
    """Count the number of occurrences of each word in a set of documents.
    """
    def map(self, line_num, line_text):
        for word in WORD_RE.findall(line_text):
            word = word.strip(string.punctuation).lower()
            if word.lower() not in STOP_WORDS:
                if word:
                    yield (word, line_num)

    def reduce(self, word, line_num_list):
        yield word,list(line_num_list)

if __name__ == '__main__':
    mrs.main(MRSInvertedIndex)
