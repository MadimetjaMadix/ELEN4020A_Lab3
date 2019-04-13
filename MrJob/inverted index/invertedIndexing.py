from mrjob.job  import MRJob
from mrjob.step import MRStep
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

class MRInvertedIndex(MRJob):
	def steps(self):
	
		return [MRStep(mapper   = self.mapper_get_word_locations,
						 reducer  = self.reducer_words_locations_list)]	
						 	 
	def mapper_get_word_locations(self, _, line):
		key, line = line.split("\t")
		# yield each word in the line and the line number in key
		for word in WORD_RE.findall(line):
			if word.lower() not in STOP_WORDS:
				if not word.isdigit():
					yield (word.lower(), key)
	
	def reducer_words_locations_list(self, word, line_numbers):
		line_numbers_list = list(line_numbers)#create list object
		yield (word, line_numbers_list)
		
if __name__ == '__main__':

	MRInvertedIndex.run()
