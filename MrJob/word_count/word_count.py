from mrjob.job  import MRJob
from mrjob.step import MRStep
from stop_words import getStopWords
import re

WORD_RE    = re.compile(r"[\w']+") #looks for words such as "word", word's "word's", word
STOP_WORDS = getStopWords()

class MRWordFrequencyCount(MRJob):
	def steps(self):
		return [  MRStep(mapper   = self.mapper_get_words,
						 combiner = self.combiner_count_words,
						 reducer  = self.combiner_count_words)]
		
	def mapper_get_words(self, _, line):
		# yield each word in the line
		for word in WORD_RE.findall(line):
			if word.lower() not in STOP_WORDS:
				if not word.isdigit():
					yield (word.lower(), 1)
			
	def combiner_count_words(self, word, counts):
		# optimization: sum the words we've seen so far
		yield (word, sum(counts))
		
if __name__ == '__main__':
	MRWordFrequencyCount.run()