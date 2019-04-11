from mrjob.job  import MRJob
from mrjob.step import MRStep
from stop_words import getStopWords
import re

WORD_RE     = re.compile(r"[\w']+") #looks for words such as "word", word's "word's", word
STOP_WORDS  = getStopWords()

class MRInvertedIndex(MRJob):	
	def mapper_raw_custom(self, file_path, file_uri):
		line_number = 0
		with open(file_path, 'rb') as text_file:
			for line in text_file:
				line_number+=1
				yield [line_number, line.strip().decode()]
		
	def steps(self):
		return [  MRStep(mapper_raw   = self.mapper_raw_custom), 
				  MRStep(mapper   = self.mapper_get_word_locations,
						 reducer  = self.reducer_words_locations_list)]
						 
	def mapper_get_word_locations(self, key, line):
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