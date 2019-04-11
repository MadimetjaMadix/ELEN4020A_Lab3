from topKQueryJob import MRTopKWordQuery
import sys

def compare_by_occurrence(word_count_pair):
		# Sorts the list in descending order
		return word_count_pair[0]
	
def sort_word_list(word_count_pairs):
	word_count_pairs_list = sorted(word_count_pairs, key = compare_by_occurrence, reverse=True)
	return word_count_pairs_list
	
def getTopKWords(word_count_pairs, k):
	word_count_pairs_list = sort_word_list(word_count_pairs)
	
	print("================ K = {} ================".format(k))
	if len(word_count_pairs_list) < k:
		k = len(word_count_pairs_list)
		
	topK_Words_List = word_count_pairs_list[:k]
	for word_count_pair in topK_Words_List:
		print(word_count_pair[1], word_count_pair[0])
		
		
word_count_pairs = []
mr_job = MRTopKWordQuery(args=sys.argv[1:])
with mr_job.make_runner() as runner:
	runner.run()
	for key, word_count_pair in mr_job.parse_output(runner.cat_output()):
		word_count_pairs.append(word_count_pair)
	

	getTopKWords(word_count_pairs, 10)
	getTopKWords(word_count_pairs, 20)