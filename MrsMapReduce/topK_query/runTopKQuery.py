import sys
import shutil, os
import time

def compare_by_occurrence(word_count_pair):
		# Sorts the list in descending order
		return word_count_pair[1]
	
def sort_word_list(word_count_pairs):
	word_count_pairs_list = sorted(word_count_pairs, key = compare_by_occurrence, reverse=True)
	return word_count_pairs_list
	
def getTopKWords(word_count_pairs_list, k):
	
	print("================ K = {} ================".format(k))
	if len(word_count_pairs_list) < k:
		k = len(word_count_pairs_list)
		
	topK_Words_List = word_count_pairs_list[:k]
	for word_count_pair in topK_Words_List:
		print(word_count_pair)
		
		
word_count_pairs = []
	
def main():	
	directory = 'outPut'

	if os.path.exists('outPut'):
		shutil.rmtree('outPut')
	
	filename = sys.argv[1]

	print("starting Mrs Top k query on :", filename)
	start = time.time()	
	os.system('python word_count.py ' + filename + ' outPut')

	f = open("outPut/source_0_split_0_.mtxt", "r")
	words = f.readlines()
	for wordInfo in words:
		key,frequency = wordInfo.split()
		word_count_pairs.append([key, frequency])
	
	word_count_pairs_list = sort_word_list(word_count_pairs)
	
	end = time.time()
	getTopKWords(word_count_pairs_list, 10)
	getTopKWords(word_count_pairs_list, 20)

	
	totalTime = (end - start)
	print("proccess took :",totalTime )


if __name__ == '__main__':
        main()








