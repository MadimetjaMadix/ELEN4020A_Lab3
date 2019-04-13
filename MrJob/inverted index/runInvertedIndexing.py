from invertedIndexing import MRInvertedIndex
import sys
import time

word_line_numbers_pairs = []
t_start = time.clock()
mr_job = MRInvertedIndex(args=sys.argv[1:])
with mr_job.make_runner() as runner:
	runner.run()
	for word, line_numbers in mr_job.parse_output(runner.cat_output()):
		word_line_numbers_pairs.append([word, line_numbers])
	
	t_end = time.clock() - t_start
	
	for distinct_word in word_line_numbers_pairs[:50]:
		print(distinct_word[0], " ", distinct_word[1])#Limit output to 50 as stated in lab brief.
	
	print("Time taken to process: ", t_end)
