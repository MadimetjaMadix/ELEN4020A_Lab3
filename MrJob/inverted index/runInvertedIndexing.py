from invertedIndexing import MRInvertedIndex

word_line_numbers_pairs = []
mr_job = MRInvertedIndex(args=['mediumText.txt'])
with mr_job.make_runner() as runner:
	runner.run()
	for word, line_numbers in mr_job.parse_output(runner.cat_output()):
		word_line_numbers_pairs.append([word, line_numbers])
	
	for distinct_word in word_line_numbers_pairs[:50]:
		print(distinct_word)#Limit output to 50 as stated in lab brief.