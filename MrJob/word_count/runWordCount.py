from word_count import MRWordFrequencyCount
import sys
import time

word_frequency_pairs = []
t_start = time.process_time()
mr_job = MRWordFrequencyCount(args=['-r', 'local', sys.argv[1:]])
with mr_job.make_runner() as runner:
	runner.run()
	for word, frequency in mr_job.parse_output(runner.cat_output()):
		word_frequency_pairs.append([word, frequency])
	
	t_end = time.process_time() - t_start
	
	for distinct_word in word_frequency_pairs:
		print(distinct_word[0], " ", distinct_word[1])#Limit output to 50 as stated in lab brief.
	
	print("Time taken to process: ", t_end)
