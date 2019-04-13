import sys
import shutil, os
import time

word_line_numbers_pairs = []
def getKeyandLineNums(wordInfo):
	firstBracketIndex = wordInfo.find("(")
	lastBracketIndex  = wordInfo.find(")")
	
	wordInfo   = wordInfo[firstBracketIndex + 1:lastBracketIndex]
	word, line_numbers = wordInfo.split(",",1)
	return word, line_numbers
	
def main():	
	directory = 'outPut'

	if os.path.exists('outPut'):
		shutil.rmtree('outPut')
	
	filename = sys.argv[1]

	print("starting Mrs Top k query on :", filename)
	start = time.time()	
	os.system('python invertedIndexing.py ' + filename + ' outPut')

	f = open("outPut/source_0_split_0_.mtxt", "r")
	words = f.readlines()
	for wordInfo in words:
		word, line_numbers = getKeyandLineNums(wordInfo)
		word_line_numbers_pairs.append([word, line_numbers])
	
	end = time.time()
	for distinct_word in word_line_numbers_pairs[:50]:
		print(distinct_word[0], " ", distinct_word[1])#Limit output to 50 as stated in lab brief.

	
	totalTime = (end - start)
	print("proccess took :",totalTime )


if __name__ == '__main__':
        main()

