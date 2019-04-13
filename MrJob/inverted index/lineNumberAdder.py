import sys

args=sys.argv[1:]
input_file_path = args[0]
line_number = 0
output_file = args[1]
with open(input_file_path, 'rb') as text_file:
	with open(output_file, 'w') as output_file:
		for line in text_file:
			line_number+=1
			output_file.write("{}\t{}".format(line_number,line.decode()))
