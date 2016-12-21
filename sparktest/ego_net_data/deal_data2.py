import random

def random_delete():
	flag = random.randint(0,9)
	if flag == 0:
		return True
	else:
		return False

def read_data(file_name):

	write_file = open(file_name.split('.txt')[0] + '_remain.txt', 'w')
	delete_file = open(file_name.split('.txt')[0] + '_delete.txt', 'w')
	delete_number = 0
	write_number = 0

	with open(file_name) as f:
		for line in f.readlines():
			flag = random_delete()
			if not flag:
				write_file.writelines(line)
			else:
				delete_file.writelines(line)

read_data('facebook_combined.txt')
