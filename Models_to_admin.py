import os.path

if __name__ == '__main__':
	lines = open(os.path.normpath(os.getcwd()+'\\models_n1.py'), 'r').readlines()
	for line in lines:
		if not line.startswith('class'):
			continue
		line = line.split(' ')[1].split('(')[0]
		print('from .models import ' + line)
	for line in lines:
		if not line.startswith('class'):
			continue
		line = line.split(' ')[1].split('(')[0]
		print('admin.site.register(' + line + ')')