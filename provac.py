from lxml import etree
import os
import sys
from multiprocessing import Process, Queue
from colorama import init, Fore, Back, Style
import getopt

class VacancyParser:
	use_ucto = True
	extract = True
	default_sbi = "nosbi"
	threads = 24
	input = ""

	def printHelp(self):
		print "Vacancy Parser v0.1 -- louis@naiaden.nl\n"
		print "Extracts vacancies from an xml file, and flattens them into tokenised files"
		print "Arguments for provac.py"
		print "-h, --help                   Prints this help and exits"
		print "-d, --dir <string>           The output directory. Default the files are saved in a directory corresponding to their sbi"
		print "-U, --no-ucto                Skip the tokenisation step"
		print "-E, --no-extract             Skip the extraction step"
		print "-s, --default-sbi <string>   The value for unknown sbi. Default is nosbi (not used in combination with -d)"
		print "-t, --threads <n>            Number of threads. Default is 24"
		print "-f, --input <string>         The input xml file"

	def __init__(self, cmdArgs):
		try:
			opts, args = getopt.getopt(cmdArgs, 'hd:UEs:t:f:', ['help', 'dir=', 'no-ucto', 'no-extract', 'default-sbi=', 'threads=', 'input='])
		except getopt.GetoptError:
			self.printHelp()
			sys.exit(2)
			
		for (opt, arg) in opts:
			if opt in ('-h', '--help'):
				self.printHelp()
				sys.exit()
			if opt in ('-d', '--dir'):
				self.dir = arg
			if opt in ('-U', '--no-ucto'):
				self.use_ucto = False
			if opt in ('-E', '--no-extract'):
				self.extract = False
			if opt in ('-s', '--default-sbi'):
				self.extract = False
			if opt in ('-t', '--threads'):
				self.threads = arg
			if opt in ('-f', '--input'):
				self.input = arg
		
		init(autoreset=True)
		
		if not self.input:
			print "No input is provided. Program halts"

	def fast_iter(self, context, func, queue):
		for event, elem in context:
			func(elem, queue)
			elem.clear()
			while elem.getprevious() is not None:
				del elem.getparent()[0]
		del context

	def process_element_write(self, pId, queue):
		while True:
			(id, sbi, text) = queue.get()
			if not id and not sbi and not text:
				return
			
			if self.extract:
				f = open("%s/%s.utxt" % (sbi, id), 'w')
				f.write(text)
				f.close()
			if self.use_ucto:
				os.system("ucto -s \"\" -P -S -L nl %s/%s.utxt 2>/dev/null | tr -s ' ' > %s/%s.txt" % (sbi, id, sbi, id))

	def process_element_read(self, elem, queue):

			tag = elem.tag.rstrip()
			val = elem.text
		
			if val is not None:
				val = val.encode('utf-8').rstrip()

				if tag == 'id':
					self.id = val
	
				if tag == 'sbi':
					self.sbi = val
		
				if tag == 'fulltxt' and self.id is not None:
					if self.sbi == "" or self.sbi is None:
						self.sbi = "nosbi"					
					
					queue.put((self.id, self.sbi, val))
					#print Fore.GREEN + "Processed [%s] %s" % (self.id, self.sbi)
					
					self.id = None
					self.sbi = None					
					
				elif tag == 'fulltxt':
					print Fore.RED + "Cannot process [%s] %s" % (self.id, self.sbi)
					
	def run(self):
		wQ = Queue()
		wP = []
		
		for i in range(vp.threads):
			p = Process(target=vp.process_element_write, args=(i,wQ,))
			wP.append(p)
			p.start()
		
		context = etree.iterparse( "../jobfeed.2011.uniq.xml")
		rP = Process(target=vp.fast_iter, args=(context,vp.process_element_read, wQ,))
		rP.start()
		rP.join()

		print "Done reading the XML file"

		for _ in wP:
			wQ.put((None, None, None))

		for p in wP:
			p.join()

		print "Done!"


vp = VacancyParser(sys.argv[1:])
vp.run()


	


