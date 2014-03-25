from lxml import etree
import os
import sys
import time
from multiprocessing import Process, Queue
from colorama import init, Fore, Back, Style
import getopt
 
class TweNCParser:
	use_ucto = True
	extract = True
	default_source = "tnc"
	threads = 28
	input_dir = ""
	output_dir = ""

	def printHelp(self):
		print "Twente News Corpus Parser v0.1 -- louis@naiaden.nl\n"
		print "Extracts articles and texts from xml files, and flattens them into tokenised files"
		print "Arguments for pronew.py"
		print "-h, --help                     Prints this help and exits"

		print "-D, --outputdir <string>       The output directory. Default the files are saved in a directory corresponding to their source"
		print "-U, --no-ucto                  Skip the tokenisation step"
		print "-E, --no-extract               Skip the extraction step"
		print "-s, --default-source <string>  The value for unknown sources. Default is tnc (not used in combination with -d)"
		print "-t, --threads <n>              Number of threads. Default is 24"
		print "-d, --inputdir <string>        The input directory"

	def __init__(self, cmdArgs):
#		try:
#			opts, args = getopt.getopt(cmdArgs, 'hcD:UEs:t:d:', ['help', 'concatenate', 'outputdir=', 'no-ucto', 'no-extract', 'default-sbi=', 'threads=', 'inputdir='])
#		except getopt.GetoptError:
#			self.printHelp()
#			sys.exit(2)
#			
#		for (opt, arg) in opts:
#			if opt in ('-h', '--help'):
#				self.printHelp()
#				sys.exit()
#			if opt in ('-D', '--outputdir'):
#				self.output_dir = arg
#			if opt in ('-U', '--no-ucto'):
#				self.use_ucto = False
#			if opt in ('-E', '--no-extract'):
#				self.extract = False
#			if opt in ('-s', '--default-sbi'):
#				self.extract = False
#			if opt in ('-t', '--threads'):
#				self.threads = arg
#			if opt in ('-d', '--inputdir'):
#				self.input_dir = arg
		
		init(autoreset=True)
		
#		if not self.input_dir:
#			print "No input directory is provided. Program halts"




        def process_file_write(self, pId, queue):
            while True:
                file = queue.get()
                if file is None:
                    return
                print Fore.CYAN + "Processing %s (%d)" % (file, queue.qsize())
                (basename, ext) = (file.split('/')[-1]).split('.')
                (source, date) = (basename[:-8], basename[-8:100])

                context = etree.iterparse(file)

                if not os.path.exists(source):
                    os.makedirs(source)

                if self.extract:
                    f = open("%s/%s.utxt" % (source, date), 'w')
                    
		    for event, elem in context:
                        tag = elem.tag.rstrip()
                        val = elem.text
                            
                        if tag == 'p' and val is not None:
                            f.write(val.encode('utf-8') + ' ')

                        elem.clear()
		        while elem is not None and elem.getprevious() is not None and elem.getparent() is not None:
		    		del elem.getparent()[0]
		    del context

                    f.close()
                if self.use_ucto:
                    os.system("ucto -s \"\" -P -S -L nl %s/%s.utxt 2>/dev/null | tr -s ' ' > %s/%s.txt" % (source, date, source, date))


	def run(self):
                program_start = time.time()

		wQ = Queue()
                wQ.cancel_join_thread()
		wP = []
		
		for i in range(tp.threads):
			p = Process(target=tp.process_file_write, args=(i,wQ,))
			wP.append(p)
			p.start()

                # 10000 files enter here
                sys.stdout.write(Fore.GREEN + "Reading from stdin...")
                input_files = sys.stdin.readlines()
                for input_file in input_files:
                        wQ.put(input_file.rstrip())
                print Fore.GREEN + "\rDone reading from stdin. I found %d files." % (wQ.qsize())

		for _ in wP:
			wQ.put(None)

		for p in wP:
			p.join()

                program_stop = time.time()
		print Fore.GREEN + "Processed %d files in %f seconds (%fs avg)" % (len(input_files), program_stop-program_start, (program_stop-program_start)/len(input_files))


tp = TweNCParser(sys.argv[1:])
tp.run()


	


