from lxml import etree
import os
from multiprocessing import Process, Queue
from colorama import init, Fore, Back, Style

init(autoreset=True)

class VacancyParser:
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
			
			f = open("%s/%s.utxt" % (sbi, id), 'w')
			f.write(text)
			f.close()
			os.system("ucto -s \"\" -P -S -L nl %s/%s.utxt 2>/dev/null | tr -s ' ' > %s/%s.txt" % (sbi, id, sbi, id))

	path = []
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
						self.sbi = "0"					
					
					queue.put((self.id, self.sbi, val))
					#print Fore.GREEN + "Processed [%s] %s" % (self.id, self.sbi)
					
					self.id = None
					self.sbi = None					
					
				elif tag == 'fulltxt':
					print Fore.RED + "Cannot process [%s] %s" % (self.id, self.sbi)
					

wQ = Queue()
wP = []

vp = VacancyParser()

threads = 24

for i in range(threads):
	p = Process(target=vp.process_element_write, args=(i,wQ,))
	wP.append(p)
	p.start()
	

#context = etree.iterparse( "vacs.xml")
#context = etree.iterparse( "vac.xml", events=("start", "end"))
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
