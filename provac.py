from lxml import etree
import os
from multiprocessing import Process, Queue

class VacancyParser:

	v = 0

	def fast_iter(self, context, func, queue):
		for event, elem in context:
			func(event, elem, queue)
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
			#print "Progress: %s to ucto" % id

	path = []
	def process_element_read(self, event, elem, queue):
			if event == 'start':
				tag = elem.tag.rstrip()
				val = elem.text
			
				if val is not None:
					val = val.encode('utf-8').rstrip()
	
					if tag == 'id':
						self.id = val
		
					if tag == 'sbi':
						self.sbi = val
						if val == "":
							self.sbi = "0"
			
					if tag == 'fulltxt' and self.id is not None and self.sbi is not None:
						queue.put((self.id, self.sbi, val))
						#print "[%s/%s] %s" % (self.id, self.sbi, val)
						self.id = None
						self.sbi = None
						
						
	
					if tag == 'vac':
						self.v = self.v + 1
						#print "Progress: %d" % self.v

		



wQ = Queue()
wP = []

vp = VacancyParser()

threads = 28

for i in range(threads):
	p = Process(target=vp.process_element_write, args=(i,wQ,))
	wP.append(p)
	p.start()
	

#context = etree.iterparse( "vacs.xml", events=("start", "end"))
context = etree.iterparse( "../jobfeed.2011.uniq.xml", events=("start", "end"))
rP = Process(target=vp.fast_iter, args=(context,vp.process_element_read, wQ,))
rP.start()
rP.join()

print "Done reading the XML file"

for _ in wP:
	wQ.put((None, None, None))

for p in wP:
	p.join()

print "Processed %d vanancies" % vp.v
