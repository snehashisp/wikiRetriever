import multiprocessing
import threading
from collections import deque
import parser
import index
import os
import time

debug = True

class IndexCreatorProcess(multiprocessing.Process):

	def __init__(self, index_loc = "./"):
		super().__init__()
		self.shard_index_loc = index_loc
		self.queue = multiprocessing.Queue()

	def _insertPage(self, page):
		queue, i = self._index_dict.setdefault(page.shard_no, (deque(), 
						index.ShardIndex(page.shard_no, self.shard_index_loc)))
		queue.append(page)

	def _getPage(self):
		for q, i in self._index_dict.values():
			if len(q):
				return (q.popleft(), i)
		return (None, None)

	def run(self):
		page = self.queue.get()
		self._index_dict = {}
		wiki_parser = parser.WikiParser()

		close_queue = False
		ct, it, c = 0,0,0
		while True:
			if type(page) == int:
				close_queue = True
			elif page:
				self._insertPage(page)
			page, index = self._getPage()
			if page:
				try:
					if debug:
						start = time.time()
						page_index = wiki_parser.createPageIndex(page)
						ct += time.time() - start
						start = time.time()
						index.addPageIndex(page_index)
						it += time.time() - start
						c += 1
					else:
						page_index = wiki_parser.createPageIndex(page)
						index.addPageIndex(page_index)

					if page.final:
						if debug:
							print("CAVG", ct/c, "IAVG", it/c)
							ct, it, c = 0,0,0
							print("Final", page.id, page.shard_no)
						index.writeIndex()
						self._index_dict.pop(page.shard_no)
						index = None
				except Exception as e:
					#print("Errror", page.id)
					# raise e
					pass
			elif close_queue and not page:
				break

			if not close_queue:
				try:
					page = self.queue.get_nowait()
				except:
					page = None

		for shard_no, index in self._index_dict.items():
			print(shard_no, len(index[0]))
			index = index[1]
			index.writeIndex()
			#self._index_dict.pop(shard_no)
			index = None


class IndexCreator():

	def __init__(self, process_count, index_loc = "./"):
		if not os.path.exists(index_loc):
			os.mkdir(index_loc)
		self.index_loc = index_loc
		self.process_list = [IndexCreatorProcess(index_loc) for i in range(process_count)]
		for process in self.process_list:
			process.start()
		self.shard_count = 0

	def addPage(self, page):
		self.process_list[page.shard_no % len(self.process_list)].queue.put(page)

	def finalize(self):
		for process in self.process_list:
			process.queue.put(1)
			process.join()

