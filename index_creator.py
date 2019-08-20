import index
import parser
import multiprocessing
import threading

class IndexCreator():

	def __init__(self, pool_count, thread_count):
		self.parse_queue = multiprocessing.Queue()
		self.index_queue = multiprocessing.Queue()
		
