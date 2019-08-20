import threading
from collections import deque
import time
import sys
import os

class ShardThread():

	def __init__(self, init_shard_no, file_buffer_size = 0, file_dir = "./"):

		self.shard_size = 0
		self.current_shard_no = init_shard_no
		self._shard_file = None
		self.queue = deque()
		self.file_dir = file_dir
		self.file_buffer_size = file_buffer_size
		self._createShard()

		self._thread_run = True
		self._thread = threading.Thread(target = self.run)
		self._thread.start()


	def _createShard(self):
		self._shard_file = open(self.file_dir + "shard-" + str(self.current_shard_no) + ".json", 'w',
			self.file_buffer_size)
		self._shard_file.write("{")

	def _closeShard(self):
		self._shard_file.seek(0, os.SEEK_END)
		self._shard_file.seek(self._shard_file.tell() - 1,os.SEEK_SET)
		self._shard_file.write("}")
		self._shard_file.close()

	def addPage(self, page):
		self.queue.append(page)
		self.shard_size += page.getSize()

	def run(self):

		while self._thread_run or len(self.queue) > 0:

			if len(self.queue) == 0:
				time.sleep(0)
			else:
				page = self.queue.popleft()
				if page.shard_no > self.current_shard_no:
					self.current_shard_no = page.shard_no
					self._closeShard()
					self._createShard()

				self._shard_file.write(page.formatJSON()[1:-1] + ",")

	def delThread(self):

		self._thread_run = False
		self._thread.join()
		self._closeShard()

	def __delete__(self):
		self.delThread()


class ShardCreator():

	def __init__(self, thread_count, max_shard_size, file_buffer_size = -1, file_dir = "./"):

		if not os.path.exists(file_dir):
			os.mkdir(file_dir)
		self._shard_counter = thread_count
		self._shard_threads = [ShardThread(i, file_buffer_size, file_dir) for i in range(self._shard_counter)]
		self._current_thread = 0
		self._max_shard_size = max_shard_size

	def add(self, page):

		if self._shard_threads[self._current_thread].shard_size + page.getSize() > self._max_shard_size:
			page.shard_no = self._shard_counter
			self._shard_threads[self._current_thread].shard_size = 1
			self._shard_counter += 1
		else:
			page.shard_no = self._shard_threads[self._current_thread].current_shard_no

		self._shard_threads[self._current_thread].addPage(page)
		self._current_thread = (self._current_thread + 1) % len(self._shard_threads)

	def cleanup(self):

		for sh in self._shard_threads:
			sh.delThread()

	def __delete__(self):
		self.cleanup()