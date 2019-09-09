import index
import os
import json
import heapq
import re

class IndexMerger():

	def __init__(self, i_index_loc = "./", index_size = 10000000):
		if os.path.exists(i_index_loc):
			files = os.listdir(i_index_loc)
		self.file_pointer = []
		self.index_size = index_size
		self.word_index_loc = {}
		self.index_loc = i_index_loc
		self.final_index_count = None
		for file in files:
			if 'i-index' in file:
				self.file_pointer += [open(self.index_loc + file,'r', encoding = 'utf-8')]

	def mergeIndices(self):
		posting = {}
		heap = []
		current_index = 0

		for i, fp in enumerate(self.file_pointer):
			text = json.loads(fp.readline())
			word, data = list(text.items())[0]
			posting[(word, i)] = data
			heapq.heappush(heap, (word, i))

		shard_index = index.ShardIndex(current_index, self.index_loc)
		current_size, last_word = 0,""
		while len(heap) > 0:
			
			word, file = heapq.heappop(heap)
			posting_list = posting[(word, file)]
			posting.pop((word, file))
			size = len(str(posting_list))
			
			if re.search(r'^[a-z][a-z0-9]+',word):
				if size + current_size > self.index_size and word != last_word:
					shard_index.writeIndex()
					current_index += 1
					current_size = 0
					shard_index = index.ShardIndex(current_index, self.index_loc)

				shard_index.mergePosting(word, posting_list)
				self.word_index_loc[current_index] = word
				last_word = word
				current_size += size
				print(word)

			fp = self.file_pointer[file]
			text = fp.readline()
			if text != '':
				text = json.loads(text)
				word, data = list(text.items())[0]
				posting[(word, file)] = data
				heapq.heappush(heap, (word, file))
			else:
				print('Done with', fp.name)
				fp.close()
				#os.unlink(fp.name)
		shard_index.writeIndex()
		self.final_index_count = current_index

	def storeIndexSearcher(self):
		with open(self.index_loc + 'index-searcher','w', encoding = 'utf-8') as fp:
			for i in range(self.final_index_count + 1):
				fp.write(self.word_index_loc[i] + "\n")




if __name__ == "__main__":
	merger = IndexMerger("/mnt/sda5/index/", index_size = 10000000)
	merger.mergeIndices()
	merger.storeIndexSearcher()




				

