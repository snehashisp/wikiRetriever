import index
import bisect
import math

class ShardCache():

	def __init__(self, cache_size, shard_loc):
		self.cache_size = cache_size
		self.current_size = 0
		self.shards = {}
		self.shard_counter = {}
		self.shard_loc = shard_loc
		self._max_counter = 0

	def _getReplacement(self):

		minc = math.inf
		mink = None
		for k, v in self.shard_counter.items():
			if v < minc:
				mink = k
				minc = v
		return mink

	def _getShard(self,shard_no):
		with open(self.shard_loc + "shard-" + str(shard_no) + ".json", 'r') as fp:
			shard = json.load(fp)
		return shard

	def getShard(self, shard_no):
		if shard_no not in self.shards:
			if len(self.shards) >= self.cache_size:
				rep = self._getReplacement()
				self.shard.pop(rep)
				self.shard_counter.pop(rep)
			self.shards[shard_no] = self._getShard(shard_no)

		self.shard_counter[shard_no] = self._max_counter 
		self._max_counter += 1
		return self.shards[shard_no]

	def getPageTitle(self, shard_no, page_no):
		shard = self.getShard(shard_no)
		return shard[str(page_no)]["title"]



class IndexCache():

	def __init__(self, index_loc, cache_size = 4):
		self.cache_size = cache_size
		self.current_size = 0
		self.index_map = {}
		self.index_counter = {}
		self.index_loc = index_loc
		self._max_counter = 0		
		with open(self.index_loc + "index-searcher", 'r') as fp:
			self.index_list = fp.read().split('\n')[:-1]

	def _loadIndex(self, index_no):
		shard_index = index.ShardIndex(index_no, self.index_loc)
		shard_index.readIndex()
		self.index_map[index_no] = shard_index

	def _getReplacement(self):
		minc = math.inf
		mink = None
		for k, v in self.index_counter.items():
			if v < minc:
				mink = k
				minc = v
		return mink

	def _getWordIndex(self, word):
		index_no = bisect.bisect_left(self.index_list, word)
		if index_no not in self.index_map:
			if len(self.index_map) >= self.cache_size:
				rep = self._getReplacement()
				self.index_map.pop(rep)
				self.index_counter.pop(rep)
			self._loadIndex(index_no)

		self.index_counter[index_no] = self._max_counter
		self._max_counter += 1
		return self.index_map[index_no]

	def getWordPosting(self, word):
		index = self._getWordIndex(word)
		return index.index.get(word, [])

class TitleCache():

	def __init__(self, index_loc, cache_size = 8):
		self.cache_size = cache_size
		self.title_map = {}
		self.title_counter = {}
		self.title_loc = index_loc
		self._max_counter = 0	
		with open(self.index_loc + "title-searcher", 'r') as fp:
			self.title_list = fp.read().split('\n')[:1]

	def _getReplacement(self):

		minc = math.inf
		mink = None
		for k, v in self.title_counter.items():
			if v < minc:
				mink = k
				minc = v
		return mink

	def _loadTitle(self, title_no):
		with open(self.title_loc + 'title-index-' + str(title_no), 'r') as fp:
			self.title_map[title_no] = json.load(fp)

	def _getTitle(self, title_no, doc_id):
		if title_no not int self.title_map:
			if len(self.title_map) >= self.cache_size:
				rep = self._getReplacement()
				self.title_map.pop(rep)
				self.title_counter.pop(rep)
			self._loadTitle(title_no)
		self.title_counter[title_no] = self._max_counter
		self._max_counter += 1
		return self.title_map[title_no].get(doc_id, "")

	def getTitle(self, doc_id):
		title_no = bisect.bisect_left(self.title_list, doc_id)
		return self._getTitle(title_no, doc_id)


if __name__ == "__main__":
	icache = IndexCache("./ind/",2)
	print(icache.getWordPosting('gandhi'))
	# print(icache.getWordPosting('austria'))
	# print(icache.getWordPosting('date'))
	# print(icache.getWordPosting('paddick'))

