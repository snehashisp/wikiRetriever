from wiki_page import Page
from index import ShardIndex
from parser import TermsCreator
import json
import os
import math
import functools
import nltk
import re
import sys
import Stemmer

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

class Response():

	def __init__(self):
		self.text = {}
		self.title = {}
		self.categories = {}
		self.reference = {}
		self.infobox = {}
		self.links = {}
		self.shard_map = {}

	def _separation(self, pos):
		count = 0
		dist = 0
		for i, p1 in enumerate(pos):
			for p2 in  pos[i:]:
				dist += abs(p2 - p1)
				count += 1
		return dist / count

	def _getAvgSeparation(self, word_list):
		word_set = set()
		unique = len(word_list)
		print(word_list)
		for wi, words in enumerate(word_list):
			pos = 0
			for wpos in words:
				if type(wpos) == int:
					pos += wpos
					word_set.add((wi, pos))

		word_list = sorted(list(word_set), key = lambda x: x[1])
		print(word_list)
		word_dict = {}
		min_sep = math.inf
		for i in range(unique):
			word_dict[word_list[i][0]] = word_list[i][1]
		for i in range(unique, len(word_list)):
			if len(word_dict.keys()) == unique:
				min_sep = min(self._separation(list(word_dict.values())), min_sep)
			word_dict.pop(word_list[i-unique][0])
			word_dict[word_list[i][0]] = word_list[i][1]
		if len(word_dict.keys()) == unique:
			min_sep = min(self._separation(list(word_dict.values())), min_sep)
		return min_sep

	def _rankFunc(self, p1, p2):
		if p1[1] == p2[1]:
			return p1[2] - p1[2]
		return -1 * (p1[1] - p2[1])

	def _rankdocs(self, doc_dict):
		doc_list = []
		for k, d in doc_dict.items():
			match_terms = len(d)
			avg_separation = self._getAvgSeparation(d) if match_terms > 1 else 0
			doc_list += [(k, match_terms, avg_separation)]
		return sorted(doc_list, key = functools.cmp_to_key(self._rankFunc))

	def _freqComp(self, p1, p2):
		if p1[0] == p2[0]:
			if p1[1] == p2[1]:
				return p1[2] - p2[2]
			return p1[1] - p2[1]
		return p1[0] - p2[0]

	def rankFrequency(self, tl = 0, ct = 1, tx = 2, inf = 3, ln = 4, ref = 5):
		page_list = []
		order = [tl, ct, inf, ln, ref]
		for i, d in enumerate([self.title, self.categories, self.infobox,
					self.links, self.reference]):
			for k, v in d.items():
				page_list += [(order[i], -1*v, k)]
		for k, v in self.text.items():
			term_count = 0
			for tc in v:
				term_count += tc[0]
			page_list += [(tx, -1*len(v), -1*term_count, k)]
		rankedp = sorted(page_list, key = functools.cmp_to_key(self._freqComp))
		#print(rankedp)
		pages = set()
		ranked_pages = []
		for page in rankedp:
			kloc = 2 if len(page) == 3 else 3
			if page[kloc] not in pages:
				ranked_pages += [page[kloc]]
				pages.add(page[kloc])
		return ranked_pages

	def merge(self, response):
		self.text.update(response.text)
		self.title.update(response.title)
		self.categories.update(response.categories)
		self.reference.update(response.reference)
		self.infobox.update(response.infobox)
		self.links.update(response.links)
		self.shard_map.update(response.shard_map)

class Query():

	def __init__(self, index_location):

		self.total_indexes = len(os.listdir(index_location)) - 1
		#self.total_indexes = 2
		self.shard_indexes = [ShardIndex(i, index_location) for i in range(self.total_indexes)]
		for index in self.shard_indexes:
			index.readIndex()
		print("Indexes Loaded")
		#print(self.shard_indexes[0].index)
		#stemmer = nltk.stem.snowball.SnowballStemmer("english")
		stemmer = Stemmer.Stemmer('english').stemWord
		stopwords = nltk.corpus.stopwords.words('english')
		self.term_creator = TermsCreator(stopwords, stemmer)
		with open(index_location + "title-index.json", 'r') as fp:
			self.title_index = json.load(fp)

	def _getShardResponse(self, wordList, shard_no, fields = 'icretob'):

		resp = Response()
		for word in wordList:
			page_list = self.shard_indexes[shard_no].index.get(word, [])
			doc_id = 0
			for page in page_list:
				doc_id += page[0]
				if type(page[1]) != int:
					if 't' in page[1] and 't' in fields:
						resp.title[doc_id] = resp.title.get(doc_id, 0) + 1
					if 'i' in page[1] and 'i' in fields:
						resp.infobox[doc_id] = resp.infobox.get(doc_id, 0) + 1
					if 'c' in page[1] and 'c' in fields:
						resp.categories[doc_id] = resp.categories.get(doc_id, 0) + 1
					if 'r' in page[1] and 'r' in fields:
						resp.reference[doc_id] = resp.reference.get(doc_id, 0) + 1
					if 'e' in page[1] and 'e' in fields:
						resp.links[doc_id] = resp.links.get(doc_id, 0) + 1
					if 'b' in fields and len(page) > 2:
						resp.text.setdefault(doc_id, []) 
						resp.text[doc_id] += [page[2:]]	
				elif 'b' in fields:
					resp.text.setdefault(doc_id, []) 
					resp.text[doc_id] += [page[1:]]
				resp.shard_map[doc_id] = shard_no

		return resp

	def queryALlIndex(self, qstring, fields = 'icretob'):
		query_terms = list(map(lambda x:x[0], 
			self.term_creator.generateTerms(qstring, group_size = 1, stem = False)))
		resp = Response()
		for i in range(self.total_indexes):
			resp.merge(self._getShardResponse(query_terms, i, fields))
		return resp

	def getQueryResults(self, query, results = 10):
		resp = self.queryALlIndex(query).rankFrequency()[:results]
		title_resp = []
		for response in resp:
			title_resp += [self.title_index[str(response)]]
		return title_resp

	def _getFeildAndValues(self, query):
		split_query = query.split(":")
		i = 1
		field = split_query[0]
		while i < len(split_query):
			if i == len(split_query) - 1:
				yield (field, split_query[i])
			else:
				next_q = split_query[i].rsplit(" ",1)
				yield (field, next_q[0])
				field = next_q[1]
			i += 1

	def getFieldQueryResults(self, query, results = 2):
		qstring = ""
		fields = ''
		title_resp = []
		for field, query in self._getFeildAndValues(query):
			if field in 'title':
				fields += 't'
			if field in 'body':
				fields += 'b'
			if field in 'category':
				fields += 'c'
			if field in 'ref':
				fields += 'r'
			if field in 'infobox':
				fields += 'i'
			qstring += query + " "
		resp = self.queryALlIndex(qstring, fields).rankFrequency()[:results]
		for response in resp:
			title_resp += [self.title_index[str(response)]]
		return title_resp


if __name__ == "__main__":

	index_loc = sys.argv[1]
	if index_loc[-1] != '/':
		index_loc += "/"
	query_file = sys.argv[2] 
	output_File = sys.argv[3]

	with open(query_file, 'r') as fp:
		queries = fp.read()
		searcher = Query(index_loc)
		with open(output_File, 'w') as wfp:
			for query in queries.strip().split('\n'):
				if ":" in query:
					results = searcher.getFieldQueryResults(query, results = 1)
				else:
					results = searcher.getQueryResults(query)
				wfp.write("\n" + "\n".join(results) + "\n")
	# searcher = Query("./ind2/")
	# #searcher.printQueryResults("gandhi")
	# searcher.printQueryResults("new york mayor")
	# print("\n")
	# searcher.printQueryResults("war")
	# print("\n")
	# searcher.printFieldQueryResults("title:gandhi body:arjun infobox:gandhi category:gandhi ref:gandhi")
	# #print(res, shard)
	# #shard_cache = ShardCache(8,"./Shards/")
	# #for page in res[:10]:
	# #	print(shard_cache.getPageTitle(shard[page[0]], page[0]))		





