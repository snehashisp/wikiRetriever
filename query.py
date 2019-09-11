from wiki_page import Page
from index import ShardIndex
from parser import TermsCreator
from cache import IndexCache, TitleCache
import json
import os
import math
import functools
import nltk
import re
import sys
import Stemmer

class Response():

	def __init__(self):
		self.text = {}
		self.title = {}
		self.categories = {}
		self.reference = {}
		self.infobox = {}
		self.links = {}
		self.shard_map = {}

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

	def rankFrequency(self, tl = 4, ct = 2.5, tx = 3, inf = 2, ln = 2, ref = 2):
		page_list = []
		order = [tl, ct, inf, ln, ref]
		for i, d in enumerate([self.title, self.categories, self.infobox,
					self.links, self.reference]):
			for k, v in d.items():
				page_list += [(order[i]*-1*v,-1*v,k)]
		for k, v in self.text.items():
			term_count = 0
			for tc in v:
				term_count += tc
			page_list += [(tx*-1*len(v), -1*term_count, k)]
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

	def __init__(self, index_location, cache_size = 1):

		self.index_cache = IndexCache(index_location, cache_size)
		stemmer = Stemmer.Stemmer('english').stemWords
		stopwords = nltk.corpus.stopwords.words('english')
		self.term_creator = TermsCreator(stopwords, stemmer)
		self.title_cache = TitleCache(index_location, cache_size = 4)

	def _getIndexResponse(self, wordList, fields = 'icretob'):

		resp = Response()
		for word in wordList:
			#print(word)
			page_list = self.index_cache.getWordPosting(word)
			doc_id = 0
			document_freq = len(page_list)
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
						resp.text[doc_id] += [page[2:][0]/document_freq]	
				elif 'b' in fields:
					resp.text.setdefault(doc_id, []) 
					resp.text[doc_id] += [page[1:][0]/document_freq]
				#resp.shard_map[doc_id] = shard_no

		return resp

	def queryIndex(self, qstring, fields = 'icretob'):
		query_terms = self.term_creator.generateTerms(qstring, group_size = 1, stem = True)
		return self._getIndexResponse(query_terms, fields)

	def getQueryResults(self, query, results = 10):
		resp = self.queryIndex(query).rankFrequency()[:results]
		#print(resp)
		title_resp = []
		for response in resp:
			title_resp += [self.title_cache.getTitle(str(response))]
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
		tl,ct,tx,inf,ln,ref = 1, 1, 1, 1, 1, 1
		for field, query in self._getFeildAndValues(query):
			if field in 'title':
				fields += 't'
				tl = 5
			if field in 'body':
				fields += 'b'
				tx = 3
			if field in 'category':
				fields += 'c'
				ct = 2.5
			if field in 'ref':
				fields += 'r'
				ref = 2
			if field in 'infobox':
				fields += 'i'
				inf = 2
			if fields in 'external':
				fields += 'e'
				ln = 2
			qstring += query + " "


		resp = self.queryIndex(qstring, fields).rankFrequency(tl = tl, ct = ct, 
			tx = tx, ref = ref, inf = inf, ln = ln)[:results]
		for response in resp:
			title_resp += [self.title_cache.getTitle(str(response))]
		return title_resp


if __name__ == "__main__":

	index_loc = sys.argv[1]
	if index_loc[-1] != '/':
		index_loc += "/"

	if '-f' in sys.argv:
		infile = sys.argv[sys.argv.index('-f') + 1]
		with open(infile, 'r') as fp:
			queries = fp.read().strip().split('\n')
	else:
		queries = [sys.argv[2]]

	if '-o' in sys.argv:
		outfile = sys.argv[sys.argv.index('-o') + 1]
		fp = open(outfile, 'w')
		printFunc = fp.write
	else:
		printFunc = print

	results_count = 10
	if '-n' in sys.argv:
		results_count = int(sys.argv[sys.argv.index('-n') + 1])

	searcher = Query(index_loc)
	for query in queries:
		print(query)
		if ":" in query:
			results = searcher.getFieldQueryResults(query, results = results_count)
		else:
			results = searcher.getQueryResults(query, results = results_count)
		printFunc("\n" + "\n".join(results) + "\n")

	if '-o' in sys.argv:
		fp.close()

	# with open(query_file, 'r') as fp:
	# 	queries = fp.read()
	# 	searcher = Query(index_loc)
	# 	with open(output_File, 'w') as wfp:
	# 		for query in queries.strip().split('\n'):
	# 			print(query)
	# 			if ":" in query:
	# 				results = searcher.getFieldQueryResults(query, results = 1)
	# 			else:
	# 				results = searcher.getQueryResults(query)
	# 			wfp.write("\n" + "\n".join(results) + "\n")
	# searcher = Query("./ind/")
	# print(searcher.getQueryResults("new york mayor"))
	# searcher.printQueryResults("new york mayor")
	# print("\n")
	# searcher.printQueryResults("war")
	# print("\n")
	# searcher.printFieldQueryResults("title:gandhi body:arjun infobox:gandhi category:gandhi ref:gandhi")
	# #print(res, shard)
	# #shard_cache = ShardCache(8,"./Shards/")
	# #for page in res[:10]:
	# #	print(shard_cache.getPageTitle(shard[page[0]], page[0]))		





