from wiki_page import Page
from index import ShardIndex
from parser import TermsCreator
import json
import os
import math
import functools

class jsonData():

	def __init__(self, shard_location):
		self.shard_dict = {}
		for file in os.listdir(shard_location):
			self.shard_dict[int(file.split('-')[-1])] = file

	def getPage(pageid, shard_no):
		with open(self.shard_dict[shard_no], 'r') as fp:
			shard = json.load(fp)
		return shard.get(pageid, None)


class Query():

	def __init__(self, index_location):

		self.total_indexes = len(os.listdir(index_location))
		self.shard_indexes = [ShardIndex(i, index_location) for in range(self.total_indexes)]
		for index in self.shard_indexes:
			index.readIndex()
		stemmer = nltk.stem.PorterStemmer()
		stopwords = nltk.corpus.stopwords.words('english')
		self.term_creator = TermsCreator(stopwords, stemmer)

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
		for wi, words in enumerate(word_list):
			pos = 0
			for wpos in words:
				pos += wpos
				word_set.add((wi, pos))

		word_list = sorted(list(word_set), key = lambda x: x[1])
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
			min_sep = min(self._separation(list(word_dict.values()), min_sep))
		return min_sep

	def _getPages(self, wordList, shardno):

		title_docs = {}
		text_docs = {}
		for word in wordList:
			page_list = self.shard_indexes[shard_no].index.get(word, [])
			doc_id = 0
			for page in page_list:
				doc_id += page[0]
				if tpye(page[1]) != int and 't' in page[1]:
					title_docs[doc_id] = title_docs.get(doc_id, 0) + 1
				else:
					text_docs.setdefault(doc_id, []) 
					text_docs[doc_id] += page[1:]
		return title_docs, text_docs

	def _rankFunc(p1, p2):
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


	def queryALlIndex(self, qstring):
		query_terms = list(map(lambda x:x[0], 
			self.term_creator.generateTerms(qstring, group_size = 1)))
		text_dict = {}
		title_dict = {}
		for i in range(self.total_indexes):
			title, text = self._getPages(query_terms, i)
			text_dict.update(text)
			title_dict.update(title)
		res = sorted([(k , c) for k, v in title_dict.items()], key = lambda x:x[1])
		res += self._rankdocs(text_dict)
		return res




