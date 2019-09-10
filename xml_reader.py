from xml.etree.ElementTree import iterparse
from wiki_page import Page
from shard import ShardCreator
from index_creator import IndexCreator, TitleIndexer
import os
import json
import sys
import psutil



class XReader():

	def __init__(self, xml_filename, index_creator, shard_creator, namespace_filter = None):

		self.file = xml_filename
		self.index_creator = index_creator
		self.shard_creator = shard_creator
		self.namespace_filter = [] if namespace_filter is None else namespace_filter
		self.namespaces = {}
		self.newpage = None
		self.title_indexer = TitleIndexer(index_creator.index_loc)

	def _eventHandler(self, event, elem):
		tag = elem.tag[elem.tag.rindex('}')+1:]

		if event == 'start':
			if tag == 'page':
				self.newpage = Page()
			elif tag == 'namespace':
				if elem.text not in self.namespace_filter:
					self.namespaces[str(elem.attrib['key'])] = elem.text				
		elif event == 'end' and self.newpage:
			if tag == 'id' and self.newpage.id is None:
				self.newpage.id = int(elem.text)
				#self.title_dict[self.newpage.id] = self.newpage.title
			elif tag == 'ns':
				if self.namespaces.get(elem.text, False) == False:
					self.newpage = None
				else: 
					self.newpage.namespace = self.namespaces[elem.text]
			elif tag == 'title':
				self.newpage.title = elem.text
				#self.title_dict[self.newpage.id] = elem.text
			elif tag == 'text':
				self.newpage.text = elem.text
			elif tag == 'page':
				if self.newpage:
					#print(self.newpage.namespace, self.newpage.title, self.newpage.id)
					self.shard_creator.add(self.newpage)
					#print(self.newpage.shard_no)
					self.index_creator.addPageInplace(self.newpage)
					self.title_indexer.addTitle(self.newpage.id, self.newpage.title)
			elem.clear()

	def iterParse(self, mem_limit = 80):

		for event, elem in iterparse(self.file, ('start','end')):
			self._eventHandler(event, elem)
		self.shard_creator.cleanup()
		self.index_creator.finalize()
		self.title_indexer.storeIndex()

if __name__ == "__main__":

	index_loc = sys.argv[2]
	if index_loc[-1] != '/':
		index_loc += '/'
	dump_loc = sys.argv[1] 
	sharder = ShardCreator(1, 100000000, file_dir = './Shards/')
	indexer = IndexCreator(1, index_loc = index_loc)
	reader = XReader(dump_loc, indexer, sharder)
	reader.iterParse()

