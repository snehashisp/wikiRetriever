from xml.etree.ElementTree import iterparse
from wiki_page import Page
from shard import ShardCreator
from index_creator import IndexCreator

class XReader():

	def __init__(self, xml_filename, index_creator, shard_creator, namespace_filter = None):

		self.file = xml_filename
		self.index_creator = index_creator
		self.shard_creator = shard_creator
		self.namespace_filter = [] if namespace_filter is None else namespace_filter
		self.namespaces = {}
		self.newpage = None

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
			elif tag == 'ns':
				if self.namespaces.get(elem.text, False) == False:
					self.newpage = None
				else: 
					self.newpage.namespace = self.namespaces[elem.text]
			elif tag == 'title':
				self.newpage.title = elem.text
			elif tag == 'text':
				self.newpage.text = elem.text
			elif tag == 'page':
				if self.newpage:
					#print(self.newpage.namespace, self.newpage.title, self.newpage.id)
					self.shard_creator.add(self.newpage)
					#print(self.newpage.shard_no)
					self.index_creator.addPage(self.newpage)
			elem.clear()

	def iterParse(self):

		for event, elem in iterparse(self.file, ('start','end')):
			self._eventHandler(event, elem)
		self.shard_creator.cleanup()
		self.index_creator.finalize()

if __name__ == "__main__":

	sharder = ShardCreator(1, 10000000, file_dir = './Shards/')
	indexer = IndexCreator(4, index_loc = "./ind/")
	reader = XReader('wiki.xml', indexer, sharder)
	reader.iterParse()
