import nltk
import mwparserfromhell
import re
import html
import json
from wiki_page import Page
from index import PageIndex, ShardIndex

class TermsCreator():

	def __init__(self, stopwords, stemmer, token_re = "\w+"):

		self.regexp = token_re
		self.stopwords = stopwords
		self.stemmer = stemmer

	def generateTerms(self, text, rel_pos = False, group_size = 1):
		pos = 0
		for t in re.finditer(self.regexp, text):
			l, r = t.span()
			word = text[l:r].lower()
			if len(word) == 1 or word in self.stopwords:
				continue
			term = self.stemmer.stem(word)
			pos += 1
			if not rel_pos:
				yield [term, l // group_size]
			yield [term, pos // group_size]

	def getTermMap(self, text, rel_pos = True):

		term_map = {}
		for term in self.generateTerms(text, rel_pos):
			pos_arr = term_map.setdefault(term[0],term[1])
			if type(pos_arr) == list:
				pos_arr.append(term[1] - pos_arr[-1])
			elif pos_arr != term[1]:
				term_map[term[0]] = [pos_arr, term[1] - pos_arr]
		return term_map


class WikiParser():

	def _addUserTerms(self, index):
		unlinktext = mwparserfromhell.parse(re.sub("\[\[|\]\]",'',str(self.data))).strip_code()
		text = unlinktext.split("\nReferences\n")
		index.text_map.update(self.term_creator.getTermMap(text[0]))
		try:
			text = text[1].split("\nExternal links\n")
			index.reference.update(list(map(lambda x: x[0], self.term_creator.generateTerms(text[0]))))
			text = text[1].split("\nCategory\n")
			index.ext_links.update(list(map(lambda x: x[0], self.term_creator.generateTerms(text[0]))))
		except:
			pass

	def _templateData(self, template):
		data = ""
		for param in template.params:
			if '=' in param:
				d = param.split('=')
				if 'url' in d[0]:
					data += re.sub('^www.', '', d[1]) + " "
				else:
					data += d[1] + " "
			else:
				data += str(param) + " "
		return data

	def _addTemplateTerms(self, index):

		for temp in self.data.ifilter_templates():
			updater = index.others
			if 'Infobox' in temp.name:
				updater = index.infobox
			elif 'cite' in temp.name:
				updater = index.reference
			updater.update(list(map(lambda x:x[0], 
				self.term_creator.generateTerms(self._templateData(temp)))))

	def _addTitle(self, index):
		index.title.update(list(map(lambda x:x[0], 
			self.term_creator.generateTerms(str(self.page.title)))))

	def _addCategories(self, index):
		categories = "" 
		for link in self.data.ifilter_wikilinks():
			if 'Category' in link.title:
				categories += link.title.split(":")[1] + " "
		index.category.update(list(map(lambda x:x[0], 
			self.term_creator.generateTerms(categories))))

	def _addExternalLinks(self, index):
		exts = ""
		for links in self.data.ifilter_external_links():
			exts += re.sub('^www.', '', links.url.split("/")[2])
			if links.title is not None:
				exts += str(links.title)
		index.ext_links.update(list(map(lambda x: x[0], 
			self.term_creator.generateTerms(exts))))

	def __init__(self):
		stemmer = nltk.stem.PorterStemmer()
		stopwords = nltk.corpus.stopwords.words('english')
		self.term_creator = TermsCreator(stopwords, stemmer)

	def createPageIndex(self, page):
		self.page = page
		unescaped_text = re.sub(' +', ' ', html.unescape(page.text))
		untagged_text = re.sub("<[^>]*>", '', unescaped_text)
		self.data = mwparserfromhell.parse(untagged_text)
		index = PageIndex(page)
		self._addUserTerms(index)
		self._addTitle(index)
		self._addCategories(index)
		self._addExternalLinks(index)
		self._addTemplateTerms(index)
		return index

#code for testing
if __name__ == "__main__":

	p1 = Page()
	p1.id = 678
	with open('test', 'r') as fp:
		p1.text = fp.read()
	p1.title = 'Deris Merrick'

	p2 = Page()
	p2.id = 690
	with open('test', 'r') as fp:
		p2.text = fp.read()
	p2.title = 'Ólafur Kristjánsson'
	shIndex = ShardIndex(1)
	wc = WikiParser()
	pindex1 = wc.createPageIndex(p1)
	pindex2 = wc.createPageIndex(p2)
	shIndex.addPageIndex(pindex1)
	shIndex.addPageIndex(pindex2)
	pindex2.page.id = 640
	shIndex.addPageIndex(pindex2)
	pindex2.page.id = 639
	shIndex.addPageIndex(pindex2)
	#pindex2.printIndex()
	shIndex.writeIndex()
	shIndex.readIndex()

	print(json.dumps(shIndex.index, indent = 2, ensure_ascii = False))






