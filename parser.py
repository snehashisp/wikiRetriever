import nltk
import re
import html
import json
import copy
from wiki_page import Page
from index import PageIndex, ShardIndex
import time
import Stemmer

class TermsCreator():

	def __init__(self, stopwords, stemmer, token_re = r"[a-zA-Z0-9]+"):

		self.regexp = token_re
		self.stopwords = set(stopwords)
		self.stemmer = stemmer

	def generateTerms(self, text, rel_pos = True, group_size = 1, stem = True):
		#stem = False
		pos = 0
		wordlist = re.findall(self.regexp, text)
		words = []
		for word in wordlist:
			word = word.lower()
			if len(word) != 1 and word not in self.stopwords:
				words.append(word)
		if stem:
			words = self.stemmer(words)
		return words

	def getTermMap(self, text, rel_pos = True):

		term_map = {}
		for term in self.generateTerms(text, rel_pos):
			pos_arr = term_map.setdefault(term[0],term[1])
			if type(pos_arr) == list:
				if term[1] != pos_arr[-1]:
					pos_arr.append(term[1] - pos_arr[-1])
			elif pos_arr != term[1]:
				if term[1] != pos_arr:
					term_map[term[0]] = [pos_arr, term[1] - pos_arr]
		return term_map

	def getTermCount(self, text):
		term_map = {}
		for term in self.generateTerms(text):
			term_map.setdefault(term[0],[0])
			term_map[term[0]][0] += 1
		return term_map

class WikiParser():


	def __init__(self):
		#stemmer = nltk.stem.snowball.SnowballStemmer("english")
		stemmer = Stemmer.Stemmer('english').stemWords
		stopwords = nltk.corpus.stopwords.words('english')
		self.term_creator = TermsCreator(stopwords, stemmer)

#========================================================
	def _get_template_terms(self, templ):
		words = ""
		for term in templ.split("|"):
			try:
				typ, data = term.split("=")
				if "url" in typ:
					webl = data.split(".")
					if "www" in webl[0]:
						words += webl[1] + " "
					else:
						words += webl[0].split(":")[1]
				else:
					words += data
			except Exception as e:
				raise e
				pass

		return self.term_creator.generateTerms(words, stem = True)

	def _parsetemplate(self, text, index):
		for template in re.findall('{{.*}}',text):
			try:
				ttype, data = template.split("|",1)
				if 'cite' in ttype:
					index.reference.update(self._get_template_terms(data))
				elif 'Infobox' in ttype:
					index.infobox.update(self._get_template_terms(data))
			except:
				pass

	def _parsedata(self, text, index):
		stext = re.sub(r'{\|.*?(\|})', '',re.sub(r'\n', '', re.sub(r'{{.*}}', '', text)))
		#stext = re.sub(r'{{.*}}', '', text)
		text = stext.split("\n==References==\n")
		index.text_map.update(self.term_creator.getTermCount(text[0]))
		try:
			text = text[1].split("\n==External links==\n")
			index.reference.update(self.term_creator.generateTerms(text[0], stem = True))
			text = text[1].split("\n==Category==\n")
			index.ext_links.update(self.term_creator.generateTerms(text[0], stem = True))
		except:
			pass

	def _parselinksCategories(self, text, index):
		links, categories = "", ""
		for cat_link in re.findall(r'\[\[.*\]\]', text):
			if 'Category:' in cat_link:
				try:
					categories += cat_link.split(":")[1] + " "
				except:
					pass
			else:
				links += cat_link + " "
		index.category.update(self.term_creator.generateTerms(categories, stem = True))
		index.ext_links.update(self.term_creator.generateTerms(links, stem = True))

	def _parseTitle(self, title, index):
		index.title.update(self.term_creator.generateTerms(title))

	def createPageIndex(self, page):
		text = re.sub(r"<[^>]*>", '', html.unescape(page.text))
		#self.data = mwparserfromhell.parse(untagged_text)
		index = PageIndex(page)
		self._parsedata(text, index)
		self._parsetemplate(text, index)
		self._parselinksCategories(text, index)
		self._parseTitle(page.title, index)
		# self._addUserTerms(index)
		# self._addTitle(index)
		# self._addCategories(index)
		# self._addExternalLinks(index)
		# self._addTemplateTerms(index)
		return index

#code for testing
if __name__ == "__main__":

	p1 = Page()
	p1.id = 678
	with open('test', 'r') as fp:
		p1.text = fp.read()
	p1.title = 'Deris Merrick'

	# p2 = Page()
	# p2.id = 690
	# with open('test2', 'r') as fp:
	# 	p2.text = fp.read()
	shIndex = ShardIndex(1)
	wc = WikiParser()
	pindex1 = wc.createPageIndex(p1)
	pindex1.printIndex()
	#pindex2 = wc.createPageIndex(p2)
	#shIndex.addPageIndex(pindex1)
	#shIndex.addPageIndex(pindex2)
	#pindex2.page.id = 640
	#shIndex.addPageIndex(pindex2)
	#index2.page.id = 639
	#shIndex.addPageIndex(pindex2)
	#pindex2.printIndex()
	shIndex.writeIndex()
	shIndex.readIndex()

	print(json.dumps(shIndex.index, indent = 2, ensure_ascii = False))







