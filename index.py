import json
import copy
import re
from dahuffman import HuffmanCodec
from dahuffman.huffmancodec import _EOF
import base64

class PageIndex():

	def __init__(self, page, title = set(), text_map = {}, infobox = set(), 
					category = set(), reference = set(), ext_links = set(), others = set()):
		self.page = copy.deepcopy(page)
		self.text_map = copy.deepcopy(text_map)
		self.infobox = copy.deepcopy(infobox)
		self.category = copy.deepcopy(category)
		self.reference = copy.deepcopy(reference)
		self.ext_links = copy.deepcopy(ext_links)
		self.title = copy.deepcopy(title)
		self.others = copy.deepcopy(others)

	def printIndex(self):
		print("\nText Map\n", self.text_map)
		print("\nInfobox\n", self.infobox)
		print("\nCategory\n", self.category)
		print("\nReference\n", self.reference)
		print("\nExternal\n", self.ext_links)
		print("\nTitle\n", self.title)
		print("\nOthers\n", self.others)

class ShardIndex():

	def __init__(self, shard_no, shard_index_dir = "./"):
		self.shard_no = shard_no
		self.shard_index_dir = shard_index_dir
		self.index = {}
		self.updated = False

	def _encodeHuffman(self, text):
		enc = HuffmanCodec.from_data(text)
		entext = base64.b64encode(enc.encode(text)).decode('utf-8')
		return entext, enc.get_code_table()

	def _decodeHuffman(self, code_table, text):
		dec = HuffmanCodec(code_table)
		return "".join(dec.decode(base64.b64decode(text)))

	def _createEntry(self, page_index, word, positions):
		entry = [page_index.page.id]
		additional = ""
		if word in page_index.infobox:
			additional += "i"
		if word in page_index.category:
			additional += "c"
		if word in page_index.reference:
			additional += "r"
		if word in page_index.ext_links:
			additional += "e"
		if word in page_index.title:
			additional += 't'
		if word in page_index.others:
			additional += 'o'
		if additional != "":
			entry += [additional]
		if type(positions) == int:
			positions = [positions]
		return entry + positions

	def readIndex(self):
		with open(self.shard_index_dir + "shard-index-" + str(self.shard_no), 'r', encoding = 'utf-8') as fp:
			re_sequence = [[r'(,|{)(\w+):', r'\1"\2":'], 
							[r',([icreto]+);', r',"\1";'],
							[r',([icreto]+),', r',"\1",'], 
							[r',([icreto]+)\|', r',"\1"]],'], 
							[r':', ':[['],
							[r';', '],['], 
							[r'\|', ']]']]
			data = json.loads(fp.read(), ensure_ascii = False)
			text, code_table = data['d'], data['t']
			for regex, sub in re_sequence:
				text = re.sub(regex, sub, text)
				code_table = re.sub(regex, sub, code_table)
			code_table = json.loads(code_table)
			code_table[_EOF] = code_table.pop("EOF")
			self.index = json.loads(self._decodeHuffman(code_table, text))
			self.updated = True

	def _addEntry(self, word, entry):
		posting = self.index.setdefault(word, [])
		if posting == []:
			posting += [entry]
		else:
			if posting[0][0] > entry[0]:
				posting[0][0] = posting[0][0] - entry[0]
				posting = [entry] + posting
			else:
				docId, i = posting[0][0], 1
				while i < len(posting) and docId < entry[0]:
					docId += posting[i][0]
					i += 1
				if docId > entry[0]:
					i -= 1
					prev = (docId - posting[i][0])
					posting[i][0] = docId - entry[0]
					entry[0] = entry[0] - prev
					posting = posting[:i] + [entry] + posting[i:]
				else:
					entry[0] = entry[0] - posting[-1][0]
					posting = posting + [entry]
			self.index[word] = posting


	def addPageIndex(self, page_index):
		wordSet = set()
		for word, positions in page_index.text_map.items():
			entry = self._createEntry(page_index, word, positions)
			self._addEntry(word, entry)
			wordSet.add(word)
		for id_type in [page_index.infobox, page_index.category, page_index.reference,
						page_index.ext_links, page_index.title, page_index.others]:
			for word in id_type:
				if word not in wordSet:
					entry = self._createEntry(page_index, word, [])
					self._addEntry(word, entry)
					wordSet.add(word)
		if len(wordSet) != 0:
			self.updated = False

	def writeIndex(self):
		with open(self.shard_index_dir + "shard-index-" + str(self.shard_no), 'w', encoding = 'utf-8') as fp:
			re_sequence = [[r' |"',''],
							[r'\],\[', ';'],
							[r':\[\[', ':'],
							[r'\]\],', '|']]
			text = json.dumps(self.index, ensure_ascii = False)
			for regex, sub in re_sequence:
				text = re.sub(regex, sub, text)
			entext, code_table = self._encodeHuffman(text)
			code_table['EOF'] = code_table.pop(_EOF)
			code_table = json.dumps(code_table, ensure_ascii = False)
			# for regex, sub in re_sequence:
			# 	code_table = re.sub(regex, sub, code_table)
			index_map = {'d':entext, "t":code_table}
			json.dump(index_map, fp, ensure_ascii = False)
			self.updated = True




