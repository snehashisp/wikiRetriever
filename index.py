import json
import copy
import re
import base64

class PageIndex():

	def __init__(self, page, title = set(), text_map = {}, infobox = set(), 
					category = set(), reference = set(), ext_links = set(), others = set()):
		self.page = page
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
			re_sequence = [[r'(\||{)(.*?):', r'\1"\2":'], 
							[r',([icreto]+);', r',"\1";'],
							[r',([icreto]+),', r',"\1",'], 
							[r',([icreto]+)\|', r',"\1"]],'], 
							[r',([icreto]+)]', r',"\1"]'],
							[r':', ':[['],
							[r';', '],['], 
							[r'\|', ']],']]
			data = json.load(fp)
			text = data['d']
			# text, code_table = data['d'], data['t']
			# code_table = re.sub(r'({|,)(.?):', r'\1"\2":', code_table)
			# code_table = json.loads(code_table)
			# code_table[_EOF] = code_table.pop("E")
			# text = self._decodeHuffman(code_table, text)
			for regex, sub in re_sequence:
				text = re.sub(regex, sub, text)
			#print(text)
			self.index = json.loads(text)
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
					entry[0] = entry[0] - docId
					posting = posting + [entry]
			self.index[word] = posting

	def mergePosting(self, word, posting):
		current_posting = self.index.get(word, [])
		combined_posting = []
		if current_posting != []:
			i, j, d1, d2, docId = 0, 0, current_posting[0][0], posting[0][0], 0
			while i < len(current_posting) and j < len(posting):
				if d1 < d2:
					current_posting[i][0] = d1
					combined_posting += [current_posting[i]]
					i += 1
					if i < len(current_posting):
						d1 += current_posting[i][0]
				else:
					posting[j][0] = d2
					combined_posting += [posting[j]]
					j += 1
					if j < len(posting):
						d2 += posting[j][0]
				if len(combined_posting) > 1:
					combined_posting[-1][0] -= docId
				docId += combined_posting[-1][0]

			if i < len(current_posting):
				current_posting[i][0] = d1
				current_posting[i][0] -= docId
				while i < len(current_posting):
					combined_posting += [current_posting[i]]
					i += 1

			if j < len(posting):
				posting[j][0] = d2
				posting[j][0] -= docId
				while j < len(posting):
					combined_posting += [posting[j]]
					j += 1
			self.index[word] = combined_posting
		else:
			self.index[word] = posting


	def getWords(self):
		return list(self.index.keys())

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
			# entext, code_table = self._encodeHuffman(text)
			# #print(entext)
			# code_table['E'] = code_table.pop(_EOF)
			# code_table = json.dumps(code_table, ensure_ascii = False)
			# code_table = re.sub(r' |"', '', code_table)
			# index_map = {'d':entext, "t":code_table}
			index_map = {'d':text}
			json.dump(index_map, fp, ensure_ascii = False)
			self.updated = True

	def writeIntermediateIndex(self):
		sortedIndex = {}
		for key in sorted(self.index.keys()):
			sortedIndex[key] = self.index[key]
		self.index = sortedIndex
		with open(self.shard_index_dir + "i-index-" + str(self.shard_no), 'w', encoding = 'utf-8') as fp:
			for key, value in self.index.items():
				fp.write(json.dumps({key:value}, ensure_ascii = False) + "\n")


if __name__ == "__main__":
	shi = ShardIndex(0, "ind/")
	shi.readIndex()
