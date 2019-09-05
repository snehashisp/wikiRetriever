import json

class Page():

	def __init__(self):
		
		self.id = None
		self.namespace = None
		self.title = None
		self.text = None
		self.shard_no = None
		self.final = False

	def __str__(self):

		return ",".join([str(self.id), str(self.namespace), str(self.title), str(self.text)])

	def formatJSON(self):

		return json.dumps({
			self.id : {
					"namespace":self.namespace,
					"title":self.title,
					"text":self.text
				}
			})

	def getSize(self):
		return len(str(self.namespace)) + len(str(self.title)) + len(str(self.text)) + len(str(self.id)) + 48




	
