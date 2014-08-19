import os, binascii
from UserDict import DictMixin
import portalocker

class StorageDict(DictMixin):
	def __init__(self, path):
		if not os.path.isdir(path):
			raise IOError("Could not open directory: %s" % path)
		self.path = path
		self.bucketlen = 4
		self.syncronous = True
		self.openfiles = {}
	
	def __getitem__(self, key):
		'''not really used'''
		h = binascii.b2a_hex(key)
		subdir = os.path.join(self.path, h[0:self.bucketlen])
		fpath = os.path.join(subdir, h)
		
		if not os.path.isfile(fpath):
			raise KeyError
		
		if key in self.openfiles:
			f = self.openfiles[key]
			f.seek(0)
		else:
			f = open(fpath, "rb")
			self.openfiles[key] = f
		data = f.read()
		return data
	
	def __setitem__(self, key, value):
		h = binascii.b2a_hex(key)
		subdir = os.path.join(self.path, h[0:self.bucketlen])
		fpath = os.path.join(subdir, h)
		
		try:
			os.mkdir(subdir)
		except OSError:
			pass

		if key in self.openfiles:
			f = self.openfiles[key]
			f.seek(0)
		else:
			f = open(fpath, "wb")
			self.openfiles[key] = f
		f.write(value)
		if self.synchronous:
			f.flush()
		
	def __delitem__(self, key):
		'''call at EVENT_SEARCH_DONE, EVENT_SEARCH_DONE6'''
		if key in self.openfiles:
			self.openfiles[key].close()
		else:
			raise KeyError

#get hashes from database with order by tid and split them to multiple crawlers, this should avoid the need for locks
