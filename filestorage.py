import hashlib, os
from UserDict import DictMixin

class PermanentDict(DictMixin):
	def __init__(self, path):
		if not os.path.isdir(path):
			raise IOError("Could not open directory: %s" % path)
		self.path = path
	
	def __getitem__(self, key):
		h = hashlib.sha256(key).hexdigest()
		subdir = os.path.join(self.path, h[0:2])
		fpath = os.path.join(subdir, h)
		
		if not os.path.isfile(fpath):
			raise KeyError
		
		with open(fpath, "rb") as f:
			data = f.read()
		
		return data
	
	def __setitem__(self, key, value):
		h = hashlib.sha256(key).hexdigest()
		subdir = os.path.join(self.path, h[0:2])
		fpath = os.path.join(subdir, h)
		
		try:
			os.mkdir(subdir)
		except OSError:
			pass
		
		with open(fpath, "wb") as f:
			f.write(value)
	
	def __delitem__(self, key):
		h = hashlib.sha256(key).hexdigest()
		subdir = os.path.join(self.path, h[0:2])
		fpath = os.path.join(subdir, h)

		if not os.path.isfile(fpath):
			raise KeyError
		
		os.unlink(fpath)
		
		try:
			os.rmdir(subdir)
		except OSError:
			pass
		
