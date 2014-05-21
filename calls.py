import os
import uuid, shutil
import riak, copy, json, mimetypes
import pyramid_storage

from pyramid.response import Response
from pyramid_storage import local
from pyramid.renderers import render_to_response
from array import *
from time import strftime

client = riak.RiakClient(pb_port=8087, protocol='pbc')
db = client.bucket('resources')
f_s = '/home/user/Development/uploads'

dataProfile = {
	'file_location': {
		'local_path': None
	},
	'metadata': {
		'author': None,
		'title': None,
		'description': None,
		'upload_date': None,
		'last_modified_date': None,
		'mime_type': None,
		'resource_type': None,
		'keywords': None,
		'version': None
	}
}

def home(request):
	variables = {'name': 'next_file'}
	return render_to_response('public/upload_file.pt', variables, request=request)

def getKeys(request):
	success_HTML = ''
	successlist = list()
	
	for next_key in db.get_keys():
		successlist.append('<li>[key] ' + next_key + '</li>\n')

	for NI in successlist:
		success_HTML = success_HTML + NI
		
	variables = {'success': success_HTML}
	return render_to_response('public/bucket_keys.pt', variables, request=request)
	
	
def basicSearch(request):
	search_term = ''
	
	try:
		search_term = request.json['search']['term']
	except ValueError:
		return Response(status=400, body='Body is not in JSON format')
	except KeyError:
		return Response(status=400, body='Body does not include required field "search.term"')
	
	obj = ['result', {'msg': 'You made a basic search request for: ' + search_term}]
		
	res = Response(status=200, json=obj)
	return res


def advancedSearch(request):
	author = ''
	keywords = ''
	resource_type = ''
	
	try:
		author = request.json['advanced_search']['author']
		keywords = request.json['advanced_search']['keywords']
		resource_type = request.json['advanced_search']['resource_type']
	except ValueError:
		return Response(status=400, body='Body is not in JSON format')
	except KeyError, ke:
		return Response(status=400, body='Body does not include required field: ' + str(ke) )
	
	obj = ['result', {'msg': 'You made an advanced search request for author: ' + author + ', keywords: ' + keywords + ', resource_type: ' + resource_type}]
	res = Response(status=200, json=obj)
	return res



def getMetadata(request):
	key_str = request.matchdict['id']
	key = db.get(request.matchdict['id'])
	db_data = key.data
	
	if not key.exists:
		obj = ['result', {'msg': 'resource id: ' + key_str + ' was not found'}]	
		res = Response(status=404, json=obj)
	else:
		obj = {'metadata': db_data['metadata']}
		res = Response(status=200, json=obj)
		
	return res
	


def setMetadata(request):
	author = ''
	title = ''
	description = ''
	keywords = ''
	mime_type = ''
	version = ''

	key_str = request.matchdict['id']
	key = db.get(request.matchdict['id'])
	
	if not key.exists:
		obj = ['result', {'msg': 'resource id: ' + key_str + ' was not found'}]	
		res = Response(status=404, json=obj)
	else:
		try:
			author = request.json['metadata']['author']
			title = request.json['metadata']['title']
			description = request.json['metadata']['description']
			keywords = request.json['metadata']['keywords']
			mime_type = request.json['metadata']['mime_type']
			version = request.json['metadata']['version']
		except ValueError:
			return Response(status=400, body='Body is not in JSON format')
		except KeyError, ke:
			return Response(status=400, body='Body does not include required field: ' + str(ke) )

		now = strftime("%Y-%m-%d %H:%M:%S")
		
		# load up the data object
		key.data['metadata']['author'] = author
		key.data['metadata']['title'] = title
		key.data['metadata']['description'] = description
		key.data['metadata']['keywords'] = keywords
		key.data['metadata']['last_modified_date'] = now
		key.data['metadata']['mime_type'] = mime_type
		key.data['metadata']['version'] = version
		key.store()
		
		obj = {'metadata': key.data['metadata']}
		res = Response(status=200, json=obj)
		return res



def uploadFile(request):
	success_HTML = ''
	failed_HTML = ''
	successlist = list()
	failedlist = list()
	fileslist = list()
	fileslist = request.POST.getall('files')
	
	for next_file in fileslist:
		filename = next_file.filename
		extension = os.path.splitext(filename)[1][1:].strip().lower()
		input_file = next_file.file

		#may need to come up with a procedure for handling a failed upload... 
		#don't know if the upload should continue or error out?
		
		if extension.lower() == 'zip' or extension.lower() == 'pdf' or extension.lower() == 'xml':
			upload_path = os.path.join(f_s, filename % uuid.uuid4())
			temp_path = upload_path + '~'
			output_file = open(temp_path, 'wb')
			
			input_file.seek(0)
			while True:
				data = input_file.read(2<<16)
				if not data:
					break
				output_file.write(data)
				
			output_file.close()
			os.rename(temp_path, upload_path)
			
			uid = filename
			if db.get(uid).exists:
				failedlist.append('<li>[400] ' + uid + '</li>\n')
				#variables = {'f': '[409] The file already exists in the database!'}
				#return render_to_response('public/upload_response.pt', variables, request=request)
			else:
				now = strftime("%Y-%m-%d %H:%M:%S")
				data = copy.deepcopy(dataProfile)
				data['file_location']['local_path'] = upload_path
				data['metadata']['upload_date'] = now
				data['metadata']['last_modified_date'] = now
				data['metadata']['resource_type'] = extension.lower()
				
				key = db.new(uid, data=data)
				key.store()
				successlist.append('<li>[200] ' + uid + '</li>\n')
				#variables = {'f': '[200] The file ' + filename + ' was successfully saved to the database!'}
				#return render_to_response('public/upload_response.pt', variables, request=request)
		else:
			failedlist.append('<li>[400] ' + filename + '</li>\n')
			#variables = {'f': '[404] The file extension is not compatible for upload!'}
			#return render_to_response('public/upload_response.pt', variables, request=request)

	for NI in successlist:
		success_HTML = success_HTML + NI
	for NI in failedlist:
		failed_HTML = failed_HTML + NI

	variables = {'success': success_HTML, 'failed': failed_HTML}
	return render_to_response('public/upload_response.pt', variables, request=request)

	
def downloadFile(request):
	filename = request.matchdict['id']
	key = db.get(request.matchdict['id'])

	if not key.exists:
		obj = ['result', {'msg': 'resource id: ' + filename + ' was not found'}]	
		res = Response(status=404, json=obj)
	else:
		file_path = key.data['file_location']['local_path']
		res = Response(content_type=get_mimetype(file_path))
		res.app_iter = FileIterable(file_path)
		res.content_length = os.path.getsize(file_path)
		res.status=200
		return res		


def deleteFile(request):
	key_str = request.matchdict['id']
	key = db.get(request.matchdict['id'])
	db_data = key.data
	
	if not key.exists:
		obj = ['result', {'msg': 'resource id: ' + key_str + ' was not found'}]	
		res = Response(status=404, json=obj)
	else:
		file_path = db_data['file_location']['local_path']
		key.delete()
		os.remove(file_path)
		obj = ['result', {'msg': 'deleteFile request for resource id: ' + key_str + ' was successful'}]	
		res = Response(status=200, json=obj)
		
	return res



def getAllVersions(request):

	key = request.matchdict['id']

	if key != "":
		obj = [u'result', {u'msg': u'You made a getAllVersions request for resource id: ' + key}]	
		res = Response(status=200, json=obj)
	else:
		res.status=404
	
	return res

	
def get_mimetype(filename):
	type, encoding = mimetypes.guess_type(filename)
	return type or 'application/octet-stream'
	

class FileIterable(object):
	def __init__(self, filename):
		self.filename = filename
	def __iter__(self):
		return FileIterator(self.filename)
		
class FileIterator(object):
	chunk_size = 4096
	def __init__(self, filename):
		self.filename = filename
		self.fileobj = open(self.filename, 'rb')
	def __iter__(self):
		return self
	def next(self):
		chunk = self.fileobj.read(self.chunk_size)
		if not chunk:
			raise StopIteration
		return chunk
	__next__ = next













		