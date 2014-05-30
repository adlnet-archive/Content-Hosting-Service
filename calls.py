import os
import uuid, shutil
import riak, copy, json, mimetypes
import pyramid_storage

from pyramid.response import Response
from pyramid_storage import local
from pyramid.renderers import render_to_response
from array import *
from time import strftime


# setup the Riak db
client = riak.RiakClient(pb_port=8087, protocol='pbc')
db = client.bucket('resources')
# initialize the file storage location
f_s = '/home/user/Development/uploads'

''' This is the data object structure format for  the file objects stored in the db '''
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

# method for serving the file upload form
def home(request):
	variables = {'name': 'next_file'}
	return render_to_response('public/upload_file.pt', variables, request=request)

# method for displaying all of the keys in the db
def getKeys(request):
	success_HTML = ''
	successlist = list()
	
	for next_key in db.get_keys():
		successlist.append('<li>[key] ' + next_key + '</li>\n')

	for NI in successlist:
		success_HTML = success_HTML + NI
		
	variables = {'success': success_HTML}
	return render_to_response('public/bucket_keys.pt', variables, request=request)
	
''' method for conducting a basic search. User submits a POST request consisting of a JSON document with a search term.
	Returns:
	200: OK: returns a list of filenames where there is a match between the search term and the metadata.
	400: Bad Request: The request body is not valid JSON, or does not contain the required field.
	404: Not Found: No results returned. '''
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

''' method for conducting an advanced search. User submits a POST request consisting of a JSON document with three 
        required search terms.
	Returns:
	200: OK: returns a list of filenames where there is a match between the search term and the metadata.
	400: Bad Request: The request body is not valid JSON, or does not contain the required field.
	404: Not Found: No results returned. '''
def advancedSearch(request):
	# variables
	author = ''
	keywords = ''
	resource_type = ''
	
	# check incoming request for validity as well as assigning values
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


''' this method takes a GET request with key {id} and returns the metadata object for that file.
    Returns:
    200: OK: the metadata was successfully returned.
    404: Not Found: the file was not in the db. '''
def getMetadata(request):
    # get key: filename {id}
	key_str = request.matchdict['id']
	# get object
	key = db.get(request.matchdict['id'])
	db_data = key.data
	
	# validate key exists in the db:
	if not key.exists:
		obj = ['result', {'msg': 'resource id: ' + key_str + ' was not found'}]	
		res = Response(status=404, json=obj)
	else:
	    # only return the metadata portion of the object
		obj = {'metadata': db_data['metadata']}
		res = Response(status=200, json=obj)
		
	return res
	

''' this method takes a POST request with key {id} and a JSON document with the metadata to set and 
    returns the metadata object for that file.
    Returns:
    200: OK: the metadata was successfully returned.
    404: Not Found: the file was not in the db. '''
def setMetadata(request):
    # variables:
	author = ''
	title = ''
	description = ''
	keywords = ''
	mime_type = ''
	version = ''

	# get key: filename {id}
	key_str = request.matchdict['id']
	key = db.get(request.matchdict['id'])
	
	# validate key exists in the db:
	if not key.exists:
		obj = ['result', {'msg': 'resource id: ' + key_str + ' was not found'}]	
		res = Response(status=404, json=obj)
	else:
		# get the data from the request JSON object, validating in the process:
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

		# get the current time of the operation for updating the 'last_modified_date' field
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
		
		# return the metadata that was just set
		obj = {'metadata': key.data['metadata']}
		res = Response(status=200, json=obj)
		return res


''' This method currently takes a POST request from a web form template to upload one or more files.
    Each file is checked against the allowed file extensions for acceptance. Files are uploaded and 
	stored on the server with a filepath saved to the 'file_location' part of the data object.
	
	Returns:
	200: OK: File extension was accepted and the file was successfully uploaded.
	400: Bad Request: File extension was not of the type accepted.
	409: Conflict: File already exists in the db. '''
def uploadFile(request):
	# TODO: look for a way to submit files for upload via JSON POST request.
	# TODO: decide how to handle failures when uploading multiple files.
	success_HTML = ''
	failed_HTML = ''
	successlist = list() # for reporting the filenames that were succefully uploaded
	failedlist = list() # for reporting the filenames that were unsuccefully uploaded
	fileslist = list()
	fileslist = request.POST.getall('files') # files selected for upload
	
	#process each file in the upload list
	for next_file in fileslist:
		filename = next_file.filename
		extension = os.path.splitext(filename)[1][1:].strip().lower()
		input_file = next_file.file
		# check for the proper file extensions
		if extension.lower() == 'zip' or extension.lower() == 'pdf' or extension.lower() == 'xml':
			upload_path = os.path.join(f_s, filename % uuid.uuid4())
			temp_path = upload_path + '~'
			output_file = open(temp_path, 'wb')
			# upload process
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

''' This method currently takes a GET request to download one file {id}. Files are retrieved using the filepath 
    from the data object.
	
	Returns:
	200: OK: File was found and successfully downloaded.
	404: Not found: File does not exist in the db. '''	
def downloadFile(request):
	# get key: filename {id}
	filename = request.matchdict['id']
	key = db.get(request.matchdict['id'])
	# validate key exists in the db:
	if not key.exists:
		obj = ['result', {'msg': 'resource id: ' + filename + ' was not found'}]	
		res = Response(status=404, json=obj)
	else:
		# get file location for download
		file_path = key.data['file_location']['local_path']
		# call download routines
		res = Response(content_type=get_mimetype(file_path))
		res.app_iter = FileIterable(file_path)
		res.content_length = os.path.getsize(file_path)
		res.status=200
		return res		

''' This method currently takes a DELETE request to delete one file {id} from the database.
	
	Returns:
	200: OK: File extension was accepted and the file was succesfully uploaded.
	404: Not found: File does not exist in the db. '''
def deleteFile(request):
	# get key: filename {id}
	key_str = request.matchdict['id']
	key = db.get(request.matchdict['id'])
	db_data = key.data
	# validate key exists in the db:
	if not key.exists:
		obj = ['result', {'msg': 'resource id: ' + key_str + ' was not found'}]	
		res = Response(status=404, json=obj)
	else:
		# get file location for delete
		file_path = db_data['file_location']['local_path']
		# delete from db
		key.delete()
		# delete from file system
		os.remove(file_path)
		obj = ['result', {'msg': 'deleteFile request for resource id: ' + key_str + ' was successful'}]	
		res = Response(status=200, json=obj)
		
	return res


''' This method takes a GET request to return a list of versions associated with one file {id}.
	(it is currently stubbed out for testing responses).
	
	Returns:
	200: OK: Returns a JSON document with a list of the file's versions.
	404: Not found: File does not exist in the db. '''
def getAllVersions(request):

	key = request.matchdict['id']

	if key != "":
		obj = [u'result', {u'msg': u'You made a getAllVersions request for resource id: ' + key}]	
		res = Response(status=200, json=obj)
	else:
		res.status=404
	
	return res

''' this method returns the file's mime-type '''	
def get_mimetype(filename):
	type, encoding = mimetypes.guess_type(filename)
	return type or 'application/octet-stream'
	
''' A wrapper class for the file iterator '''
class FileIterable(object):
	def __init__(self, filename):
		self.filename = filename
	def __iter__(self):
		return FileIterator(self.filename)

''' A file iterator for iterating over a file and grabbing chunks of its data to pass along for download'''		
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













		