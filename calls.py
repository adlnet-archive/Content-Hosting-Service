import os
import uuid, shutil, pprint
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
db.enable_search()
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
	},
	'paradata': {
		'user_reviews': [
			{'review_id': None,
			 'user_rating': None,
			 'user_name': None,
			 'user_review_title': None,
			 'user_review': None,
			 'timestamp': None
			 }
		],
		'user_comments': [
			{'review_id': None,
			 'comment_id': None,
			 'helpful': None,
			 'user_name': None,
			 'user_comment': None,
			 'timestamp': None
			 }
		]
	}
	
}

# method for serving the file upload form
def home(request):
	variables = {'name': 'next_file'}
	return render_to_response('templates/upload_file.pt', variables, request=request)

# method for displaying all of the keys in the db
def getKeys(request):
	success_HTML = ''
	successlist = list()
	
	for next_key in db.get_keys():
		successlist.append('<li>[key] ' + next_key + '</li>\n')

	for NI in successlist:
		success_HTML = success_HTML + NI
		
	variables = {'success': success_HTML}
	return render_to_response('templates/bucket_keys.pt', variables, request=request)
	
''' method for conducting a basic search. User submits a POST request consisting of a JSON document with a search 
    group: (metadata/paradata), field and term.
	Returns:
	200: OK: returns a list of filenames (keys) where there is a match between the search term and the metadata.
	400: Bad Request: The request body is not valid JSON, or does not contain the required field.
	404: Not Found: No results returned. '''
def basicSearch(request):
	search_group = ''
	search_field = ''
	search_term = ''
	keys_found = {'results': []}
	
	try:
		search_group = request.json['search']['group']
		search_field = request.json['search']['field']
		search_term = request.json['search']['term']
	except ValueError:
		return Response(status=400, body='Body is not in JSON format')
	except KeyError:
		return Response(status=400, body='Body does not include required field "search.term"')
	
	try:
		term = search_group.encode("utf-8") + '_' + search_field.encode("utf-8") + ':' + search_term.encode("utf-8")
		print 'search query: ' + term
		search_query = client.search('resources', term)
	
		try:
			#print 'result entries found: ' + str(len(search_results))
			for result in search_query.run():
				result_key = result[1]
				print result[0]
				print result[1]
				print result[2]
				keys_found['results'].append({'key': result_key})

		except Exception, e:
			return Response(status=400, body='Problem with iterating search result: '  + str(e))
	
	except TypeError, te:
		return Response(status=400, body='Problem with search request: '  + str(te))
		
	res = Response(status=200, json=keys_found)
	return res

''' method for conducting an advanced search. User submits a POST request consisting of a JSON document with up to   
    at least two and at most three search groups: (metadata/paradata), field and term.
	Returns:
	200: OK: returns a list of filenames (keys) where there is a match between the search term and the metadata.
	400: Bad Request: The request body is not valid JSON, or does not contain the required field.
	404: Not Found: No results returned. '''
def advancedSearch(request):
	# variables
	term1 = ''
	term2 = ''
	term3 = ''
	query = ''
	search_group1 = ''
	search_group2 = ''
	search_group3 = ''
	search_field1 = ''
	search_field2 = ''
	search_field3 = ''
	search_term1 = ''
	search_term2 = ''
	search_term3 = ''
	keys_found = {'results': []}
	
	# check incoming request for validity as well as the required two sets of groups/fields/terms 
	# for the advanced query
	try:
		# require at least two for advanced search: group/field/term  AND group/field/term
		search_group1 = request.json['advanced_search']['group1']
		search_group2 = request.json['advanced_search']['group2']
		search_group3 = request.json['advanced_search']['group3']
		search_field1 = request.json['advanced_search']['field1']
		search_field2 = request.json['advanced_search']['field2']
		search_field3 = request.json['advanced_search']['field3']
		search_term1 = request.json['advanced_search']['term1']
		search_term2 = request.json['advanced_search']['term2']
		search_term3 = request.json['advanced_search']['term3']
	except ValueError:
		return Response(status=400, body='Advanced search body is not in JSON format')
	except KeyError, ke:
		return Response(status=400, body='Advanced search body does not include required fields: ' + str(ke) )

	if search_group1 != '':
		term1 = search_group1.encode("utf-8") + '_' + search_field1.encode("utf-8") + ':' + search_term1.encode("utf-8")
		
	if search_group2 != '':
		term2 = search_group2.encode("utf-8") + '_' + search_field2.encode("utf-8") + ':' + search_term2.encode("utf-8")

	if search_group3 != '':
		term3 = search_group3.encode("utf-8") + '_' + search_field3.encode("utf-8") + ':' + search_term3.encode("utf-8")
		
	try:
		if term1 != '' and term2 != '' and term3 != '':
			query = term1 + ' AND ' + term2 + ' AND ' + term3		
		elif term1 != '' and term2 != '':
			query = term1 + ' AND ' + term2
		elif term1 != '':
			query = term1
		else:
			return Response(status=400, body='Advanced search terms are empty')
			
		print 'advanced search query: ' + query
		search_query = client.search('resources', query)
	
		try:
			#print 'result entries found: ' + str(len(search_results))
			for result in search_query.run():
				result_key = result[1]
				keys_found['results'].append({'key': result_key})

		except Exception, e:
			return Response(status=400, body='Problem with iterating advanced search result: '  + str(e))
	
	except TypeError, te:
		return Response(status=400, body='Problem with advanced search request: '  + str(te))
		
	res = Response(status=200, json=keys_found)
	return res


''' this method takes a GET request with key {id} and returns the metadata object for that file.
    Returns:
    200: OK: the metadata was successfully returned.
    404: Not Found: the file was not in the db. '''
def getMetadata(request):
    # get key: filename {id}
	key_str = request.matchdict['id']
	# get object
	db_key = db.get(key_str)
	db_data = db_key.data
	
	# validate key exists in the db:
	if not db_key.exists:
		obj = ['result', {'msg': 'resource id: ' + key_str + ' was not found'}]	
		res = Response(status=404, json=obj)
	else:
	    # only return the metadata portion of the object: file_location = [0], metadata = [1]
		res = Response(status=200, json=db_data[1])
		
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
	db_key = db.get(key_str)
	
	# validate key exists in the db:
	if not db_key.exists:
		obj = ['result', {'msg': 'resource id: ' + key_str + ' was not found'}]	
		res = Response(status=404, json=obj)
	else:
		db_data = db_key.data
		db_mdata = db_data[1]
		
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
		db_mdata['metadata']['author'] = author
		db_mdata['metadata']['title'] = title
		db_mdata['metadata']['description'] = description
		db_mdata['metadata']['keywords'] = keywords
		db_mdata['metadata']['last_modified_date'] = now
		db_mdata['metadata']['mime_type'] = mime_type
		db_mdata['metadata']['version'] = version
		db_key.data[1] = db_mdata
		db_key.store()
		
		# return the metadata that was just set
		#obj = {'metadata': key.data['metadata']}
		res = Response(status=200, json=db_key.data[1])
		return res

''' this method takes a GET request with key {id} and returns the paradata object for that file.
    Returns:
    200: OK: the paradata was successfully returned.
    404: Not Found: the file was not in the db. '''
def getParadata(request):
    # get key: filename {id}
	key_str = request.matchdict['id']
	# get object
	db_key = db.get(key_str)
	db_data = db_key.data
	
	# validate key exists in the db:
	if not db_key.exists:
		obj = ['result', {'msg': 'resource id: ' + key_str + ' was not found'}]	
		res = Response(status=404, json=obj)
	else:
	    # only return the paraadata portion of the object: file_location = [0], metadata = [1], paradata = [2]
		res = Response(status=200, json=db_data[2])
		
	return res

		
''' this method takes a POST request with key {id} and a JSON document with the user review paradata
    to set and returns the paradata part of the object's JSON data for that file.

    Returns:
    200: OK: the user review paradata was successfully set and returned.
    404: Not Found: the file was not in the db. '''
def setUserReviewParadata(request):
    # variables for user reviews:
	review_id = ''
	user_rating = ''
	user_name = ''
	user_review_title = ''
	user_review = ''
	timestamp = ''

	# get key: filename {id}
	key_str = request.matchdict['id']
	db_key = db.get(key_str)
	# get the current time of the operation for updating the 'timestamp' field
	now = strftime("%Y-%m-%d %H:%M:%S")
	
	# validate key exists in the db:
	if not db_key.exists:
		obj = ['result', {'msg': 'resource id: ' + key_str + ' was not found'}]	
		res = Response(status=404, json=obj)
	else:
		db_data = db_key.data
		db_pdata = db_data[2]
		
		# get the data from the request JSON object, validating in the process:
		try:
			review_id = str(uuid.uuid4())
			user_rating = request.json['user_reviews']['user_rating']
			user_name = request.json['user_reviews']['user_name']
			user_review_title = request.json['user_reviews']['user_review_title']
			user_review = request.json['user_reviews']['user_review']
			timestamp = now
		except ValueError:
			return Response(status=400, body='Body is not in JSON format')
		except KeyError, ke:
			return Response(status=400, body='Body does not include required field: ' + str(ke) )

		# add the new user review paradata object
		db_pdata['paradata']['user_reviews'].append({'review_id': review_id, 'user_rating': user_rating, 'user_name': user_name, 'user_review_title': user_review_title, 'user_review': user_review, 'timestamp': timestamp})
		db_key.data[2] = db_pdata
		db_key.store()
		
		# return the paradata that was just set
		res = Response(status=200, json=db_key.data[2])
		return res
		
''' this method takes a POST request with key {fid}, review id {rid} and a JSON document with the 
    user comment paradata to set and returns the paradata part of the object's JSON data for that file.

    Returns:
    200: OK: the user comment paradata was successfully set and returned.
    404: Not Found: the file was not in the db. '''
def setUserCommentParadata(request):
    # variables for user comments:
	review_id = ''
	comment_id = ''
	helpful = ''
	user_name = ''
	user_comment = ''
	timestamp = ''

	# get key: filename {fid}
	key_str = request.matchdict['fid']
	db_key = db.get(key_str)
	
	# get review_id: {rid}
	review_id = request.matchdict['rid']
	
	# get the current time of the operation for updating the 'timestamp' field
	now = strftime("%Y-%m-%d %H:%M:%S")
	
	# validate key exists in the db:
	if not db_key.exists:
		obj = ['result', {'msg': 'resource id: ' + key_str + ' was not found'}]	
		res = Response(status=404, json=obj)
	else:
		db_data = db_key.data
		db_pdata = db_data[2]
		
		# get the data from the request JSON object, validating in the process:
		try:
			comment_id = str(uuid.uuid4())
			helpful = request.json['user_comments']['helpful']
			user_name = request.json['user_comments']['user_name']
			user_comment = request.json['user_comments']['user_comment']
			timestamp = now
		except ValueError:
			return Response(status=400, body='Body is not in JSON format')
		except KeyError, ke:
			return Response(status=400, body='Body does not include required field: ' + str(ke) )

		# add the new user comment paradata object
		db_pdata['paradata']['user_comments'].append({'review_id': review_id, 'comment_id': comment_id, 'helpful': helpful, 'user_name': user_name, 'user_comment': user_comment, 'timestamp': timestamp})
		db_key.data[2] = db_pdata
		db_key.store()
		
		# return the paradata that was just set
		res = Response(status=200, json=db_key.data[2])
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
				#return render_to_response('templates/upload_response.pt', variables, request=request)
			else:
				now = strftime("%Y-%m-%d %H:%M:%S")
				key = db.new(uid, [{'file_location': {'local_path': upload_path}}, {'metadata':{'author':'', 'title':'', 'description':'', 'upload_date': now, 'last_modified_date': now, 'mime_type':'', 'resource_type': extension.lower(), 'keywords':'', 'version': ''}}, {'paradata': {'user_reviews': [], 'user_comments': []} }], content_type='application/json', encoded_data=None)
				
				key.store()
				successlist.append('<li>[200] ' + uid + '</li>\n')
				#variables = {'f': '[200] The file ' + filename + ' was successfully saved to the database!'}
				#return render_to_response('templates/upload_response.pt', variables, request=request)
		else:
			failedlist.append('<li>[400] ' + filename + '</li>\n')
			#variables = {'f': '[404] The file extension is not compatible for upload!'}
			#return render_to_response('templates/upload_response.pt', variables, request=request)

	for NI in successlist:
		success_HTML = success_HTML + NI
	for NI in failedlist:
		failed_HTML = failed_HTML + NI

	variables = {'success': success_HTML, 'failed': failed_HTML}
	return render_to_response('templates/upload_response.pt', variables, request=request)

''' This method currently takes a GET request to download one file {id}. Files are retrieved using the filepath 
    from the data object.
	
	Returns:
	200: OK: File was found and successfully downloaded.
	404: Not found: File does not exist in the db. '''	
def downloadFile(request):
	# get key: filename {id}
	filename = request.matchdict['id']
	db_key = db.get(filename)
	db_data = db_key.data

	# validate key exists in the db:
	if not db_key.exists:
		obj = ['result', {'msg': 'resource id: ' + filename + ' was not found'}]	
		res = Response(status=404, json=obj)
	else:
		# get file location for download
		db_fdata = db_data[0]
		file_path = db_fdata['file_location']['local_path']
		
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
	db_key = db.get(key_str)
	db_data = db_key.data

	# validate key exists in the db:
	if not db_key.exists:
		obj = ['result', {'msg': 'resource id: ' + key_str + ' was not found'}]	
		res = Response(status=404, json=obj)
	else:
		# get file location for delete
		db_fdata = db_data[0]
		file_path = db_fdata['file_location']['local_path']
		# delete from db
		db_key.delete()
		# delete from file system
		os.remove(file_path)
		obj = ['result', {'msg': 'deleteFile request for resource id: ' + key_str + ' was successful'}]	
		res = Response(status=200, json=obj)
		
	return res

''' This method takes a GET request to delete ALL files in the db.
	
	Returns:
	200: OK: Returns a JSON document with a message that all files were deleted.'''
def deleteAll(request):
	
	for next_key in db.get_keys():
		db_key = db.get(next_key)
		db_data = db_key.data
		db_fdata = db_data[0]

		file_path = db_fdata['file_location']['local_path']
		db_key.delete()
		os.remove(file_path)
		
	obj = ['result', {'msg': 'All files deleted.'}]	
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













		