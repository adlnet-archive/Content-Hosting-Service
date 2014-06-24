import sys
import calls

from wsgiref.simple_server import make_server
from pyramid.config import Configurator
from pyramid.response import Response
from pyramid_storage import local

def main(args):
	
	config = Configurator()
	
	# create homepage with file upload form
	config.add_route('home', pattern='/CHS/')
	config.add_view(calls.home, route_name='home')
	
	#list all keys in bucket
	config.add_route('getKeys', pattern='/CHS/keys/', request_method='GET')
	config.add_view(calls.getKeys, route_name='getKeys')
	
	# configure search routines
	config.add_route('basicSearch', pattern='/CHS/search', request_method='POST')
	config.add_view(calls.basicSearch, route_name='basicSearch')
	config.add_route('advancedSearch', pattern='/CHS/advanced_search', request_method='POST')
	config.add_view(calls.advancedSearch, route_name='advancedSearch')
	
	# configure access metadata and paradata routines
	config.add_route('getMetadata', pattern='/CHS/get_metadata/{id}', request_method=('GET', 'HEAD'))
	config.add_view(calls.getMetadata, route_name='getMetadata')
	config.add_route('setMetadata', pattern='/CHS/set_metadata/{id}', request_method=('POST', 'PUT'))
	config.add_view(calls.setMetadata, route_name='setMetadata')
	config.add_route('getParadata', pattern='/CHS/get_paradata/{id}', request_method=('GET', 'HEAD'))
	config.add_view(calls.getParadata, route_name='getParadata')
	# set the two types of paradata: user reviews and user comments
	config.add_route('setUserReviewParadata', pattern='/CHS/set_user_review_paradata/{id}', request_method=('POST', 'PUT'))
	config.add_view(calls.setUserReviewParadata, route_name='setUserReviewParadata')
	config.add_route('setUserCommentParadata', pattern='/CHS/set_user_comment_paradata/{fid}/{rid}', request_method=('POST', 'PUT'))
	config.add_view(calls.setUserCommentParadata, route_name='setUserCommentParadata')
	
	# configure file handling routines
	config.add_route('uploadFile', pattern='/CHS/upload_file')
	config.add_view(calls.uploadFile, route_name='uploadFile', request_method='POST')
	config.add_route('downloadFile', pattern='/CHS/download_file/{id}', request_method=('GET', 'HEAD'))
	config.add_view(calls.downloadFile, route_name='downloadFile')
	config.add_route('deleteFile', pattern='/CHS/{id}', request_method='DELETE')
	config.add_view(calls.deleteFile, route_name='deleteFile')
	config.add_route('deleteAll', pattern='/CHS/deleteAll/', request_method='GET')
	config.add_view(calls.deleteAll, route_name='deleteAll')

	
	# configure other routines
	config.add_route('getAllVersions', pattern='/CHS/get_all_versions/{id}', request_method=('GET', 'HEAD'))
	config.add_view(calls.getAllVersions, route_name='getAllVersions')
	
	config.add_static_view('static', 'static/')
	
	app = config.make_wsgi_app()
	server = make_server('0.0.0.0', 8080, app)
	
	# launch server
	print 'Server active on {0[0]}:{0[1]}'.format(server.server_address)
	print ''
	
	try:
		server.serve_forever()
	except KeyboardInterrupt:
		print 'Good-bye!'
		quit()
		
if __name__ == '__main__':
	main(sys.argv)