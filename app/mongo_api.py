# -*- coding: utf-8 -*-
import os
import sys
import time
from flask import request, jsonify, Flask
from flask_restplus import fields, Api, Resource, Namespace
sys.path.insert(0, os.path.abspath('..'))
sys.path.insert(0, os.path.abspath('.'))
from src.utility.utils import Logger, set_env
from src.database.mongo.mongodb import MongodbUtility, ApiQuery


global_logger = Logger().get_logger('api')
dir_path = os.path.dirname(os.path.realpath(__file__))
global_config = set_env(logger=global_logger,
                        env_file_path=str(dir_path.split('app')[0]) + 'env_files/dev/.env',
                        config_folder_name='configs')

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False  # make Han characters recognizable
app_healthcheck = Flask(__name__)
app_healthcheck.config['JSON_AS_ASCII'] = False
version = 'v1'
repo_tag = 'latest'
server_url = '<your-server-host:port>'

# blueprint = Blueprint(version, __name__, url_prefix='/api/{version}'.format(version=version))  # urls must start with a leading slash, and don't end with one

api = Api(app,
          ui=True,  # https://stackoverflow.com/questions/32477878/flask-restplus-route
          doc='/swagger/',
          version=version,
          title='591-Renting-API',
          description='Swagger Doc of 591 House Renting API',
          prefix='/api/{version}'.format(version=version),
          # # urls must start with a leading slash, and don't end with one
          default="",
          default_label="",
          strict_slashes=False,
          )


class InvalidUsage(Exception):
    """
    https://stackoverflow.com/questions/60324360/what-is-best-practice-for-flask-error-handling
    https://flask.palletsprojects.com/en/1.1.x/patterns/apierrors/
    """
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv  # return render_template('500.htm'), 500


@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


#######################################################################################################################

# request payload
health_payload_request = api.model('HealthPayload',  # Model name in $ref we see
                                   dict(offset=fields.String(required=True,
                                                             description='offset',
                                                             example="1"),
                                        limit=fields.String(required=True,
                                                            description='limit',
                                                            example="100")))
# response
health_response = api.model('HealthResponse',
                            dict(response=fields.Integer(required=False,
                                                         description='response code',
                                                         example=200)))

#######################################################################################################################

# common fields
city_enum_fields = fields.String(required=True,
                                 description='city',
                                 enum=['taipei_city', 'new_taipei_city'])

gender_enum_fields = fields.String(required=True,
                                   description='gender',
                                   enum=['男', '女'])

owner_identity_fields = fields.String(required=True,
                                      description='owner\'s identity',
                                      example='屋主 or 仲介 or ...')

mongo_object_id_fields = fields.String(required=True,
                                       description='ObjectId in mongodb',
                                       example='6080c4693d3a69615bf56100')

floor_fields = fields.String(required=True,
                             description='floor',
                             example='3F')

story_fields = fields.String(required=True,
                             description='floor and number of storie',
                             example='3F/3F')

gender_request_fields = fields.String(required=True,
                                      description='gender request to renters',
                                      example='男女生皆可')

lot_size_fields = fields.String(required=True,
                                description='lot size; area',
                                example='6坪')

nick_name_fields = fields.String(required=True,
                                 description='information of contactor',
                                 example='仲介 沈先生')

owner_last_name_fields = fields.String(required=True,
                                       description='last name',
                                       example='吳 or 黃 or 王 or 潘 or 楊 or 李 or 林 or 張')

phone_fields = fields.String(required=True,
                             description='phone number',
                             example='0912-345-678')

post_id_fields = fields.Integer(required=True,
                                description='post_id on 591 webpage, for each apartment object',
                                example=9890771)

renter_fields = fields.String(required=True,
                              description='renter',
                              example='沈先生')

housing_object_status_fields = fields.String(required=True,
                                             description='status of the housing object',
                                             example='獨立套房')

housing_object_types_fields = fields.String(required=True,
                                            description='types of the housing object',
                                            example='公寓')

"""
{
  "code": 0, 
  "data": [
    {
      "_id": "6080c4693d3a69615bf56100", 
      "city": "台北市", 
      "floor": "3F", 
      "gender_request": "男女生皆可", 
      "lot_size": "6坪", 
      "nick_name": "仲介 沈先生", 
      "owner_gender": "男", 
      "owner_identity": "仲介", 
      "owner_last_name": "沈", 
      "phone": "0905-059-091", 
      "post_id": 9890771, 
      "renter": "沈先生", 
      "status": "獨立套房", 
      "story": "3F/3F", 
      "types": "公寓"
    }, 
  ], 
  "message": "success", 
  "time_elapsed_api": 0.175548

"""
# response - api common return
resp_data_dict = api.model('ColumnDataInResponse',
                           dict(_id=mongo_object_id_fields,
                                city=city_enum_fields,
                                floor=floor_fields,
                                gender_request=gender_request_fields,
                                lot_size=lot_size_fields,
                                nick_name=nick_name_fields,
                                owner_gender=gender_enum_fields,
                                owner_identity=owner_identity_fields,
                                owner_last_name=owner_last_name_fields,
                                phone=phone_fields,
                                post_id=post_id_fields,
                                renter=renter_fields,
                                status=housing_object_status_fields,
                                story=story_fields,
                                types=housing_object_types_fields))

response_format = api.model('CommonResponseFormat',
                            dict(code=fields.Integer(required=False,
                                                     description='response code',
                                                     example=0),
                                 data=fields.List(fields.Nested(model=resp_data_dict)),
                                 message=fields.String(required=False,
                                                       description='response code',
                                                       enum=['success', 'failed']),
                                 time_elapsed_api=fields.Float(required=False,
                                                               description='elapsed time from api return',
                                                               example=0.001)))

#######################################################################################################################

"""

curl \
-H "Content-Type: application/json" \
-d '{"city": "taipei_city", "gender":"男"}' \
-X POST \
http://127.0.0.1:30000/api/v1/renter/gender

"""

# request payload
renter_gender_req_payload = api.model('RenterGenderRequestPayload',  # Model name in $ref we see
                                      dict(city=city_enum_fields,
                                           gender=gender_enum_fields))

#######################################################################################################################

"""

curl \
-H "Content-Type: application/json" \
-d '{"phone": "0905-059-091"}' \
-X POST \
http://127.0.0.1:30000/api/v1/owner/phone

"""

# request payload
owner_phone_req_payload = api.model('OwnerPhoneRequestPayload',  # Model name in $ref we see
                                    dict(phone=phone_fields))

#######################################################################################################################

"""

curl \
-H "Content-Type: application/json" \
-d '{"negative_id_lst": ["屋主"]}' \
-X POST \
http://127.0.0.1:30000/api/v1/owner/identity

"""

# request payload
owner_identity_req_payload = api.model('OwnerIdentityRequestPayload',  # Model name in $ref we see
                                       dict(negative_id_lst=fields.List(owner_identity_fields)))

#######################################################################################################################

"""

curl \
-H "Content-Type: application/json" \
-d '{"city": "new_taipei_city", "owner_gender": "男", "owner_last_name": "楊"}' \
-X POST \
http://127.0.0.1:30000/api/v1/owner/gender/last-name

"""

# request payload
owner_gender_lastname_req_payload = api.model('OwnerGenderLastNameRequestPayload',  # Model name in $ref we see
                                              dict(city=city_enum_fields,
                                                   owner_gender=gender_enum_fields,
                                                   owner_last_name=owner_last_name_fields))


#######################################################################################################################


@app.route('/healthcheck', strict_slashes=False)
@api.doc(body=health_payload_request)  # request payload schema
def healthcheck():
    # code = None
    try:
        mongodb_handler = MongodbUtility(global_config, global_logger)
        mongodb_handler.logger.info("db status code: 200")
        # mongodb_handler.close()
        code = 200
        del mongodb_handler
    except Exception as e:
        # mongodb_handler.logger.error(e)  # if we failed generating mongodb_handler in 'try', we don't have 'logger'!
        exception_logger = Logger().get_logger('db-health-check-exception-logger')
        exception_logger.info("error message: {err}".format(err=e))
        code = 400
        del exception_logger
    return str(code)


@app.route('/version/', strict_slashes=False)
def hello_world():
    message = dict(
        version_info='591 House Renting - API Version:{version}, :{tag}\n'.format(version=version, tag=repo_tag),
        document_info='Please refer to: ' + server_url + '/swagger/')
    return message


ns_renter = Namespace("renter", description='Methods for renters', strict_slashes=False)


@ns_renter.route('/gender', strict_slashes=False)
class RenterGender(Resource):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = Logger().get_logger('api')
        env_file = './env_files/.env'
        self.config = set_env(logger=self.logger, env_file_path=env_file, config_folder_name='configs')
        self.query_handler = ApiQuery(global_config, global_logger)

    @ns_renter.doc(body=renter_gender_req_payload)  # request payload schema
    @ns_renter.response(200, 'Success', response_format)
    def post(self):
        """
        given city of house and renter's gender request, return apartments
        """
        # check keys in the requested payload
        necessary_keys = {'city', 'gender'}  # a set

        start = time.time()
        payload = request.json

        if necessary_keys.issubset(payload.keys()):
            self.logger.info("payload: {payload_type}, {payload}".format(payload_type=type(payload), payload=payload))
            # res = self.query_handler.query_renter_gender(city='taipei_city', gender='男')  # FIXME: test
            res = self.query_handler.query_renter_gender(city=payload['city'], gender=payload['gender'])
            end = time.time()
            time_elapsed_api = float("{:.6f}".format(end - start))

            if res:
                result = {
                    "code": 0,
                    "message": "success",
                    "data": res,  # [dumps(x) for x in res['data']],
                    "time_elapsed_api": time_elapsed_api,
                }
                self.logger.info('time_elapsed_api: {}'.format(time_elapsed_api))
            else:
                result = {
                    "code": -1,
                    "message": "failed",
                    "data": None,
                    "time_elapsed_api": 0,
                }
                self.logger.warning('no such data in db!')

            # # The return type must be a string, dict, tuple, Response instance, or WSGI callable - rest api outputs json
            result = jsonify(result)

            return result
        else:
            status_code = 400
            message_400 = '''{status_code} Bad Request. Please make sure your data payload with columns: {col}'''.format(
                status_code=status_code, col=necessary_keys)
            raise InvalidUsage(message_400, status_code=status_code)


ns_owner = Namespace("owner", description='Methods for owners', strict_slashes=False)


@ns_owner.route('/phone', strict_slashes=False)
class OwnerPhone(Resource):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = Logger().get_logger('api')
        env_file = './env_files/.env'
        self.config = set_env(logger=self.logger, env_file_path=env_file, config_folder_name='configs')
        self.query_handler = ApiQuery(global_config, global_logger)

    @ns_owner.doc(body=owner_phone_req_payload)  # request payload schema
    @ns_owner.response(200, 'Success', response_format)
    def post(self):
        """
        given owner's phone number, return apartments
        """
        # check keys in the requested payload
        necessary_keys = {'phone'}  # a set

        start = time.time()
        payload = request.json

        if necessary_keys.issubset(payload.keys()):
            self.logger.info("payload: {payload_type}, {payload}".format(payload_type=type(payload), payload=payload))
            res = self.query_handler.query_owner_phone(phone=payload['phone'])
            # res = self.query_handler.query_owner_phone(phone='0933-668-596')  # FIXME: test
            end = time.time()
            time_elapsed_api = float("{:.6f}".format(end - start))

            if res:
                result = {
                    "code": 0,
                    "message": "success",
                    "data": res,  # [dumps(x) for x in res],
                    "time_elapsed_api": time_elapsed_api,
                }
                self.logger.info('time_elapsed_api: {}'.format(time_elapsed_api))
            else:
                result = {
                    "code": -1,
                    "message": "failed",
                    "data": None,
                    "time_elapsed_api": 0,
                }
                self.logger.warning('no such data in db!')

            # # The return type must be a string, dict, tuple, Response instance, or WSGI callable - rest api outputs json
            result = jsonify(result)

            return result
        else:
            status_code = 400
            message_400 = '''{status_code} Bad Request. Please make sure your data payload with columns: {col}'''.format(
                status_code=status_code, col=necessary_keys)
            raise InvalidUsage(message_400, status_code=status_code)


@ns_owner.route('/identity', strict_slashes=False)
class OwnerIdentity(Resource):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = Logger().get_logger('api')
        env_file = './env_files/.env'
        self.config = set_env(logger=self.logger, env_file_path=env_file, config_folder_name='configs')
        self.query_handler = ApiQuery(global_config, global_logger)

    @ns_owner.doc(body=owner_identity_req_payload)  # request payload schema
    @ns_owner.response(200, 'Success', response_format)
    def post(self):
        """
        given owner's identity (using positive or negative listing), return apartments
        """
        # check keys in the requested payload
        necessary_keys = {'negative_id_lst'}  # a set

        start = time.time()
        payload = request.json

        if necessary_keys.issubset(payload.keys()):
            self.logger.info("payload: {payload_type}, {payload}".format(payload_type=type(payload), payload=payload))
            res = self.query_handler.query_owner_identity(negative_id_lst=payload['negative_id_lst'])
            # res = self.query_handler.query_owner_phone(phone='0933-668-596')  # FIXME: test
            end = time.time()
            time_elapsed_api = float("{:.6f}".format(end - start))

            if res:
                result = {
                    "code": 0,
                    "message": "success",
                    "data": res,  # [dumps(x) for x in res],
                    "time_elapsed_api": time_elapsed_api,
                }
                self.logger.info('time_elapsed_api: {}'.format(time_elapsed_api))
            else:
                result = {
                    "code": -1,
                    "message": "failed",
                    "data": None,
                    "time_elapsed_api": 0,
                }
                self.logger.warning('no such data in db!')

            # # The return type must be a string, dict, tuple, Response instance, or WSGI callable - rest api outputs json
            result = jsonify(result)

            return result
        else:
            status_code = 400
            message_400 = '''{status_code} Bad Request. Please make sure your data payload with columns: {col}'''.format(
                status_code=status_code, col=necessary_keys)
            raise InvalidUsage(message_400, status_code=status_code)


@ns_owner.route('/gender/last-name', strict_slashes=False)
class OwnerGenderLastname(Resource):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = Logger().get_logger('api')
        env_file = './env_files/.env'
        self.config = set_env(logger=self.logger, env_file_path=env_file, config_folder_name='configs')
        self.query_handler = ApiQuery(global_config, global_logger)

    @ns_owner.doc(body=owner_gender_lastname_req_payload)  # request payload schema
    @ns_owner.response(200, 'Success', response_format)
    def post(self):
        """
        given city, owner's gender and last name, return apartments
        """
        # check keys in the requested payload
        necessary_keys = {'city', 'owner_gender', 'owner_last_name'}  # a set

        start = time.time()
        payload = request.json

        if necessary_keys.issubset(payload.keys()):
            self.logger.info("payload: {payload_type}, {payload}".format(payload_type=type(payload), payload=payload))
            res = self.query_handler.query_owner_gender_last_name(payload['city'], payload['owner_gender'],
                                                                  payload['owner_last_name'])
            # res = self.query_handler.query_owner_gender_last_name('new_taipei_city', '男', '楊')  # FIXME: test
            end = time.time()
            time_elapsed_api = float("{:.6f}".format(end - start))

            if res:
                result = {
                    "code": 0,
                    "message": "success",
                    "data": res,  # [dumps(x) for x in res],
                    "time_elapsed_api": time_elapsed_api,
                }
                self.logger.info('time_elapsed_api: {}'.format(time_elapsed_api))
            else:
                result = {
                    "code": -1,
                    "message": "failed",
                    "data": None,
                    "time_elapsed_api": 0,
                }
                self.logger.warning('no such data in db!')

            # # The return type must be a string, dict, tuple, Response instance, or WSGI callable - rest api outputs json
            result = jsonify(result)

            return result
        else:
            status_code = 400
            message_400 = '''{status_code} Bad Request. Please make sure your data payload with columns: {col}'''.format(
                status_code=status_code, col=necessary_keys)
            raise InvalidUsage(message_400, status_code=status_code)


if __name__ == '__main__':
    server_port = '30000'  # FIXME: it should be in config settings

    api.add_namespace(ns_renter)
    api.add_namespace(ns_owner)

    app.run(debug=True, host='0.0.0.0', port=server_port)

"""
Sample request payload

===

api health check.

curl \
-H "Content-Type: application/json" \
-X GET \
http://127.0.0.1:30000/healthcheck/

======================================================

curl \
-H "Content-Type: application/json" \
-d '{"city": "taipei_city", "gender":"男"}' \
-X POST \
http://127.0.0.1:30000/api/v1/renter/gender

===

curl \
-H "Content-Type: application/json" \
-d '{"phone": "0905-059-091"}' \
-X POST \
http://127.0.0.1:30000/api/v1/owner/phone

===

curl \
-H "Content-Type: application/json" \
-d '{"negative_id_lst": ["屋主"]}' \
-X POST \
http://127.0.0.1:30000/api/v1/owner/identity

===

curl \
-H "Content-Type: application/json" \
-d '{"city": "new_taipei_city", "owner_gender": "男", "owner_last_name": "楊"}' \
-X POST \
http://127.0.0.1:30000/api/v1/owner/gender/last-name

"""
