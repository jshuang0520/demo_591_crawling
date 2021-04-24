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
                        env_file_path=str(dir_path.split('app')[0])+'env_files/dev/.env',
                        config_folder_name='configs')

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False  # make Han characters recognizable
app_healthcheck = Flask(__name__)
app_healthcheck.config['JSON_AS_ASCII'] = False
version = 'v1'
repo_tag = 'latest'
server_url = '<server_url>'

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
                                                            example="100"))
                                   )
# response
health_response = api.model('HealthResponse',
                            dict(response=fields.Integer(required=False,
                                                         description='response code',
                                                         example=200)
                                 )

                            )

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
    message = dict(version_info='591 House Renting - API Version:{version}, :{tag}\n'.format(version=version, tag=repo_tag),
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

    @ns_renter.response(200, 'Success')
    def post(self):
        """
        given city of house, gender of renter,
        find apratments
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

            if res['data']:
                result = {
                    "code": 0,
                    "message": "success",
                    "data": res['data'],  # [dumps(x) for x in res['data']],
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

    @ns_owner.response(200, 'Success')
    def post(self):
        """
        given city of house, gender of owner,
        find apratments
        """
        # check keys in the requested payload
        necessary_keys = {'phone'}  # a set

        start = time.time()
        payload = request.json

        if necessary_keys.issubset(payload.keys()):
            self.logger.info("payload: {payload_type}, {payload}".format(payload_type=type(payload), payload=payload))
            res = self.query_handler.query_owner_phone(phone='0933-668-596')  # FIXME: test
            end = time.time()
            time_elapsed_api = float("{:.6f}".format(end - start))

            if res['data']:
                result = {
                    "code": 0,
                    "message": "success",
                    "data": res['data'],  # [dumps(x) for x in res['data']],
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
    server_port = '30000'  # FIXME: it should be port 30000

    api.add_namespace(ns_renter)
    api.add_namespace(ns_owner)

    app.run(debug=True, host='0.0.0.0', port=server_port)

"""
Sample request payload


api_1. (local query, with level 1, 2, 3, 4)

curl \
-X POST \
-H "Content-Type: application/json" \
-d '{"coordinates": {"latitude":"24.2386","longitude":"120.855"}, "level":"1", "offset":"0", "limit":"100"}' \
http://127.0.0.1:30000/api/v1/living-area/scooters/

===

api health check.

curl -d '{"offset":"1", "limit":"100"}' -H "Content-Type: application/json" -X GET http://127.0.0.1:30000/healthcheck/

===

curl \
-X POST \
-H "Content-Type: application/json" \
-d '{"owner": "屋主 黃先生", "gender":"男"}' \
http://127.0.0.1:30000/api/v1/591-housing/renter/

===

curl \
-X POST \
-H "Content-Type: application/json" \
-d '{"city": "taipei_city", "gender":"男"}' \
http://127.0.0.1:30000/api/v1/renter/gender

===

curl \
-X POST \
-H "Content-Type: application/json" \
-d '{"phone": "0905-059-091"}' \
http://127.0.0.1:30000/api/v1/owner/phone


"""