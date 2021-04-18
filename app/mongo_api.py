# -*- coding: utf-8 -*-
import os
import sys
import time
from flask import request, jsonify, Flask
from flask_restplus import fields, Api, Resource, Namespace
sys.path.insert(0, os.path.abspath('..'))
sys.path.insert(0, os.path.abspath('.'))
from src.utility.utils import Logger, set_env
from src.database.mongo.mongodb import MongodbUtility


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
          title='591-Mongodb-Api',
          description='Swagger Doc of 591 House Pricing',
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
        return rv
        # return render_template('500.htm'), 500


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
        healthcheck_logger = Logger().get_logger('db-health-check-api')
        mongodb_handler = MongodbUtility(global_config, global_logger)
        mongodb_handler.logger.info("db status code: 200")  # print("db status code: 200")
        code = 200
        # mongodb_handler.close()
        del mongodb_handler
    except Exception as e:
        # mongodb_handler.logger.error(e)  # if we failed generating mongodb_handler in 'try', we don't have 'logger'!
        exception_logger = Logger().get_logger('living-area')  # logging.getLogger()
        exception_logger.info("error message: {err}".format(err=e))
        code = 400
        del exception_logger
    return str(code)


@app.route('/version/', strict_slashes=False)
def hello_world():
    message = dict(version_info='Living Area API Version:{version}, :{tag}\n'.format(version=version, tag=repo_tag),
                   document_info='Please refer to: ' + server_url + '/swagger/')
    return message


if __name__ == '__main__':
    server_port = '30000'  # FIXME: it should be port 30000

    # api.add_namespace(ns_living_area)
    # api.add_namespace(ns_scooters)

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

"""