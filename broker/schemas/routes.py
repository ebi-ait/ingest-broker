import logging
from http import HTTPStatus

from flask import Blueprint, request
from flask import current_app as app

from broker.common.util import response_json
from broker.service.schema_service import SchemaService

schemas_bp = Blueprint(
    'schemas', __name__, url_prefix='/'
)

LOGGER = logging.getLogger(__name__)


# e.g. GET http://0.0.0.0:5000/schemas/json?url=${schemaUrl}&deref

@schemas_bp.route('/schemas/json', methods=['GET'])
def get_schemas():
    args = request.args
    url = args.get('url')
    deref = 'deref' in args
    schema_service = SchemaService()

    if not url:
        return response_json(HTTPStatus.BAD_REQUEST, {'message': 'The "url" request parameter is required'})

    if deref:
        data = schema_service.get_dereferenced_schema(url)
    else:
        data = schema_service.get_json_schema(url)

    return response_json(HTTPStatus.OK, data)


# e.g. GET http://0.0.0.0:5000/schemas/query?high_level_entity=type&domain_entity=biomaterial&concrete_entity=donor_organism&latest

@schemas_bp.route('/schemas/query', methods=['GET'])
def query_schema():
    args = request.args

    # params
    high_level_entity = args.get('high_level_entity')
    domain_entity = args.get('domain_entity')
    concrete_entity = args.get('concrete_entity')
    latest = 'latest' in args

    result = app.ingest_api.get_schemas(
        latest_only=latest,
        high_level_entity=high_level_entity,
        domain_entity=domain_entity,
        concrete_entity=concrete_entity
    )

    return response_json(HTTPStatus.OK, result)

