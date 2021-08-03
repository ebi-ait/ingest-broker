import json
from flask import current_app


def response_json(status_code, data):
    response = current_app.response_class(
        response=json.dumps(data),
        status=status_code,
        mimetype='application/json'
    )
    return response
