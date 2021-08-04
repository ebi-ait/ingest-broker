import json
from dataclasses import is_dataclass, asdict

from flask import current_app


def response_json(status_code, data):
    if is_dataclass(data) and not isinstance(data, type):
        data = asdict(data)
    response = current_app.response_class(
        response=json.dumps(data),
        status=status_code,
        mimetype='application/json'
    )
    return response
