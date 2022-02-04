# --- core imports
from http import HTTPStatus
import re
import tempfile

# --- application imports
from broker.common.util import response_json

# --- third-party imports
from flask import Blueprint, send_file, request
from flask_cors import cross_origin
from geo_to_hca import geo_to_hca

import_geo_bp = Blueprint(
    'import_geo', __name__, url_prefix='/'
)


def is_valid_geo_accession(geo_accession):
    regex = re.compile("^GSE.*$")
    return bool(regex.match(geo_accession))


@import_geo_bp.route("/import-geo", methods=['POST'])
@cross_origin()
def get_spreadsheet_using_geo():
    args = request.args
    geo_accession = args.get('accession')

    if not is_valid_geo_accession(geo_accession):
        return response_json(HTTPStatus.BAD_REQUEST, "The given geo accession is invalid")

    try:
        workbook = geo_to_hca.create_spreadsheet_using_geo_accession(geo_accession)

        if workbook:
            temp_file = tempfile.NamedTemporaryFile()
            filename = f"hca_metadata_spreadsheet-{geo_accession}.xlsx"
            workbook.save(temp_file.name)

            return send_file(temp_file.name,
                             as_attachment=True,
                             cache_timeout=0,
                             attachment_filename=filename)

    except Exception:
        return response_json(HTTPStatus.INTERNAL_SERVER_ERROR, "Unable to find HCA metadata against given accession")
