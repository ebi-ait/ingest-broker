# --- core imports
from http import HTTPStatus
import tempfile

# --- application imports
from broker.common.util import response_json

# --- third-party imports
from flask import Blueprint, send_file, request
from flask_cors import cross_origin
from geo_to_hca import geo_to_hca

geo_accession_bp = Blueprint(
    'geo_accession', __name__, url_prefix='/geo-accession'
)


@cross_origin()
@geo_accession_bp.route("/spreadsheet")
def get_spreadsheet_using_geo():
    args = request.args
    geo_accession = args.get('accession')

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