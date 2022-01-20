# --- core imports
from http import HTTPStatus
import io

# --- application imports
from broker.common.util import response_json

# --- third-party imports
from flask import Blueprint, send_file, request
from flask_cors import cross_origin
from geo_to_hca.geo_to_hca import create_spreadsheet_using_geo_accession


geo_accession_bp = Blueprint(
    'geo_accession', __name__, url_prefix='/geo-accession'
)


@cross_origin()
@geo_accession_bp.route("/spreadsheet")
def get_spreadsheet_using_geo():
    args = request.args
    geo_accession = args.get('accession')

    try:
        workbook = create_spreadsheet_using_geo_accession(geo_accession)

        if workbook:
            file_stream = io.BytesIO()
            workbook.save(file_stream)
            file_stream.seek(0)

            return send_file(
                file_stream,
                attachment_filename=f"hca_metadata_spreadsheet-{geo_accession}.xlsx",
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                cache_timeout=0
            )
    except:
        return response_json(HTTPStatus.NOT_FOUND, "Unable to find HCA metadata against this accession")