from unittest import TestCase
from broker.service.spreadsheet_generation.spreadsheet_generator import SpreadsheetGenerator, SpreadsheetSpec, TypeSpec, LinkSpec
from ingest.api.ingestapi import IngestApi


class TestSpreadsheetGenerator(TestCase):

    def test_link_column_generation(self):
        cell_suspension_spec = TypeSpec("cell_suspension", [], [], False, LinkSpec(["donor_organism"], []))

        ingest_url = "https://api.ingest.dev.archive.data.humancellatlas.org"
        ingest_api = IngestApi(ingest_url)
        parsed_tab = SpreadsheetGenerator(ingest_api).tab_for_type(cell_suspension_spec)

        self.assertTrue("donor_organism.biomaterial_core.biomaterial_id" in [col.path for col in parsed_tab.columns])
