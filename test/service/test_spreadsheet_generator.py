from unittest import TestCase, skip
from broker.service.spreadsheet_generation.spreadsheet_generator import SpreadsheetGenerator, SpreadsheetSpec, TypeSpec, LinkSpec
from ingest.api.ingestapi import IngestApi


class TestSpreadsheetGenerator(TestCase):

    def test_link_column_generation(self):
        cell_suspension_spec = TypeSpec("cell_suspension", [], [], False, LinkSpec(["donor_organism"], []))

        ingest_url = "https://api.ingest.dev.archive.data.humancellatlas.org"
        ingest_api = IngestApi(ingest_url)
        parsed_tab = SpreadsheetGenerator(ingest_api).tab_for_type(cell_suspension_spec)

        self.assertTrue("donor_organism.biomaterial_core.biomaterial_id" in [col.path for col in parsed_tab.columns])

    def test_context_to_string(self):
        context = ["project", "project_core", "project_shortname"]
        self.assertEqual(SpreadsheetGenerator.context_to_path_string(context), "project.project_core.project_shortname")

        context = ["project"]
        self.assertEqual(SpreadsheetGenerator.context_to_path_string(context), "project")

        context = []
        self.assertEqual(SpreadsheetGenerator.context_to_path_string(context), "")

    @skip
    def test_generate(self):
        ingest_url = "https://api.ingest.dev.archive.data.humancellatlas.org"
        ingest_api = IngestApi(ingest_url)
        spreadsheet_generator = SpreadsheetGenerator(ingest_api)

        test_spreadsheet_spec = SpreadsheetSpec(
            [TypeSpec("project", [], [], False, LinkSpec(["specimen_from_organism"], [])),
             TypeSpec("imaged_specimen", [], [], True, LinkSpec([], ["imaging_protocol"])),
             TypeSpec("imaging_protocol", ["channel"], [], False, None),
             TypeSpec("specimen_from_organism", [], [], True, LinkSpec(["donor_organism"], []))])

        test_spreadsheet = spreadsheet_generator.generate(test_spreadsheet_spec, "ss.xlsx")
