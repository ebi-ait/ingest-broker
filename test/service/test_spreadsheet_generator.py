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

    def test_spreadsheet_spec_json_deserialization(self):
        test_spreadsheet_spec_json = {
            "types": [{
                "schemaName": "donor_organism",
                "excludeFields": ["human_specific"],
                "excludeModules": ["death"],
                "embedProcess": True,
                "linkSpec": {
                    "linkEntities": [],
                    "linkProtocols": ["biomaterial_collection_protocol"]
                }}]
        }

        deserialized_spec = SpreadsheetSpec.from_dict(test_spreadsheet_spec_json)
        self.assertTrue(len(deserialized_spec.types) == 1)
        self.assertTrue(deserialized_spec.types[0].schema_name == "donor_organism")
        self.assertTrue("human_specific" in deserialized_spec.types[0].exclude_fields)
        self.assertTrue("death" in deserialized_spec.types[0].exclude_modules)
        self.assertTrue(deserialized_spec.types[0].link_spec is not None)
        self.assertTrue(len(deserialized_spec.types[0].link_spec.link_entities) == 0)
        self.assertTrue("biomaterial_collection_protocol" in deserialized_spec.types[0].link_spec.link_protocols)

    def test_spreadsheet_spec_hashcode(self):
        test_type_spec_1 = TypeSpec("project", [], [], False, LinkSpec(["specimen_from_organism"], []))
        test_type_spec_2 = TypeSpec("specimen_from_organism", [], [], True, LinkSpec(["donor_organism"], []))

        spreadsheet_spec = SpreadsheetSpec([test_type_spec_1, test_type_spec_2])
        spreadsheet_spec_dict = spreadsheet_spec.to_dict()

        self.assertEqual(spreadsheet_spec.hashcode(), SpreadsheetSpec.from_dict(spreadsheet_spec_dict).hashcode())

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
