from unittest import TestCase, skip
from broker.service.spreadsheet_generation.spreadsheet_generator import SpreadsheetGenerator, SpreadsheetSpec, TypeSpec, LinkSpec, ObjectSpec, IncludeSomeModules, IncludeAllModules, TemplateTab, ParseUtils
from ingest.api.ingestapi import IngestApi

import pandas as pd


class TestSpreadsheetGenerator(TestCase):

    def test_link_column_generation(self):
        cell_suspension_spec = TypeSpec("cell_suspension", IncludeAllModules(), False, LinkSpec(["donor_organism"], []))

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
                "includeModules": ["death"],
                "embedProcess": True,
                "linkSpec": {
                    "linkEntities": [],
                    "linkProtocols": ["biomaterial_collection_protocol"]
                }}]
        }

        deserialized_spec = SpreadsheetSpec.from_dict(test_spreadsheet_spec_json)
        self.assertTrue(len(deserialized_spec.types) == 1)
        self.assertTrue(deserialized_spec.types[0].schema_name == "donor_organism")
        self.assertTrue(isinstance(deserialized_spec.types[0].include_modules, IncludeSomeModules))
        self.assertTrue("death" in deserialized_spec.types[0].include_modules.modules)
        self.assertTrue(deserialized_spec.types[0].link_spec is not None)
        self.assertTrue(len(deserialized_spec.types[0].link_spec.link_entities) == 0)
        self.assertTrue("biomaterial_collection_protocol" in deserialized_spec.types[0].link_spec.link_protocols)

    def test_spreadsheet_spec_hashcode(self):
        test_type_spec_1 = TypeSpec("project", IncludeSomeModules(["contributors"]), False, LinkSpec(["specimen_from_organism"], []))
        test_type_spec_2 = TypeSpec("specimen_from_organism", IncludeAllModules(), True, LinkSpec(["donor_organism"], []))

        spreadsheet_spec = SpreadsheetSpec([test_type_spec_1, test_type_spec_2])
        spreadsheet_spec_dict = spreadsheet_spec.to_dict()

        self.assertEqual(spreadsheet_spec.hashcode(), SpreadsheetSpec.from_dict(spreadsheet_spec_dict).hashcode())

    def test_template_tabs_from_parsed_tabs(self):
        test_type_spec_1 = TypeSpec("project", IncludeAllModules(), False, LinkSpec([], []))
        test_type_spec_2 = TypeSpec("imaged_specimen", IncludeAllModules(), True, LinkSpec([], ["imaging_protocol"]))
        test_type_spec_3 = TypeSpec("imaging_protocol", IncludeAllModules(), False, None)
        test_type_spec_4 = TypeSpec("specimen_from_organism", IncludeAllModules(), True, LinkSpec(["donor_organism"], []))
        test_type_spec_5 = TypeSpec("collection_protocol", IncludeAllModules(), True,LinkSpec(["specimen_from_organism"], []))

        types = [test_type_spec_1,test_type_spec_2,test_type_spec_3,test_type_spec_4,test_type_spec_5]

        ingest_url = "https://api.ingest.dev.archive.data.humancellatlas.org"
        ingest_api = IngestApi(ingest_url)

        parsed_tabs = []
        for type_spec in types:
            tab_for_type = SpreadsheetGenerator(ingest_api).tab_for_type(type_spec)
            parsed_tabs.append(tab_for_type)

        all_tabs = SpreadsheetGenerator.flatten([[tab] + tab.sub_tabs for tab in parsed_tabs])
        template_tabs = [TemplateTab(t.schema_name, SpreadsheetGenerator.chomp_32(t.display_name),
                                     [col.path for col in t.columns]) for t in all_tabs]

        #self.assertTrue("collection_protocol" in [tab.schema_name for tab in template_tabs])
        #self.assertTrue("Collection protocol" in [tab.display_name for tab in template_tabs])
        #self.assertTrue(len([tab.display_name for tab in template_tabs]) == 1)
        self.assertTrue("project" in [tab.schema_name for tab in template_tabs])
        self.assertTrue("contributors" in [tab.schema_name for tab in template_tabs])
        self.assertTrue("Project" in [tab.display_name for tab in template_tabs])
        self.assertTrue(any("specimen_from_organism.biomaterial_core.biomaterial_id" in cols for cols in [tab.columns for tab in template_tabs]))

    def test_temp(self):
        ingest_url = "https://api.ingest.dev.archive.data.humancellatlas.org"
        ingest_api = IngestApi(ingest_url)
        spreadsheet_generator = SpreadsheetGenerator(ingest_api)
        schema_name = "collection_protocol"
        schema_properties = spreadsheet_generator.metadata_properties_for_type(schema_name)
        field_name = schema_name
        data = schema_properties
        items = data.items()
        sub_fields = dict(filter(lambda entry: isinstance(entry[1], dict) and "value_type" in entry[1], data.items()))
        field_specs = [ParseUtils.parse_field(entry[0], entry[1]) for entry in sub_fields.items()]
        schema_spec = ObjectSpec(field_name, data["multivalue"], data["description"], data["required"],
                          data["identifiable"],
                          data["external_reference"], data["schema"]["high_level_entity"],
                          data["schema"]["domain_entity"], data["schema"]["module"], data["schema"]["version"],
                          data["schema"]["url"], field_specs) # field_specs = list of Objects (e.g. sub_fields['protocol_core'], sub_fields['method'], sub_fields['reagents'] for collection_protocol)

        #parsed_tab = spreadsheet_generator._generate_tab(spreadsheet_generator.tab_name_for_type(schema_spec), schema_spec,
                                        #include_modules=type_spec.include_modules,
                                        #context=[schema_name])
        #parsed_tab.columns.extend(self.links_for_tab(type_spec))
        #parsed_tab.columns.extend(self.process_columns() if type_spec.embed_process else [])

        self.assertTrue(schema_properties)
        #parsed_tab = self._generate_tab(self.tab_name_for_type(schema_spec), schema_spec,
                                        #include_modules=type_spec.include_modules,
                                        #context=[schema_name])
        #parsed_tab.columns.extend(self.links_for_tab(type_spec))
        #parsed_tab.columns.extend(self.process_columns() if type_spec.embed_process else [])

    def test_generate(self):
        ingest_url = "https://api.ingest.dev.archive.data.humancellatlas.org"
        ingest_api = IngestApi(ingest_url)
        spreadsheet_generator = SpreadsheetGenerator(ingest_api)

        test_spreadsheet_spec = SpreadsheetSpec(
            [TypeSpec("project", IncludeAllModules(), False, LinkSpec(["specimen_from_organism"], [])),
             TypeSpec("imaged_specimen", IncludeAllModules(), True, LinkSpec([], ["imaging_protocol"])),
             TypeSpec("imaging_protocol", IncludeAllModules(), False, None),
             TypeSpec("specimen_from_organism", IncludeAllModules(), False, LinkSpec(["donor_organism"], [])),
             TypeSpec("collection_protocol", IncludeAllModules(), False, LinkSpec(["specimen_from_organism"], [])),
             TypeSpec("dissociation_protocol", IncludeAllModules(), False, LinkSpec(["specimen_from_organism"], [])),
             TypeSpec("aggregate_generation_protocol", IncludeAllModules(), False, LinkSpec(["specimen_from_organism"], [])),
             TypeSpec("ipsc_induction_protocol", IncludeAllModules(), False,LinkSpec(["specimen_from_organism"], [])),
             TypeSpec("differentiation_protocol", IncludeAllModules(), False,LinkSpec(["specimen_from_organism"], [])),
             TypeSpec("sequence_file", IncludeAllModules(), False, LinkSpec(["cell_suspension"], []))])

        output_filename = spreadsheet_generator.generate(test_spreadsheet_spec, "ss4.xlsx")
        self.assertTrue("ss4.xlsx" in output_filename)

        xls = pd.ExcelFile("ss4.xlsx")
        actual_tab_names = xls.sheet_names

        expected_tab_names1 = ["Project", "Project - Contributors", "Project - Publications",
                               "Project - Funding source(s)",
                               "Imaged specimen", "Imaging protocol", "Imaging protocol - Channel",
                               "Imaging protocol - Probe", "Specimen from organism", "Collection protocol", "Dissociation protocol",
                               "Aggregate generation protocol", "iPSC induction protocol", "Differentiation protocol", "Sequence file", "Schemas"]

        self.assertEqual(actual_tab_names, expected_tab_names1)

        xls = pd.ExcelFile("ss4.xlsx")
        df = pd.read_excel(xls, "Project")
        self.assertEqual(df.columns[0], "PROJECT LABEL (Required)")

        expected_col_names = ["project.project_core.project_short_name", "project.project_core.project_title",
                              "project.project_core.project_description",
                              "project.supplementary_links", "project.insdc_project_accessions",
                              "project.geo_series_accessions", "project.array_express_accessions",
                              "project.insdc_study_accessions", "project.biostudies_accessions",
                              "specimen_from_organism.biomaterial_core.biomaterial_id"]
        actual_col_names = list(df.iloc[2])
        self.assertEqual(expected_col_names, actual_col_names)