from ingest.api.ingestapi import IngestApi
from ingest.template.schema_template import SchemaTemplate
from ingest.template.vanilla_spreadsheet_builder import VanillaSpreadsheetBuilder

from broker.service.spreadsheet_generation.schema_spec.schema_spec import SchemaSpec, ParseUtils, FieldSpec, ObjectSpec, StringSpec, IntegerSpec, NumberSpec, OntologySpec, BooleanSpec

from dataclasses import dataclass
from io import FileIO
from typing import List, Dict, Optional, Union

from functools import reduce
from operator import iconcat

import tempfile
import yaml

@dataclass
class TabColumn:
    name: str
    description: str
    example: str
    path: str


@dataclass
class TemplateTab:
    schema_name: str
    display_name: str
    columns: List[str]


@dataclass
class TemplateYaml:
    tabs: List[TemplateTab]

    def to_yml_dict(self) -> Dict:
        return {
            "tabs": [{
                tab.schema_name: {
                    "columns": tab.columns,
                    "display_name": tab.display_name
                }
            } for tab in self.tabs]
        }


@dataclass
class LinkSpec:
    link_entities: List[str]
    link_protocols: List[str]


@dataclass
class TypeSpec:
    schema_name: str
    exclude_modules: List[str]
    exclude_fields: List[str]
    embed_process: bool
    link_spec: Optional[LinkSpec]


@dataclass
class SpreadsheetSpec:
    types: List[TypeSpec]


@dataclass
class ParsedTab:
    """
    Represents the layout of a spreadsheet tab.

    sub_tabs refers to the mechanism whereby modules are represented in adjacent tabs e.g a ParsedTab for Project
    would have Contributors in its sub_tabs.
    """
    schema_name: str
    display_name: str
    columns: List[TabColumn]
    sub_tabs: List['ParsedTab']


class SpreadsheetGenerator:

    def __init__(self, ingest_api: IngestApi):
        self.ingest_api = ingest_api
        self.schema_template = SchemaTemplate(ingest_api_url=ingest_api.url)

    def generate(self, spreadsheet_spec: SpreadsheetSpec) -> FileIO:
        parsed_tabs = []
        for type_spec in spreadsheet_spec.types:
            tab_for_type = self.tab_for_type(type_spec)
            parsed_tabs.append(tab_for_type)

        template_tabs = self.template_tabs_from_parsed_tabs(parsed_tabs)

        yml = TemplateYaml(template_tabs)

        return self.spreadsheet_from_template_yaml(yml)

    def spreadsheet_from_template_yaml(self, template_yaml: TemplateYaml) -> FileIO:
        with tempfile.NamedTemporaryFile('w') as yaml_file:
            yaml.dump(template_yaml.to_yml_dict(), yaml_file)
            spreadsheet_file = tempfile.NamedTemporaryFile('w')
            with open("ss.xlsx", "w") as f:
                spreadsheet_builder = VanillaSpreadsheetBuilder(f.name, True)
                spreadsheet_builder.generate_spreadsheet(tabs_template=yaml_file.name,
                                                         schema_urls=self.schema_template.metadata_schema_urls,
                                                         include_schemas_tab=True)
                spreadsheet_builder.save_spreadsheet()

    def tab_for_type(self, type_spec: TypeSpec) -> ParsedTab:
        schema_name = type_spec.schema_name
        schema_properties = self.schema_template.meta_data_properties[schema_name]
        schema_spec = ParseUtils.parse_schema_spec(schema_name, schema_properties)

        parsed_tab = self._generate_tab(self.tab_name_for_type(schema_spec), schema_spec,
                                        exclude_modules=type_spec.exclude_modules,
                                        exclude_fields=type_spec.exclude_fields,
                                        context=[schema_name])
        parsed_tab.columns.extend(self.links_for_tab(type_spec))
        parsed_tab.columns.extend(self.process_columns() if type_spec.embed_process else [])

        return parsed_tab

    def _generate_tab(self, tab_name: str, schema_spec: SchemaSpec, exclude_modules: List[str], exclude_fields: List[str], context: List[str]) -> ParsedTab:
        columns: List[TabColumn] = []
        subtabs: List[ParsedTab] = []

        fields = self.exclude_fields(schema_spec.fields, exclude_modules, exclude_fields)
        for field in fields:
            if self.field_is_ontology(field):
                columns.extend(self.columns_for_ontology_module(field, context))
            elif self.field_is_object(field):
                if field.multivalue:
                    # generate sub-tabs for this multivalue module
                    subtab_name = f'{tab_name} - {self.tab_name_for_sub_module(schema_spec, field)}'
                    subtab = self._generate_tab(subtab_name, field, exclude_modules, exclude_fields, context=context + [field.field_name])
                    subtabs.append(subtab)
                else:
                    columns.extend(self.columns_for_field(field, context=context + [field.field_name]))
            elif self.field_is_atomic(field):
                columns.append(self.parse_atomic_column(field, context + [field.field_name]))

        return ParsedTab(schema_spec.field_name, tab_name, columns, subtabs)

    # TODO: make this a method of SchemaTemplate, and refactor SchemaTemplate.tabs to make this less nutty
    def tab_name_for_type(self, schema_spec: SchemaSpec) -> str:
        tab_config_entry = list(filter(lambda entry: schema_spec.field_name in entry, self.schema_template.tabs))[0]
        tab_display_name = tab_config_entry[schema_spec.field_name]["display_name"]
        return tab_display_name

    def tab_name_for_sub_module(self, parent_schema: SchemaSpec, sub_module: SchemaSpec):
        try:
            parent_schema_json = [s for s in self.schema_template.json_schemas
                                  if "name" in s and s["name"] == parent_schema.field_name][0]
            sub_schema_display_name = parent_schema_json["properties"][sub_module.field_name]["user_friendly"]
            return sub_schema_display_name
        except (IndexError, KeyError):
            return sub_module.field_name

    def links_for_tab(self, type_spec: TypeSpec) -> List[TabColumn]:
        link_spec = type_spec.link_spec
        if not link_spec:
            return []
        else:
            return [self.link_column_for_schema(ParseUtils.parse_schema_spec(entity, self.schema_template.meta_data_properties[entity]))
                     for entity in link_spec.link_entities]

    def link_column_for_schema(self, schema_spec: SchemaSpec) -> TabColumn:
        display_name = self.tab_name_for_type(schema_spec)

        if schema_spec.domain_entity == "biomaterial":
            return TabColumn(name=f'{display_name} - ID',
                             description="A Biomaterial id",
                             example="ABC12345",
                             path=f'{schema_spec.field_name}.biomaterial_core.biomaterial_id')
        elif schema_spec.domain_entity == "file":
            return TabColumn(name=f'{display_name} - ID',
                             description="A file ID",
                             example="ABC12345",
                             path=f'{schema_spec.field_name}.file_core.file_id')
        elif schema_spec.domain_entity == "protocol":
            return TabColumn(name=f'{display_name} - ID',
                             description="A protocol ID",
                             example="ABC12345",
                             path=f'{schema_spec.field_name}.protocol.protocol_id')
        elif schema_spec.domain_entity == "process":
            return TabColumn(name=f'{display_name} - ID',
                             description="A process ID",
                             example="ABC12345",
                             path=f'{schema_spec.field_name}.process.process_id')
        else:
            raise


    @staticmethod
    def columns_for_ontology_module(ontology_spec: OntologySpec, context: List[str]) -> List[TabColumn]:
        text_column_context = context + [ontology_spec.field_name, ontology_spec.text_field.field_name]
        text_column = TabColumn(name=ontology_spec.text_field.user_friendly,
                                description=ontology_spec.text_field.description,
                                example=ontology_spec.text_field.example,
                                path=SpreadsheetGenerator.context_to_path_string(text_column_context))

        ontology_column_context = context + [ontology_spec.field_name, ontology_spec.ontology_field.field_name]
        ontology_column = TabColumn(name=ontology_spec.ontology_field.user_friendly,
                                    description=ontology_spec.ontology_field.description,
                                    example=ontology_spec.ontology_field.example,
                                    path=SpreadsheetGenerator.context_to_path_string(ontology_column_context))

        label_column_context = context + [ontology_spec.field_name, ontology_spec.ontology_label.field_name]
        label_column = TabColumn(name=ontology_spec.ontology_label.user_friendly,
                                 description=ontology_spec.ontology_label.description,
                                 example=ontology_spec.ontology_label.example,
                                 path=SpreadsheetGenerator.context_to_path_string(label_column_context))

        return [text_column, ontology_column, label_column]

    @staticmethod
    def columns_for_field(field: FieldSpec, context: List[str]) -> List[TabColumn]:
        if SpreadsheetGenerator.field_is_atomic(field):
            return [SpreadsheetGenerator.parse_atomic_column(field, context)]
        elif SpreadsheetGenerator.field_is_object(field):
            return SpreadsheetGenerator.flatten([SpreadsheetGenerator.columns_for_field(field, context + [field.field_name])
                                                 for field in field.fields])

    def process_columns(self) -> List[TabColumn]:
        process_schema_spec = ParseUtils.parse_schema_spec("process", self.schema_template.meta_data_properties["process"])
        return SpreadsheetGenerator.columns_for_field(process_schema_spec, ["process"])

    @staticmethod
    def field_is_atomic(field: FieldSpec) -> bool:
        return isinstance(field, StringSpec) or isinstance(field, NumberSpec) or \
               isinstance(field, IntegerSpec) or isinstance(field, BooleanSpec)

    @staticmethod
    def field_is_object(field: FieldSpec) -> bool:
        return isinstance(field, ObjectSpec)

    @staticmethod
    def field_is_ontology(field: FieldSpec) -> bool:
        return isinstance(field, OntologySpec)

    @staticmethod
    def parse_atomic_column(field_spec: Union[StringSpec, NumberSpec], context: List[str]) -> TabColumn:
        return TabColumn(name=field_spec.user_friendly,
                         description=field_spec.description,
                         example=field_spec.example,
                         path=SpreadsheetGenerator.context_to_path_string(context))

    @staticmethod
    def context_to_path_string(context: List[str]) -> str:
        """
        e.g given context = ["process", "process_core", "process_id"], return "process.process_core.process_id"
        e.g given ["process"], return just "process"
        """
        if len(context) == 0:
            return ""
        elif len(context) == 1:
            return context[0]
        else:
            return f'{context[0]}.{SpreadsheetGenerator.context_to_path_string(context[1:])}'


    @staticmethod
    def exclude_fields(field_specs: List[FieldSpec], modules_to_exclude: List[str], fields_to_exclude: List[str]) -> List[FieldSpec]:
        return [f for f in field_specs if f.field_name not in modules_to_exclude and f.field_name not in fields_to_exclude]

    @staticmethod
    def template_tabs_from_parsed_tabs(parsed_tabs: List[ParsedTab]) -> List[TemplateTab]:
        all_tabs = SpreadsheetGenerator.flatten([[tab] + tab.sub_tabs for tab in parsed_tabs])
        return [TemplateTab(t.schema_name, t.display_name, [col.path for col in t.columns])
                for t in all_tabs]

    @staticmethod
    def flatten(list_of_lists: List[List]) -> List:
        """
        e.g given [[1,2], [3,4], [5,6,7]] return [1,2,3,4,5,6,7]
        """
        return reduce(iconcat, list_of_lists, [])
