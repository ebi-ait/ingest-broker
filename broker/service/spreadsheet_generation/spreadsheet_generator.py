from ingest.api.ingestapi import IngestApi
from ingest.template.schema_template import SchemaTemplate
from ingest.template.vanilla_spreadsheet_builder import VanillaSpreadsheetBuilder
from ingest.template.tab_config import TabConfig

from broker.service.spreadsheet_generation.schema_spec import SchemaSpec, ParseUtils, FieldSpec, ObjectSpec, StringSpec, IntegerSpec, NumberSpec, OntologySpec, BooleanSpec

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Union

from functools import reduce
from operator import iconcat

import tempfile
import yaml
from hashlib import md5
from collections import OrderedDict
import json


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
    link_entities: List[str] = field(default_factory=list)
    link_protocols: List[str] = field(default_factory=list)

    @staticmethod
    def from_dict(data: Dict) -> 'LinkSpec':
        link_entities = data.get("linkEntities", [])
        link_protocols = data.get("linkProtocols", [])
        return LinkSpec(link_entities, link_protocols)

    def to_json_dict(self) -> Dict:
        return OrderedDict({
            "linkEntities": self.link_entities,
            "linkProtocols": self.link_protocols
        })


class IncludeAllModules:
    pass


@dataclass
class IncludeSomeModules:
    modules: List[str]


IncludeModules = Union[IncludeAllModules, IncludeSomeModules]


@dataclass
class TypeSpec:
    schema_name: str = ''
    embed_process: bool = False
    include_modules: IncludeModules = field(default_factory=list)
    link_spec: Optional[LinkSpec] = LinkSpec()

    @staticmethod
    def from_json_dict(data: Dict) -> 'TypeSpec':
        try:
            link_spec = LinkSpec.from_dict(data["linkSpec"]) if data.get("linkSpec") else None
            include_modules = IncludeSomeModules(data["includeModules"]) if isinstance(data["includeModules"], list) else IncludeAllModules()
            return TypeSpec(data["schemaName"], include_modules, data["embedProcess"], link_spec)
        except (IndexError, KeyError) as e:
            raise

    def to_json_dict(self) -> Dict:
        return OrderedDict({
            "schemaName": self.schema_name,
            "includeModules": self.include_modules.modules if isinstance(self.include_modules, IncludeSomeModules) else "ALL",
            "embedProcess": self.embed_process,
            "linkSpec": self.link_spec.to_json_dict() if self.link_spec is not None else None
        })


@dataclass
class SpreadsheetSpec:
    types: List[TypeSpec]

    @staticmethod
    def from_dict(data: Dict) -> 'SpreadsheetSpec':
        try:
            return SpreadsheetSpec([TypeSpec.from_json_dict(type_data) for type_data in data["types"]])
        except (IndexError, KeyError) as e:
            raise e

    def to_dict(self) -> Dict:
        return OrderedDict({
            "types": [TypeSpec.to_json_dict(t_spec) for t_spec in self.types]
        })

    def hashcode(self) -> str:
        """
        generate an md5 checksum of this spec, for purpose of identifying spreadsheets generated from this spec
        """
        spreadsheet_spec_dict = self.to_dict()
        sorted_keys = dict(sorted(spreadsheet_spec_dict.items()))
        return md5(json.dumps(sorted_keys).encode("utf-8")).hexdigest()


@dataclass
class ParsedTab:
    """
    Represents the layout of a spreadsheet tab.

    sub_tabs refers to the mechanism whereby modules are represented in adjacent tabs e.g a ParsedTab for Project
    might have Contributors, Funders, and Publications in its sub_tabs.
    """
    schema_name: str
    display_name: str
    columns: List[TabColumn]
    sub_tabs: List['ParsedTab']


class UnknownSchemaException(Exception):
    pass


class SpreadsheetGenerator:

    def __init__(self, ingest_api: IngestApi):
        self.ingest_api = ingest_api
        self.schema_template = SchemaTemplate(ingest_api_url=ingest_api.url)

    def generate(self, spreadsheet_spec: SpreadsheetSpec, output_file_path: Optional[str]) -> str:
        parsed_tabs = []
        for type_spec in spreadsheet_spec.types:
            tab_for_type = self.tab_for_type(type_spec)
            parsed_tabs.append(tab_for_type)

        template_tabs = self.template_tabs_from_parsed_tabs(parsed_tabs)

        yml = TemplateYaml(template_tabs)

        return self.spreadsheet_from_template_yaml(yml, output_file_path)

    def spreadsheet_from_template_yaml(self, template_yaml: TemplateYaml, output_file_path: Optional[str]) -> str:
        with tempfile.NamedTemporaryFile('w') as yaml_file:
            yaml.dump(template_yaml.to_yml_dict(), yaml_file)
            tab_config = TabConfig().load(yaml_file.name)
            spreadsheet_file = open(output_file_path, "w") if output_file_path is not None else tempfile.NamedTemporaryFile('w')

            spreadsheet_builder = VanillaSpreadsheetBuilder(spreadsheet_file.name, True)
            spreadsheet_builder.include_schemas_tab = True
            spreadsheet_builder.build(SchemaTemplate(self.ingest_api.url, json_schema_docs=self.schema_template.json_schemas, tab_config=tab_config))
            spreadsheet_builder.save_spreadsheet()
            spreadsheet_file.close()

            return spreadsheet_file.name

    def tab_for_type(self, type_spec: TypeSpec) -> ParsedTab:
        schema_name = type_spec.schema_name
        schema_properties = self.metadata_properties_for_type(schema_name)
        schema_spec = ParseUtils.parse_schema_spec(schema_name, schema_properties)

        parsed_tab = self._generate_tab(self.tab_name_for_type(schema_spec), schema_spec,
                                        include_modules=type_spec.include_modules,
                                        context=[schema_name])
        parsed_tab.columns.extend(self.links_for_tab(type_spec))
        parsed_tab.columns.extend(self.process_columns() if type_spec.embed_process else [])

        return parsed_tab

    def _generate_tab(self, tab_name: str, schema_spec: SchemaSpec, include_modules: IncludeModules, context: List[str]) -> ParsedTab:
        columns: List[TabColumn] = []
        subtabs: List[ParsedTab] = []

        fields = self.filter_fields(schema_spec.fields, include_modules)
        for field in fields:
            if self.field_is_ontology(field):
                columns.extend(self.columns_for_ontology_module(field, context))
            elif self.field_is_object(field):
                if field.multivalue:
                    if field.field_name in ['reagents', 'familial_relationships']:
                        columns.extend(self.columns_for_field(field, context=context + [field.field_name]))
                    else:
                        # generate sub-tabs for this multivalue module
                        subtab_name = f'{tab_name} - {self.tab_name_for_sub_module(schema_spec, field)}'
                        subtab = self._generate_tab(subtab_name, field, IncludeAllModules(),
                                                    context=context + [field.field_name])
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

    def metadata_properties_for_type(self, schema_name: str) -> Dict:
        try:
            return self.schema_template.meta_data_properties[schema_name]
        except KeyError as e:
            raise UnknownSchemaException(f'Unknown schema: {schema_name}')

    def links_for_tab(self, type_spec: TypeSpec) -> List[TabColumn]:
        link_spec = type_spec.link_spec
        if not link_spec:
            return []
        else:
            return [self.link_column_for_schema(ParseUtils.parse_schema_spec(entity, self.metadata_properties_for_type(entity)))
                    for entity in link_spec.link_entities + link_spec.link_protocols]

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
        elif "protocol" in schema_spec.domain_entity:  # the domain entity for protocol schemas can be something like "protocol/imaging"
            return TabColumn(name=f'{display_name} - ID',
                             description="A protocol ID",
                             example="ABC12345",
                             path=f'{schema_spec.field_name}.protocol_core.protocol_id')
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
        process_schema_spec = ParseUtils.parse_schema_spec("process", self.metadata_properties_for_type("process"))
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
    def filter_fields(field_specs: List[FieldSpec], modules_to_include: IncludeModules) -> List[FieldSpec]:
        if isinstance(modules_to_include, IncludeAllModules):
            return field_specs
        else:
            return [f for f in field_specs if f.field_name in modules_to_include.modules]

    @staticmethod
    def template_tabs_from_parsed_tabs(parsed_tabs: List[ParsedTab]) -> List[TemplateTab]:
        all_tabs = SpreadsheetGenerator.flatten([[tab] + tab.sub_tabs for tab in parsed_tabs])
        return [TemplateTab(t.schema_name, SpreadsheetGenerator.chomp_32(t.display_name), [col.path for col in t.columns])
                for t in all_tabs]

    @staticmethod
    def flatten(list_of_lists: List[List]) -> List:
        """
        e.g given [[1,2], [3,4], [5,6,7]] return [1,2,3,4,5,6,7]
        """
        return reduce(iconcat, list_of_lists, [])

    @staticmethod
    def chomp_32(string: str) -> str:
        """
        Excel spreadsheets disallow tab names with length > 32.
        This method chomps strings greater than size 32, setting the 29th, 30th and 31st chars
        to ellipses

        e.g given "<long string> something", returns "<long_string> som..."
        """
        return string if len(string) < 32 else str(string[:28]) + "..."
