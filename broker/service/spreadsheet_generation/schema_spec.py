from dataclasses import dataclass

from typing import List, Optional, ClassVar, Union, Dict

## TODO: At some point, move these definitions to ingest-client's SchemaTemplate libs so that we don't have to parse it all here

@dataclass
class _FieldSpec:
    field_name: str
    multivalue: bool
    description: str
    required: bool
    identifiable: bool
    external_reference: bool

    @property
    def value_type(self): raise NotImplementedError


@dataclass
class _AtomicSpec(_FieldSpec):
    user_friendly: str
    example: Optional[str]

    @property
    def value_type(self): raise NotImplementedError


@dataclass
class StringSpec(_AtomicSpec):
    guidelines: Optional[str]
    value_type: ClassVar[str] = "string"


@dataclass
class NumberSpec(_AtomicSpec):
    value_type: ClassVar[str] = "number"


@dataclass
class IntegerSpec(_AtomicSpec):
    value_type: ClassVar[str] = "integer"


@dataclass
class BooleanSpec(_AtomicSpec):
    value_type: ClassVar[str] = "boolean"


@dataclass
class ObjectSpec(_FieldSpec):
    high_level_entity: str
    domain_entity: str
    module: str
    version: str
    url: str
    fields: List['FieldSpec']
    value_type: ClassVar[str] = "object"

    def from_dict(self, field_name: str, data: Dict) -> 'ObjectSpec':
        try:
            return ParseUtils.parse_object_field(field_name, data)
        except Exception:
            raise


@dataclass
class OntologySpec(ObjectSpec):
    text_field: StringSpec
    ontology_field: StringSpec
    ontology_label: StringSpec


SchemaSpec = ObjectSpec
FieldSpec = Union[StringSpec, NumberSpec, ObjectSpec, IntegerSpec, BooleanSpec, OntologySpec]


class ParseUtils:

    @staticmethod
    def parse_string_field(field_name: str, data: Dict) -> StringSpec:
        return StringSpec(field_name, data["multivalue"], data["description"], data["required"],
                          data["identifiable"], data["external_reference"], data["user_friendly"],
                          data.get("example"), data.get("guidelines"))

    @staticmethod
    def parse_number_field(field_name: str, data: Dict) -> NumberSpec:
        return NumberSpec(field_name, data["multivalue"], data["description"], data["required"], data["identifiable"],
                          data["external_reference"], data["user_friendly"], data["example"])

    @staticmethod
    def parse_integer_field(field_name: str, data: Dict) -> IntegerSpec:
        return IntegerSpec(field_name, data["multivalue"], data["description"], data["required"], data["identifiable"],
                          data["external_reference"], data["user_friendly"], data.get("example"))

    @staticmethod
    def parse_boolean_field(field_name: str, data: Dict) -> BooleanSpec:
        return BooleanSpec(field_name, data["multivalue"], data["description"], data["required"], data["identifiable"],
                           data["external_reference"], data["user_friendly"], data.get("example"))

    @staticmethod
    def parse_object_field(field_name: str, data: Dict) -> ObjectSpec:
        sub_fields = dict(filter(lambda entry: isinstance(entry[1], dict) and "value_type" in entry[1], data.items()))
        field_specs = [ParseUtils.parse_field(entry[0], entry[1]) for entry in sub_fields.items()]
        return ObjectSpec(field_name, data["multivalue"], data["description"], data["required"], data["identifiable"],
                          data["external_reference"], data["schema"]["high_level_entity"],
                          data["schema"]["domain_entity"], data["schema"]["module"], data["schema"]["version"],
                          data["schema"]["url"], field_specs)

    @staticmethod
    def parse_ontology_field(field_name: str, data: Dict) -> OntologySpec:
        sub_fields = dict(filter(lambda entry: isinstance(entry[1], dict) and "value_type" in entry[1], data.items()))
        field_specs = [ParseUtils.parse_field(entry[0], entry[1]) for entry in sub_fields.items()]
        try:
            text_value_spec = list(filter(lambda field_spec: field_spec.field_name == "text", field_specs))[0]
            ontology_value_spec = list(filter(lambda field_spec: field_spec.field_name == "ontology", field_specs))[0]
            ontology_label_spec = list(filter(lambda field_spec: field_spec.field_name == "ontology_label", field_specs))[0]
            return OntologySpec(field_name, data["multivalue"], data["description"], data["required"],
                                data["identifiable"], data["external_reference"], data["schema"]["high_level_entity"],
                                data["schema"]["domain_entity"], data["schema"]["module"], data["schema"]["version"],
                                data["schema"]["url"], field_specs, text_value_spec, ontology_value_spec,
                                ontology_label_spec)
        except IndexError:
            raise

    @staticmethod
    def parse_field(field_name: str, data_dict: Dict) -> FieldSpec:
        value_type = data_dict["value_type"]
        if value_type == "string":
            return ParseUtils.parse_string_field(field_name, data_dict)
        elif value_type == "number":
            return ParseUtils.parse_number_field(field_name, data_dict)
        elif value_type == "integer":
            return ParseUtils.parse_integer_field(field_name, data_dict)
        elif value_type == "boolean":
            return ParseUtils.parse_boolean_field(field_name, data_dict)
        elif value_type == "object":
            if data_dict["schema"]["domain_entity"] == "ontology":
                return ParseUtils.parse_ontology_field(field_name, data_dict)
            else:
                return ParseUtils.parse_object_field(field_name, data_dict)
        else:
            raise Exception(f'Unknown value type "{value_type}", required string, number, or object')

    @staticmethod
    def parse_schema_spec(schema_name: str, data: Dict) -> SchemaSpec:
        return ParseUtils.parse_object_field(schema_name, data)
