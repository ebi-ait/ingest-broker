import ast
import json
import jsonref


class SchemaService(object):
    def __init__(self):
        self.json_loader = jsonref.JsonLoader()

    def dereference_schema(self, json_schema):
        json_ref_obj = jsonref.loads(json.dumps(json_schema), loader=self.json_loader)
        return json_ref_obj

    def get_json_schema(self, schema_url):
        json_schema = self.json_loader(schema_url)
        return json_schema

    def get_dereferenced_schema(self, schema_url):
        json_schema = self.get_json_schema(schema_url)
        deref_schema_str = str(self.dereference_schema(json_schema))
        deref_schema_json = ast.literal_eval(deref_schema_str)
        return deref_schema_json
