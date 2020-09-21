from unittest.case import TestCase

from broker.service.spreadsheet_generation import type_spec_utils
from broker.service.spreadsheet_generation.spreadsheet_generator import TypeSpec, LinkSpec


def _type_spec(name, entity_links=None) -> TypeSpec:
    return TypeSpec(schema_name=name,
                    link_spec=LinkSpec(link_entities=entity_links if entity_links is not None else []))


class TypeSpecSorterTest(TestCase):

    def test_no_links_first(self):
        # given:
        child_1 = _type_spec('child_1')
        child_2 = _type_spec('child_2')
        parent = _type_spec('parent', ['child_1', 'child_2'])

        # when:
        spec = [parent, child_1, child_2]
        type_spec_utils.sort(spec)

        # then:
        self.assertEqual(3, len(spec))
        self.assertListEqual(spec, [child_1, parent, child_2])

    def test_breadth_first_ordering(self):
        # given:
        donor = _type_spec('donor')
        specimen = _type_spec('specimen', [donor.schema_name])
        organoid = _type_spec('organoid', [specimen.schema_name])
        cell_line = _type_spec('cell_line', [specimen.schema_name])
        cell_suspension = _type_spec('cell_suspension', [organoid.schema_name, cell_line.schema_name])
        imaged_specimen = _type_spec('imaged_specimen', [organoid.schema_name])
        sequence_file = _type_spec('sequence_file', [cell_suspension.schema_name])

        # when:
        spec = [specimen, sequence_file, imaged_specimen, donor, organoid, cell_line, cell_suspension]
        type_spec_utils.sort(spec)

        # then:
        self.assertEqual(7, len(spec))
        self.assertListEqual(spec, [donor, specimen, organoid, cell_line, imaged_specimen, cell_suspension,
                                    sequence_file])
