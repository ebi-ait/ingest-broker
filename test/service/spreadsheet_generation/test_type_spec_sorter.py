from unittest.case import TestCase

from broker.service.spreadsheet_generation import type_spec_utils
from broker.service.spreadsheet_generation.spreadsheet_generator import TypeSpec, LinkSpec


class TypeSpecSorterTest(TestCase):

    def test_no_links_first(self):
        # given:
        child_1 = TypeSpec(schema_name='child_1')
        child_2 = TypeSpec(schema_name='child_2')
        parent = TypeSpec(schema_name='parent',
                          link_spec=LinkSpec(link_entities=['child_1', 'child_2']))

        # when:
        spec = [parent, child_1, child_2]
        type_spec_utils.sort(spec)

        # then:
        self.assertEqual(3, len(spec))
        self.assertListEqual(spec, [child_1, parent, child_2])
