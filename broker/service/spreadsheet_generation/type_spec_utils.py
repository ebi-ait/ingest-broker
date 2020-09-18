from collections import namedtuple, OrderedDict

Data = namedtuple('Data', ['spec', 'back_links'])


def sort(specs):
    adjacency = OrderedDict()
    roots = []
    for spec in specs:
        data = adjacency.get(spec.schema_name)
        if data is None:
            adjacency[spec.schema_name] = Data(spec, [])
        elif data.spec is None:
            data = Data(spec, data.back_links)
            adjacency[spec.schema_name] = data

        if spec.link_spec is not None:
            if len(spec.link_spec.link_entities) == 0:
                roots.append(spec)
            else:
                for link_entity in spec.link_spec.link_entities:
                    if link_entity not in adjacency.keys():
                        adjacency[link_entity] = Data(None, [])
                    adjacency[link_entity].back_links.append(spec.schema_name)

    if len(roots) > 0:
        specs.clear()
    deque = []
    for root in roots:
        deque.append(root)
        while len(deque) > 0:
            next_spec = deque.pop(0)
            if next_spec.schema_name in adjacency.keys():
                specs.append(next_spec)
                data = adjacency[next_spec.schema_name]
                undiscovered_links = [link for link in data.back_links if link in adjacency.keys()]
                for entity_link in undiscovered_links:
                    deque.append(adjacency[entity_link].spec)
                del adjacency[next_spec.schema_name]  # visited
