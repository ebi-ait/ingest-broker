from collections import namedtuple

Data = namedtuple('Data', ['spec', 'back_links'])


def sort(specs):
    adjacency = {}
    stack = []
    for spec in specs:
        data = adjacency.get(spec.schema_name)
        if data is None:
            adjacency[spec.schema_name] = Data(spec, set())
        elif data.spec is None:
            data = Data(spec, data.back_links)
            adjacency[spec.schema_name] = data

        if len(spec.link_spec.link_entities) == 0:
            stack.insert(0, spec)  # keep the original ordering
        else:
            for link_entity in spec.link_spec.link_entities:
                if link_entity not in adjacency.keys():
                    adjacency[link_entity] = Data(None, set())
                adjacency[link_entity].back_links.add(spec.schema_name)

    if len(stack) > 0:
        specs.clear()
    while len(stack) > 0:
        next_spec = stack.pop()
        specs.append(next_spec)
        data = adjacency[next_spec.schema_name]
        undiscovered_links = [link for link in data.back_links if link in adjacency.keys()]
        for entity_link in undiscovered_links:
            stack.append(adjacency[entity_link].spec)
        del adjacency[next_spec.schema_name]  # visited
