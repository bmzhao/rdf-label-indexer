from rdflib import Graph, RDFS
import sys
import elasticsearch.helpers
from rdflib.namespace import SKOS
from rdflib import Namespace

index_name = 'kb-label-lookup'
label_map_type = 'label-map'  # this "table" in the index serves to map labels to an entity uri

dbpedia_namespace = Namespace('http://dbpedia.org/property/')

#todo normalize the canonical label and description into a separate type under the same index
def create_bulk(nt_triple_file):
    g = Graph()
    g.parse(nt_triple_file, format='nt')
    for (subject, object) in g.subject_objects(RDFS.label):
        yield {
            '_index': index_name,
            '_type': label_map_type,
            '_source': {
                'entity_uri': subject,
                'label': object,
                'canonical_label': g.value(subject=subject, predicate=SKOS.prefLabel),
                'description': g.value(subject=subject, predicate=dbpedia_namespace.description)
            }
        }


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.stderr.write('Usage: python index.py [file1.nt] [file2.nt] ...')
        sys.exit()

    es_client = elasticsearch.Elasticsearch()

    for nt_triple_file in sys.argv[1:]:
        bulk_index_iterator = create_bulk(nt_triple_file)
        elasticsearch.helpers.bulk(es_client, bulk_index_iterator)
