from rdflib import Graph, RDFS
import sys
import elasticsearch.helpers
from rdflib.namespace import SKOS

index_name = 'kb-label-lookup'
type_name = 'label-map'


def create_bulk(nt_triple_file):
    g = Graph()
    g.parse(nt_triple_file, format='nt')
    for (subject, object) in g.subject_objects(RDFS.label):
        yield {
            '_index': index_name,
            '_type': type_name,
            '_source':{
                'entity_uri': subject,
                'label': object,
                'canonical_label': g.value(subject=subject,predicate=SKOS.prefLabel)
            }
        }


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.stderr.write('Usage: python index.py [file1.nt] [file2.nt] ...')
        sys.exit()

    es_client = elasticsearch.Elasticsearch()

    for nt_triple_file in sys.argv[1:]:
        bulk_index_iterator = create_bulk(nt_triple_file)
        elasticsearch.helpers.bulk(es_client,bulk_index_iterator)