# RDF Label Indexer

Takes all RDF triples in an ntriple file and indexes (Label, IRI) tuple objects into elasticsearch.
The motivation is for ease of fuzzy search on the label field, to retrieve the uri of the resource for yoda's label lookup service.

Later we might want to also index the entire graph into ES for ease of search? 