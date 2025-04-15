import sys
from cassandra.cluster import Cluster


class CassandraIndexer:
    """Handles document indexing operations in Cassandra."""

    def __init__(self, keyspace='index_keyspace', hosts=['cassandra-server']):
        """Initialize connection to Cassandra cluster."""
        self.cluster = Cluster(hosts)
        self.session = self.cluster.connect(keyspace)

    def index_term_frequency(self, doc_id, doc_title, term, frequency, corpus="whole_corpus"):
        """Index term frequency within a specific document."""
        self.session.execute(
            """INSERT INTO term_frequency_in_doc 
               (term, corpus_name, doc_id, doc_title, term_frequency) 
               VALUES (%s, %s, %s, %s, %s)""",
            (term, corpus, doc_id, doc_title, frequency)
        )

    def index_document_info(self, doc_id, doc_title, length):
        """Store basic document information."""
        self.session.execute(
            """INSERT INTO doc_info 
               (doc_id, doc_title, doc_length) 
               VALUES (%s, %s, %s)""",
            (doc_id, doc_title, length)
        )

    def update_corpus_stats(self, docs_added, total_length, corpus="whole_corpus"):
        """Update corpus-level statistics (creates or updates record)."""
        existing = self.session.execute(
            """SELECT doc_n, total_doc_length 
               FROM corpus_info 
               WHERE corpus_name=%s""",
            (corpus,)
        ).one()

        if existing:
            new_count = existing.doc_n + docs_added
            new_length = existing.total_doc_length + total_length
            self._update_corpus_record(corpus, new_count, new_length)
        else:
            self._create_corpus_record(corpus, docs_added, total_length)

    def _update_corpus_record(self, corpus, count, length):
        """Update existing corpus statistics."""
        self.session.execute(
            """UPDATE corpus_info 
               SET doc_n=%s, total_doc_length=%s 
               WHERE corpus_name=%s""",
            (count, length, corpus)
        )

    def _create_corpus_record(self, corpus, count, length):
        """Create new corpus statistics record."""
        self.session.execute(
            """INSERT INTO corpus_info 
               (corpus_name, doc_n, total_doc_length) 
               VALUES (%s, %s, %s)""",
            (corpus, count, length)
        )

    def close(self):
        """Close database connection."""
        self.cluster.shutdown()


class DocumentProcessor:
    """Processes document data and maintains aggregation state."""

    def __init__(self):
        self.term_frequencies = {}
        self.doc_metadata = {}
        self.corpus_stats = {'doc_count': 0, 'total_length': 0}

    def process_line(self, line):
        """Process single input line and update aggregations."""
        try:
            doc_id, doc_title, term, count = line.strip().split('\t')
            doc_id = int(doc_id)
            count = int(count)

            # Update term frequency
            key = (doc_id, doc_title, term)
            self.term_frequencies[key] = self.term_frequencies.get(key, 0) + count

            # Update document length
            doc_key = (doc_id, doc_title)
            self.doc_metadata[doc_key] = self.doc_metadata.get(doc_key, 0) + count

        except ValueError:
            # Skip malformed lines
            pass

    def get_corpus_stats(self):
        """Calculate final corpus statistics."""
        self.corpus_stats['doc_count'] = len(self.doc_metadata)
        self.corpus_stats['total_length'] = sum(self.doc_metadata.values())
        return self.corpus_stats


def main():
    # Initialize data processor
    processor = DocumentProcessor()

    # Process all input lines
    for line in sys.stdin:
        processor.process_line(line)

    # Initialize database connection
    indexer = CassandraIndexer()
    try:
        # Index term frequencies
        for (doc_id, doc_title, term), count in processor.term_frequencies.items():
            indexer.index_term_frequency(doc_id, doc_title, term, count)

        # Index document info
        for (doc_id, doc_title), length in processor.doc_metadata.items():
            indexer.index_document_info(doc_id, doc_title, length)

        # Update corpus stats
        stats = processor.get_corpus_stats()
        indexer.update_corpus_stats(stats['doc_count'], stats['total_length'])

    finally:
        indexer.close()


if __name__ == "__main__":
    main()