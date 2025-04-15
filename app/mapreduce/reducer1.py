import sys
from cassandra.cluster import Cluster


class CassandraFrequencyManager:
    """Handles Cassandra operations for term frequency tracking."""

    def __init__(self, keyspace='index_keyspace', hosts=['cassandra-server']):
        """Initialize connection to Cassandra cluster."""
        self.cluster = Cluster(hosts)
        self.session = self.cluster.connect(keyspace)

    def update_term_frequency(self, term, frequency, corpus="whole_corpus"):
        """
        Update document frequency for a term in Cassandra.
        Increments existing frequency or creates new record if term doesn't exist.
        """
        existing = self.session.execute(
            "SELECT doc_frequency FROM doc_frequency_of_term "
            "WHERE corpus_name = %s AND term = %s",
            (corpus, term)
        ).one()

        if existing:
            new_freq = existing.doc_frequency + frequency
            self._update_existing_term(corpus, term, new_freq)
        else:
            self._insert_new_term(corpus, term, frequency)

    def _update_existing_term(self, corpus, term, frequency):
        """Update existing term frequency record."""
        self.session.execute(
            "UPDATE doc_frequency_of_term SET doc_frequency = %s "
            "WHERE corpus_name = %s AND term = %s",
            (frequency, corpus, term)
        )

    def _insert_new_term(self, corpus, term, frequency):
        """Insert new term frequency record."""
        self.session.execute(
            "INSERT INTO doc_frequency_of_term "
            "(term, corpus_name, doc_frequency) "
            "VALUES (%s, %s, %s)",
            (term, corpus, frequency)
        )

    def close(self):
        """Close Cassandra connection."""
        self.cluster.shutdown()


def aggregate_word_counts(input_stream):
    """Process input stream and return word frequency dictionary."""
    frequencies = {}
    for line in input_stream:
        try:
            word, count = line.strip().split('\t')
            frequencies[word] = frequencies.get(word, 0) + int(count)
        except ValueError:
            continue  # Skip malformed lines
    return frequencies


def main():
    # Aggregate word counts from standard input
    word_counts = aggregate_word_counts(sys.stdin)

    # Initialize Cassandra manager and update frequencies
    cassandra_mgr = CassandraFrequencyManager()
    try:
        for term, count in word_counts.items():
            cassandra_mgr.update_term_frequency(term, count)
    finally:
        cassandra_mgr.close()


if __name__ == "__main__":
    main()