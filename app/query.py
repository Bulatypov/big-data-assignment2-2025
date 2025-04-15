import sys
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import lit, col, log
from pyspark.sql import DataFrame


class BM25Ranker:
    def __init__(self, query):
        self.query = query
        self.corpus_name = 'whole_corpus'
        self.k1 = 1.2  # BM25 parameter for term frequency saturation
        self.b = 0.75  # BM25 parameter for document length normalization

        # Initialize Spark session with Cassandra configuration
        self.spark = SparkSession.builder \
            .appName('BM25Ranking') \
            .config("spark.cassandra.connection.host", "cassandra-server") \
            .getOrCreate()

    def process_query(self):
        """Main pipeline for BM25 ranking process"""
        query_terms = self._extract_query_terms()
        vocab_df = self._load_vocabulary_data(query_terms)

        if not vocab_df.head(1):
            return self._write_empty_results()

        tf_df = self._load_term_frequencies(vocab_df)
        doc_ids = self._extract_unique_doc_ids(tf_df)
        docs_df = self._load_document_metadata(doc_ids)

        corpus_stats = self._load_corpus_stats()
        if not corpus_stats:
            return self._write_empty_results()

        ranked_docs = self._calculate_bm25_scores(tf_df, vocab_df, docs_df, corpus_stats)
        self._write_results(ranked_docs)

    def _extract_query_terms(self):
        """Extract and normalize query terms"""
        return list(set([word.lower() for word in self.query[1].split()]))

    def _load_vocabulary_data(self, query_terms):
        """Load document frequency data for query terms from Cassandra"""
        conditions = " OR ".join(
            f"(corpus_name = '{self.corpus_name}' AND term = '{term}')"
            for term in query_terms
        )
        return self.spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="doc_frequency_of_term", keyspace="index_keyspace") \
            .load() \
            .where(conditions)

    def _load_term_frequencies(self, vocab_df):
        """Load term frequency data for matching vocabulary terms"""
        vocab_rows = vocab_df.collect()
        dfs = []

        for row in vocab_rows:
            condition = f"corpus_name = '{row['corpus_name']}' AND term = '{row['term']}'"
            tf_df = self.spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="term_frequency_in_doc", keyspace="index_keyspace") \
                .load() \
                .where(condition)
            dfs.append(tf_df)

        return reduce(DataFrame.unionByName, dfs) if len(dfs) > 1 else dfs[0]

    def _extract_unique_doc_ids(self, tf_df):
        """Extract unique document IDs from term frequency data"""
        return [row["doc_id"] for row in tf_df.select("doc_id").distinct().collect()]

    def _load_document_metadata(self, doc_ids):
        """Load document metadata for relevant documents"""
        dfs = []
        for doc_id in doc_ids:
            doc_df = self.spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="doc_info", keyspace="index_keyspace") \
                .load() \
                .where(f"doc_id = {doc_id}")
            dfs.append(doc_df)

        return reduce(DataFrame.unionByName, dfs) if len(dfs) > 1 else dfs[0]

    def _load_corpus_stats(self):
        """Load corpus statistics from Cassandra"""
        corpus_df = self.spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="corpus_info", keyspace="index_keyspace") \
            .load() \
            .where(f"corpus_name = '{self.corpus_name}'")

        row = corpus_df.first()
        return {
            'total_docs': row["doc_n"],
            'avg_doc_len': float(row["total_doc_length"] / row["doc_n"])
        } if row else None

    def _calculate_bm25_scores(self, tf_df, vocab_df, docs_df, corpus_stats):
        """Calculate BM25 scores for documents"""
        # Join term frequencies with document frequencies
        joined_df = tf_df.join(vocab_df, on=["term", "corpus_name"])

        # Join with document metadata
        joined_df = joined_df.join(docs_df, on=["doc_id", "doc_title"])

        # Calculate BM25 components
        idf = log(lit(corpus_stats['total_docs']) / col("doc_frequency"))
        tf_component = (self.k1 + 1) * col("term_frequency")
        length_norm = self.k1 * (1 - self.b + self.b * col("doc_length") / corpus_stats['avg_doc_len'])
        tf_normalized = tf_component / (length_norm + col("term_frequency"))

        # Calculate final BM25 score and aggregate per document
        return joined_df.withColumn("bm25", idf * tf_normalized) \
            .groupBy("doc_id", "doc_title") \
            .sum("bm25") \
            .withColumnRenamed("sum(bm25)", "doc_rank") \
            .orderBy(col("doc_rank").desc()) \
            .limit(10)

    def _write_results(self, results_df):
        """Write ranked results to output"""
        results_df.write.csv("/bm25_output", sep="\t", mode="overwrite")
        self.spark.stop()

    def _write_empty_results(self):
        """Handle case when no results are found"""
        schema = StructType([
            StructField("doc_id", IntegerType(), True),
            StructField("doc_title", StringType(), True),
            StructField("doc_rank", FloatType(), True),
        ])
        self.spark.createDataFrame([], schema) \
            .write.csv("/bm25_output", sep="\t", mode="overwrite")
        self.spark.stop()
        sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit query.py <query>")
        sys.exit(1)

    ranker = BM25Ranker(sys.argv)
    ranker.process_query()