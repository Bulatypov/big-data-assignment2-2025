CREATE KEYSPACE IF NOT EXISTS index_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE index_keyspace;

CREATE TABLE IF NOT EXISTS doc_frequency_of_term (
    term TEXT,
    corpus_name TEXT,
    doc_frequency INT,
    PRIMARY KEY (corpus_name, term)
);

CREATE TABLE IF NOT EXISTS term_frequency_in_doc (
    term TEXT,
    corpus_name TEXT,
    doc_id INT,
    doc_title TEXT,
    term_frequency INT,
    PRIMARY KEY ((term, corpus_name), doc_id, doc_title)
);

CREATE TABLE IF NOT EXISTS doc_info (
    doc_id INT,
    doc_title TEXT,
    doc_length INT,
    PRIMARY KEY (doc_id, doc_title)
);

CREATE TABLE IF NOT EXISTS corpus_info (
    corpus_name TEXT PRIMARY KEY,
    doc_n INT,
    total_doc_length INT
);