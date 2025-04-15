import re
import sys


def extract_tokens(text_content):
    """Extract all alphanumeric tokens (including apostrophes) from text in lowercase."""
    return re.findall(r'[a-z0-9\']+', text_content.lower())


def process_input_data():
    """Process each line from stdin, extract words, and emit doc metadata with word counts."""
    for input_line in sys.stdin:
        stripped_line = input_line.strip()
        if not stripped_line:  # Skip empty lines
            continue

        # Split into ID, title, and content (max 3 parts)
        line_components = stripped_line.split('\t', 2)
        if len(line_components) < 3:  # Skip incomplete entries
            continue

        document_id, document_title, content = line_components
        tokens = extract_tokens(content)

        # Emit each word with document metadata
        for token in tokens:
            print(f"{document_id}\t{document_title}\t{token}\t1")


if __name__ == "__main__":
    process_input_data()