import re
import sys


def tokenize_text(content):
    """Extract all alphanumeric tokens (including apostrophes) from text and return as unique set."""
    return set(re.findall(r'[a-z0-9\']+', content.lower()))


def process_stdin_lines():
    """Process each line from standard input and emit unique words with count 1."""
    for input_line in sys.stdin:
        stripped_line = input_line.strip()
        if not stripped_line:  # Skip empty lines
            continue

        parts = stripped_line.split('\t', maxsplit=2)  # Split into max 3 components
        if len(parts) < 3:  # Skip malformed lines
            continue

        _, _, content = parts  # We only need the text content
        unique_words = tokenize_text(content)

        for term in unique_words:
            print(f"{term}\t1")  # Emit each term with count 1


if __name__ == "__main__":
    process_stdin_lines()