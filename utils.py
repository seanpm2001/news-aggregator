from typing import List
import glob

def ensure_scheme(domain):
    """Helper utility for ensuring a domain has a scheme. If none is attached
       this will use the https scheme.

       Note: this will break if domain has a non http(s) scheme.
       example.com ==> https://example.com
       http://example.com ==> http://example.com
       file://example.com ==> https://file://example.com
    """
    if not domain.startswith('http'):
        domain = f'https://{domain}'
    return domain

def get_all_domains() -> List[str]:
    """Helper utility for getting all domains across all sources"""
    source_files = glob.glob('sources*.csv')
    result = set()
    for source_file in source_files:
        with open(source_file) as f:
            # Skip the first line, with the headers.
            lines = f.readlines()[1:]

            # The domain is the first field on the line
            yield from [line.split(',')[0].strip() for line in lines]
