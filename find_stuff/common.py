'''
Created on Mar 21, 2016

'''
import json
from whoosh.analysis.filters import Filter
from whoosh.analysis.ngrams import NgramTokenizer


config_file = "indexer.json"

def load_config():
    with open(config_file,"r") as fh:
        config = json.load(fh)
    return config

# https://gist.github.com/lukhnos/8800394
class CJKFilter(Filter):
    def __call__(self, tokens):
        ngt = NgramTokenizer(minsize=1, maxsize=2)

        for t in tokens:
            if len(t.text) > 0 and ord(t.text[0]) >= 0x2e80:
                for t in ngt(t.text):
                    t.pos = True
                    yield t
            else:
                yield t


# def save_config(config):
#     with open(config_file,"w") as fh:
#         json.dump(config, fh)
