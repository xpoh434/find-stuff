'''
Created on Mar 21, 2016

'''

from cStringIO import StringIO
from logging import getLogger, basicConfig
import multiprocessing
import os
from os.path import exists
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from whoosh.analysis.analyzers import StemmingAnalyzer
from whoosh.fields import Schema, TEXT, ID, STORED
from whoosh.index import create_in, open_dir
from whoosh.util.text import rcompile

from find_stuff.common import load_config, CJKFilter


logger = getLogger("indexer")

stem_ana = StemmingAnalyzer() | CJKFilter()
stem_ana.cachesize = -1

pattern2 = rcompile(r"[A-Za-z0-9]+(\.?[A-Za-z0-9]+)*")
stem_ana2 = StemmingAnalyzer(expression=pattern2) | CJKFilter()
stem_ana2.cachesize = -1


schema = Schema(title=TEXT(analyzer=stem_ana2,stored=True), content=TEXT(analyzer=stem_ana), time=STORED, path=STORED, filetype=ID)

class TxtHandler(object):
    
    def extract_content(self, filepath):
        with open(filepath,"r") as fh:
            return fh.read()

class PdfHandler(object):
    
    def __init__(self):
        self.rsrcmgr = PDFResourceManager()
        self.laparams = LAParams(all_texts=True)

    def extract_content(self, filepath):
        outfp = StringIO()
        device = TextConverter(self.rsrcmgr, outfp, codec="utf-8", laparams=self.laparams,
                               imagewriter=None)

        with open(filepath, 'rb') as fp:
            interpreter = PDFPageInterpreter(self.rsrcmgr, device)
            try:
                for page in PDFPage.get_pages(fp, set(),
                                              maxpages=0, password='',
                                              caching=True, check_extractable=False):
                    try:
                        interpreter.process_page(page)
                    except KeyboardInterrupt:
                        raise
                    except:
                        logger.exception("error occurred")
            except KeyboardInterrupt:
                raise
            except:
                logger.exception("error occured.")
            finally:
                device.close()
                txt= unicode(outfp.getvalue(),encoding='utf-8')
                outfp.close()
        return txt

handlers = {
            ".txt": TxtHandler(),
            ".pdf": PdfHandler()
            }

def get_handler(ext):
    return handlers[ext]

# https://whoosh.readthedocs.org/en/latest/indexing.html#incremental-indexing
def incremental_index(ix, target_path, indexables):
    # The set of all paths in the index
    indexed_paths = set()
    # The set of all paths we need to re-index
    to_index = set()

    with ix.searcher() as searcher:
        writer = ix.writer(limitmb=512, procs=multiprocessing.cpu_count())

        # Loop over the stored fields in the index
        for fields in searcher.all_stored_fields():
            indexed_path = fields['path']
            indexed_paths.add(indexed_path)
            
            filepath = os.path.join(target_path, indexed_path)
            if not os.path.exists(filepath):
                # This file was deleted since it was indexed
                writer.delete_by_term('path', indexed_path)

            else:
                # Check if this file was changed since it
                # was indexed
                indexed_time = fields['time']
                mtime = os.path.getmtime(filepath)
                if mtime > indexed_time:
                    # The file has changed, delete it and add it to the list of
                    # files to reindex
                    writer.delete_by_term('path', indexed_path)
                    to_index.add(indexed_path)

        count = 0
        try:
            for root, _, files  in os.walk(target_path):
                for f in files:
                    filepath = os.path.join(root, f)
                    filename, ext = os.path.splitext(f)
                    if ext in indexables:
                        relpath = os.path.relpath(filepath, target_path)
                        if relpath not in indexed_paths or relpath in to_index:
                            logger.info("indexing... %s", relpath)
                            content = get_handler(ext).extract_content(filepath)
                            writer.add_document(title=filename, content=content,
                                        path=relpath, filetype=ext, time=os.path.getmtime(filepath))
                            count+=1
        except KeyboardInterrupt:
            pass
        except:
            logger.exception("error occurred")

        writer.commit()
        logger.info("indexed %d files",count)

def main(argv):
    
    basicConfig(level="INFO")
    getLogger().setLevel("WARN")
    logger.setLevel("INFO")
    
    config = load_config()
    index_path = config['index_path']
    target_path = config['target_path']
    indexables = config['indexables']
    
    if not exists(index_path):
        os.makedirs(index_path)
        ix = create_in(index_path, schema)
    else:
        ix = open_dir(index_path)
        
    incremental_index(ix, target_path, indexables)
    
#     save_config(config)

if __name__ == '__main__':
    import sys
    main(sys.argv)