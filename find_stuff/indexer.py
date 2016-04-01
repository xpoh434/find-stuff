'''
Created on Mar 21, 2016

'''

from argparse import ArgumentParser
from cStringIO import StringIO
import codecs
from contextlib import contextmanager
from find_stuff.common import load_config, CJKFilter
from logging import getLogger, basicConfig
import multiprocessing
import os
from os.path import exists, join, splitext
import shutil
import tempfile
from zipfile import ZipFile

import patoolib
from whoosh.analysis.analyzers import StemmingAnalyzer
from whoosh.fields import Schema, TEXT, ID, STORED
from whoosh.index import create_in, open_dir
from whoosh.util.text import rcompile


try:
    from bs4 import BeautifulSoup
except:
    pass

try:
    import docx
except:
    pass

try:
    import epub
except:
    pass
    
try:
    from pdfminer.converter import TextConverter
    from pdfminer.layout import LAParams
    from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
    from pdfminer.pdfpage import PDFPage
except:
    pass

try:
    from find_stuff.chmfile import SimpleChmFile
except:
    pass

try:
    from pyth.plugins.rtf15.reader import Rtf15Reader
    from pyth.plugins.plaintext.writer import PlaintextWriter
except:
    pass    


logger = getLogger("indexer")

stem_ana = StemmingAnalyzer() | CJKFilter()
stem_ana.cachesize = -1

pattern2 = rcompile(r"[A-Za-z0-9]+(\.?[A-Za-z0-9]+)*")
stem_ana2 = StemmingAnalyzer(expression=pattern2) | CJKFilter()
stem_ana2.cachesize = -1


schema = Schema(title=TEXT(analyzer=stem_ana2,stored=True), content=TEXT(analyzer=stem_ana), time=STORED, path=ID(stored=True), filetype=ID)

handlers = {}

class TxtHandler(object):
    
    def extract_content(self, filepath):
        with codecs.open(filepath, encoding='utf-8') as fh:
            return fh.read()


def to_utf8(v):
    if type(v) == str:
        return unicode(v, encoding='utf-8')
    return v

#http://stackoverflow.com/questions/22799990/beatifulsoup4-get-text-still-has-javascript
def extract_html(html):
    soup = BeautifulSoup(html,"lxml")
    
    # kill all script and styl elements
    for script in soup(["script", "style"]):
        script.extract()    # rip it out
    
    # get text
    text = soup.get_text()
    
    return text

class RtfHandler(object):
    
    def extract_content(self, filepath):
        with open(filepath, "rb") as fh:
            doc = Rtf15Reader.read(fh)
        
        return to_utf8(PlaintextWriter.write(doc).getvalue())

class DocxHandler(object):
    
    def extract_content(self, filepath):
        document = docx.Document(filepath)
        docText = '\n\n'.join([
            paragraph.text.encode('utf-8') for paragraph in document.paragraphs
        ])
        return to_utf8(docText)

class HtmlHandler(object):
    
    def extract_content(self, filepath):
        with codecs.open(filepath, encoding='utf-8') as fh:
            return extract_html(fh.read())

class EpubHandler(object):
    
    def extract_content(self, filepath):
        book = epub.open_epub(filepath)
        sio = StringIO()
        for item in book.opf.manifest.values():
            # read the content
            if item.media_type in ('application/xhtml+xml'):
                data = book.read_item(item)
                #sio.write(epub.utils.get_node_text(data))
                sio.write(extract_html(data).encode('utf-8'))
                sio.write("\n")
        return to_utf8(sio.getvalue())
    
class ChmHandler(object):
    
    def extract_content(self, filepath):
        chm = SimpleChmFile(filepath)
        sio = StringIO()
        for page in chm:
            if page is None:
                continue
            sio.write(extract_html(page).encode('utf-8'))
            sio.write("\n")
        return to_utf8(sio.getvalue())
        
try:
    import djvu.decode
    
    #http://apt-browse.org/browse/debian/wheezy/main/i386/python-djvu/0.3.9-1/file/usr/share/doc/python-djvu/examples/djvu-dump-text
    class DjvuHandler(object):
        
        def get_text(self, sexpr):
            sio = StringIO()
            if isinstance(sexpr, djvu.sexpr.ListExpression):
                #print str(sexpr[0].value), [sexpr[i].value for i in xrange(1, 5)]
                for child in sexpr[5:]:
                    sio.write(self.get_text(child))
                    sio.write(" ")
            else:
                sio.write(sexpr.value.strip())
                sio.write(" ")
            return sio.getvalue()
    
        class Context(djvu.decode.Context):
            def handle_message(self, message):
                if isinstance(message, djvu.decode.ErrorMessage):
                    logger.error(message)
        
        def extract_content(self, filepath):
            ctx = self.Context()
            document = ctx.new_document(djvu.decode.FileURI(filepath))
            document.decoding_job.wait()
            sio = StringIO()
            for page in document.pages:
    #             page.get_info()
                sio.write(self.get_text(page.text.sexpr))
                sio.write("\n")
            
            txt = sio.getvalue()
            return to_utf8(txt)

    handlers[".djvu"] = DjvuHandler()

except:
    pass


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
                        logger.error("error occurred.")
            finally:
                device.close()
                txt= unicode(outfp.getvalue(),encoding='utf-8')
                outfp.close()
        return to_utf8(txt)
    
    

htmlhdr = HtmlHandler()
txthdr = TxtHandler()
handlers[".txt"] = txthdr
handlers[".pdf"] = PdfHandler()
handlers[".epub"] = EpubHandler()
handlers[".html"] = htmlhdr
handlers[".htm"] = htmlhdr
handlers[".chm"] = ChmHandler()
handlers[".docx"] = DocxHandler()
handlers[".rtf"] = RtfHandler()

class ArchiveHandler(object):
    
    @contextmanager            
    def extract(self, filepath):
        try:
            tmp_dir = tempfile.mkdtemp()
            #with ZipFile(filepath,'r') as z:
            tmppath = join(tmp_dir,os.path.basename(filepath))
            os.makedirs(tmppath)
            patoolib.extract_archive(filepath,outdir=tmppath)
                
            yield tmppath
        finally:
            shutil.rmtree(tmp_dir)
            
    def path_exists(self, archive, path):
        with ZipFile(archive,'r') as z:
            return path in z.namelist()
        

generic_archive_handler = ArchiveHandler()

archive_handlers = {
                    ".zip": generic_archive_handler,
                    ".tar": generic_archive_handler,
                    ".rar": generic_archive_handler,
                    ".gz": generic_archive_handler
                    }

def walk_path(path):
    for root,dirs_,files in os.walk(path):
        archives = []
        non_archives = []
        for f in files:
            _,ext = splitext(f)
            if ext in archive_handlers:
                archives.append(f)
            else:
                non_archives.append(f)
        yield root, dirs_ + archives, non_archives, root
        
        for a in archives:
            filepath=join(root, a)
            _,ext = splitext(a)
            
            with archive_handlers[ext].extract(filepath) as realpath:
                for root2, dirs2, files2 in os.walk(realpath):
                    relpath=os.path.relpath(root2, realpath)
                    yield join(filepath, relpath), dirs2, files2, root2

def _get_archive_paths(path):
    elems = path.split(os.path.sep)
    for i,t in enumerate(elems):
        _,ext = splitext(t)
        if ext in archive_handlers:
            break
    else:
        return None, None, None
    archive_file = os.path.sep.join(elems[:i+1])
    path_in_archive = "/".join(elems[i+1:])
    
    return ext, archive_file, path_in_archive

def path_exists(path):
    if not os.path.exists(path):
        ext, archive_file, path_in_archive = _get_archive_paths(path)
        if ext is None:
            return False
        else:
            return os.path.exists(archive_file) and archive_handlers[ext].path_exists(archive_file, path_in_archive)
    else:
        return True
    
def getmtime(path):
    if os.path.exists(path):
        mtime = os.path.getmtime(path)
    else:
        ext, archive_file, _= _get_archive_paths(path)
        if ext is None:
            raise IOError, "path %s does not exists" % path
        else:
            mtime = os.path.getmtime(archive_file)
        
    return mtime
        

def get_handler(ext):
    return handlers.get(ext)

# https://whoosh.readthedocs.org/en/latest/indexing.html#incremental-indexing
def incremental_index(ix, target_path, indexables, work_path):
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
            #if not os.path.exists(filepath):
            if not path_exists(filepath):
                # This file was deleted since it was indexed
                logger.info("remove: %s", indexed_path)
                writer.delete_by_term('path', indexed_path)
            
            else:
                # Check if this file was changed since it
                # was indexed
                indexed_time = fields['time']
                mtime = getmtime(filepath)
                if mtime > indexed_time:
                    # The file has changed, delete it and add it to the list of
                    # files to reindex
                    writer.delete_by_term('path', indexed_path)
                    to_index.add(indexed_path)

        count = 0
        try:
            work_path = work_path or target_path
            for root, _, files, realroot  in walk_path(work_path):
                for f in files:
                    filename, ext = os.path.splitext(f)
                    if ext in indexables:
                        virtualpath = os.path.join(root, f)
                        relpath = os.path.relpath(virtualpath, target_path)
                        if relpath not in indexed_paths or relpath in to_index:
                            logger.info("indexing... %s", relpath)
                            hdr = get_handler(ext)
                            if hdr is None:
                                content = "" 
                            else:
                                try:
                                    filepath = os.path.join(realroot, f)
                                    content = hdr.extract_content(filepath.encode('utf-8'))
                                    writer.add_document(title=filename, content=content,
                                                path=relpath, filetype=ext, time=getmtime(virtualpath))
                                    count+=1
                                except KeyboardInterrupt:
                                    raise
                                except:
                                    logger.exception("error occurred")

        except KeyboardInterrupt:
            pass
        except:
            logger.exception("error occurred")

        writer.commit()
        logger.info("indexed %d files",count)

def main(argv):
    
    argparser = ArgumentParser()
    argparser.add_argument("--work",type=unicode,help="the path to work on",default=None)
    opts = argparser.parse_args(argv)
    
    work_path = opts.work
    
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
        
    incremental_index(ix, target_path, indexables, work_path)
    
#     save_config(config)

if __name__ == '__main__':
    import sys
    main(sys.argv[1:])