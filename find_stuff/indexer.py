'''
Created on Mar 21, 2016

'''

from argparse import ArgumentParser
from cStringIO import StringIO
import codecs
from contextlib import contextmanager
from find_stuff.common import load_config, CJKFilter
import gzip
from logging import getLogger, basicConfig
import multiprocessing
import os
from os.path import exists, join, splitext
import shutil
import tarfile
import tempfile
from zipfile import ZipFile

from whoosh.analysis.analyzers import StemmingAnalyzer
from whoosh.fields import Schema, TEXT, ID, STORED
from whoosh.index import create_in, open_dir
from whoosh.util.text import rcompile


logger = getLogger("indexer")
basicConfig(level="INFO")

try:
    from rarfile import RarFile
    import rarfile
    
    if os.name == 'nt':
        rarfile.UNRAR_TOOL = 'C:\\Program Files\\WinRAR\\unrar.exe'
        
    rar_support = False
except:
    logger.warn("failed to import rarfile")
    rar_support = True

try:
    from bs4 import BeautifulSoup
except:
    logger.warn("failed to import BeautifulSoup")

try:
    import docx
except:
    logger.warn("failed to import docx")

try:
    import epub
except:
    logger.warn("failed to import epub")
    
try:
    from pdfminer.converter import TextConverter
    from pdfminer.layout import LAParams
    from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
    from pdfminer.pdfpage import PDFPage
except:
    logger.warn("failed to import pdfminer")

try:
    from find_stuff.chmfile import SimpleChmFile
except:
    logger.warn("failed to import chm packages")    

try:
    from pyth.plugins.rtf15.reader import Rtf15Reader
    from pyth.plugins.plaintext.writer import PlaintextWriter
except:
    logger.warn("failed to import rtf packages")    


stem_ana = StemmingAnalyzer() | CJKFilter()
stem_ana.cachesize = -1

pattern2 = rcompile(r"[A-Za-z0-9]+(\.?[A-Za-z0-9]+)*")
stem_ana2 = StemmingAnalyzer(expression=pattern2) | CJKFilter()
stem_ana2.cachesize = -1


schema = Schema(title=TEXT(analyzer=stem_ana2,stored=True), content=TEXT(analyzer=stem_ana), time=STORED, path=ID(stored=True), real_path=STORED, filetype=ID)

handlers = {}

class TxtHandler(object):
    
    def extract_content(self, filepath):
        with codecs.open(filepath, encoding='utf-8') as fh:
            return fh.read()


def to_utf8(v):
    if type(v) == str:
        return unicode(v, encoding='utf-8',errors='replace')
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
    logger.warn("failed to initialize djvu handler")


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
                        pass
                        #logger.error("error occurred.")
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

class ZipHandler(object):
    
    @contextmanager            
    def extract(self, filepath):
        try:
            tmp_dir = tempfile.mkdtemp()
            with ZipFile(filepath,'r') as z:
                tmppath = join(tmp_dir,os.path.basename(filepath))
                os.makedirs(tmppath)
                z.extractall(tmppath)
                
            yield tmppath
        finally:
            shutil.rmtree(tmp_dir)
            
    def path_exists(self, archive, path):
        with ZipFile(archive,'r') as z:
            return path in z.namelist()

class RarHandler(object):
    
    @contextmanager            
    def extract(self, filepath):
        try:
            tmp_dir = tempfile.mkdtemp()
            with RarFile(filepath,'r') as z:
                tmppath = join(tmp_dir,os.path.basename(filepath))
                os.makedirs(tmppath)
                z.extractall(tmppath)
                
            yield tmppath
        finally:
            shutil.rmtree(tmp_dir)
            
    def path_exists(self, archive, path):
        with RarFile(archive,'r') as z:
            return path in z.namelist()
        
class TarHandler(object):
    
    @contextmanager            
    def extract(self, filepath):
        try:
            tmp_dir = tempfile.mkdtemp()
            with tarfile.open(filepath,'r') as z:
                tmppath = join(tmp_dir,os.path.basename(filepath))
                os.makedirs(tmppath)
                z.extractall(tmppath)
                
            yield tmppath
        finally:
            shutil.rmtree(tmp_dir)
            
    def path_exists(self, archive, path):
        with tarfile.open(archive,'r') as z:
            return path in z.getnames()
        
class GzipHandler(object):
    
    @contextmanager            
    def extract(self, filepath):
        try:
            tmp_dir = tempfile.mkdtemp()
            with gzip.open(filepath,'rb') as z:
                tmppath = join(tmp_dir,os.path.basename(filepath))
                os.makedirs(tmppath)
                #z.extractall(tmppath)
                bn, _ = os.path.splitext(os.path.basename(filepath))
                with open(join(tmppath,bn),'wb') as f:
                    f.write(z.read())
                
            yield tmppath
        finally:
            shutil.rmtree(tmp_dir)
            
    def path_exists(self, archive, path):
        return path + ".gz" == archive
        

archive_handlers = {
                    ".zip": ZipHandler(),
                    ".tar": TarHandler(),
                    ".tar.gz": TarHandler(),
                    ".gz": GzipHandler(),
                    }

if rar_support:
    archive_handlers[".rar"] = RarHandler()

def os_path(p):
    if os.name == 'nt':
        return p.replace('/',os.path.sep)
    else:
        return p

def std_path(p):
    if os.name == 'nt':
        return p.replace(os.path.sep, '/')
    return p    

def path_join(path, *args):
    return os.path.join(path, *args)
    
    
def path_exists(path):
    return os.path.exists(path)
    
def getmtime(path):
    if path_exists(path):
        mtime = os.path.getmtime(path)
        return mtime
    raise IOError, "path %s does not exists" % path
        

def get_handler(ext):
    return handlers.get(ext)

def splitext(f):
    filename, ext = os.path.splitext(f)
    if ext == '.gz':
        _, ext2 = splitext(filename)
        if ext2 == '.tar':
            ext = '.tar.gz'
    
    return filename,ext

def get_paths(work_path, indexed_paths, to_index, target_path):
    for root, _, files in os.walk(work_path):
        for f in files:
            _, ext = splitext(f)
            if ext in archive_handlers:
                archive_path = path_join(root, f)
                relpath = os.path.relpath(archive_path, target_path)
                if relpath not in indexed_paths or relpath in to_index:
                     
                    with archive_handlers[ext].extract(archive_path) as realroot:
                        for root2, _, files2 in os.walk(realroot):
                            for f in files2:
                                
                                real_path = path_join(root2, f)
                                index_path = path_join(relpath, os.path.relpath(real_path, realroot))
                                
                                yield real_path, index_path, relpath
            else:
                real_path = path_join(root, f)
                index_path = os.path.relpath(real_path, target_path)
                yield real_path, index_path, index_path

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
            real_path = fields['real_path']

            indexed_paths.add(os_path(real_path))
            
            filepath = path_join(target_path, real_path)
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
                    to_index.add(os_path(real_path))

        count = 0
        try:
            work_path = work_path or target_path
            
            for real_path, index_path, archive_path in get_paths(work_path, indexed_paths, to_index, target_path):
                _,ext = splitext(real_path)
                if ext in indexables:
                    filename = os.path.basename(real_path)
                    if index_path not in indexed_paths or index_path in to_index:
                        logger.info("indexing... %s", index_path)
                        try:
                            hdr = get_handler(ext)
                            if hdr is None:
                                content = "" 
                            else:
                                content = hdr.extract_content(real_path.encode('utf-8'))
                            writer.add_document(title=filename, content=content,
                                        path=std_path(index_path), filetype=ext, time=getmtime(path_join(target_path,archive_path)), real_path=std_path(archive_path))
                            count+=1
                        except KeyboardInterrupt:
                            raise
                        except:
                            logger.exception("error occurred")
                
                if count % 20 == 0 and count > 0:
                    writer.commit()
                    logger.info("indexed %d files",count)

                    writer = ix.writer(limitmb=512, procs=multiprocessing.cpu_count())
                    count = 0

        except KeyboardInterrupt:
            pass
        except:
            logger.exception("error occurred")

        if count > 0:
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