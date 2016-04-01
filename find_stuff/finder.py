'''
Created on Mar 21, 2016

'''
from logging import basicConfig, getLogger
from whoosh import query
from whoosh.index import open_dir
from whoosh.qparser.default import QueryParser

from find_stuff.common import load_config, CJKFilter  # @UnusedImport


logger = getLogger("finder")

def main(argv):
    
    basicConfig(level="INFO")
    getLogger().setLevel("WARN")
    logger.setLevel("INFO")
    
    config = load_config()
    index_path = config['index_path']
    
    ix = open_dir(index_path)
    searcher = ix.searcher()
    
    print "Doc count=%d"%searcher.doc_count()
    while True:
        try:
            querystring = raw_input("find something? >")
        except KeyboardInterrupt:
            print 
            break
        with ix.searcher() as searcher:
            querystring = querystring.strip()
            if querystring == "":
                q = query.Every()
            else:
                parser = QueryParser("content", ix.schema)
                q = parser.parse(querystring)
            results = searcher.search_page(q, 1, pagelen=20)
            if len(results) == 0:
                print "No result"
            else:
                print "Found %d results"%len(results)
                quit_ =False
                for p in range(1, results.pagecount+1):
                    while not quit_:
                        for i, hit in enumerate(results):
                            print "%d >> %s" %(i + (p-1)*20 + 1,hit)
                        inp = raw_input("Page %d/%d, (Enter: next page|q: quit) ? >" % (p, results.pagecount))
                        if inp.strip() == 'q':
                            quit_ = True
                        else:
                            if p < results.pagecount:
                                results = searcher.search_page(q, p+1, pagelen=20)
                            break
                    if quit_:
                        break
            
if __name__ == '__main__':
    import sys
    main(sys.argv)
