import urllib2
from BeautifulSoup import *
from urlparse import urljoin
import sqlite3
import re

# some words to ignore
ignore_words = ['the','of','to','and','a','in','is','it','for','then']

class crawler:
    
    def __init__(self,dbname):
        self.con = sqlite3.connect(dbname)

    def __del__(self):
        self.con.close()
    
    def dbcommit(self):
        self.con.commit()

    # Return true if this url is already indexed
    def is_indexed(self,url):
        '''
        This function checks if a url has been entered into the url_list
        table and if yes, then further checks if an entry has been made
        in the word_location table for that url and if these two conditions
        are met - it returns True. Otherwise False.
        '''
        
        u = self.con.execute \
            ("select rowid from url_list where url = '%s'" % url)
        x = u.fetchone()

        if x != None:
            # checking if this url has been crawled at least once
            v = self.con.execute \
                ("select * from word_location where url_id = %d" % x[0])
            z = v.fetchone()
            if z is not None:
                return True
        return False
            

    def getentryid(self,table,field,value,createnew=True):
        curr = self.con.execute \
            ("select rowid from %s where %s = '%s'" % (table,field,value))
        res = curr.fetchone()

        # if this value is not in the database previously
        # first add it into the database then return lastrowid
        if res == None:
            curr = self.con.execute \
                ("insert into %s (%s) values ('%s')" % (table,field,value))
            return curr.lastrowid
        else:
            return res[0]
        
    def add_to_index(self,url,soup):
        '''
           Register the url in the url_list, then registers every word
           (using soup) with it's url, word_id and location in the page
        '''
        if self.is_indexed(url) == True:
            return
        
        print "Indexing %s" % url

        text = self.get_text_only(soup)
        words = self.separate_words(text)

        # retrieves or sets and retrieves a url's rowid
        url_id = self.get_entry_id('url_list', 'url', url)

        # iterating through all the words of this url
        for i in xrange(len(words)):
            word = words[i]
            if word in ignore_words:
                continue
            
            # retrieves or sets and retrieves a word's rowid
            word_id = self.get_entry_id('word_list','word',word)

            self.con.execute \
                    ("insert into word_location(url_id,word_id,location) values (%d, %d, %d)" %(url_id, word_id, i))
            

    # Extracting only text from an HTML page
    def get_text_only(self,soup):
        ''' Collecting the HTML page into a long string
            by traversing the DOM recursively    
        '''
        v = soup.string()

        if not v:
            c = soup.contents
            result_text = [self.get_text_only(t) for t in c]
            return '\n'.join(result_text)
        else:
            return v.strip()
        
        return None

    #Separating the words by any non whitespace character
    def separate_words(self,text):
        ''' Accept a string and returns a list
            of lowercase words
        '''
        # consider anything which is not a number or a letter to be a separator
        # and return all the words to the lowercase
        splitter = re.compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s != '']

    #Add a link between two pages
    def add_link_ref(self,urlFrom,urlTo,linkText):
        pass

    #Create the database tables
    def create_index_tables(self):
        self.con.execute('create table url_list(url)')
        self.con.execute('create table word_list(word)')
        self.con.execute('create table word_location(url_id,word_id,location)')
        self.con.execute('create table link(from_id integer, to_id integer)')
        self.con.execute('create table link_words(word_id, link_id)')
        self.con.execute('create index url_idx on url_list(url)')
        self.con.execute('create index word_idx on word_list(word)')
        self.con.execute('create index word_url_idx on word_location(word_id)')
        self.con.execute('create index url_to_idx on link(to_id)')
        self.con.execute('create index url_from_idx on link(from_id)')
        self.db_commit()
        
        
    # Starting with the list of pages
    # doing a bfs traversal upto a given depth
    # indexing pages with traversing
    def crawl(self, pages, depth = 2):
        for i in range(depth):
            newpages = set()
            for page in pages:
                try:
                    c = urllib2.urlopen(page)
                except:
                    print "Could not open %s" % page
                    continue
                soup = BeautifulSoup(c.read())

                # process the page and record the findings in db
                self.add_to_index(page,soup)
                # get all the links
                links = soup('a')

                for link in links:
                    if 'href' in dict(link.attrs):
                        url = urljoin(page, link['href'])
                        #print(link['href'])
                       
                        if url.find("'") != -1:
                            continue
                        url = url.split('#')[0]
                        
                        if (url[0:4]=='file' or url[0:4] == 'http' or url[0:4] == 'https') and self.is_indexed(url) == False:
                            newpages.add(url)
                            link_text = self.get_text_only(link)
                            self.add_link_ref(page, url, link_text)

            # save changes to database
            self.dbcommit()    
            pages = newpages
            
            print(pages)
            
class searcher:
    
    def __init__(self, dbname):
        self.con = sqlite3.connect(db_name)

    def __del__(self):
        self.con.close() 
