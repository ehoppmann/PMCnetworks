import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime
from parserlib import EFetchExtractor #custom parser fns
import os
import fnmatch
import numpy as np
files_path = "input_path" ## PATH TO INPUT FILES
fileslist = []
for root, dirnames, filenames in os.walk(files_path):
    for filename in fnmatch.filter(filenames, '*.nxml'):
        fileslist.append(os.path.join(root, filename))
print len(fileslist)

# init SQL tables
import sqlite3
conn = sqlite3.connect('pmcv1.db')
c = conn.cursor()
c.execute('''CREATE TABLE refs (pmid integer, refpmid integer)''')
c.execute('''CREATE TABLE pmcidmap (pmid integer, pmcid integer)''')
c.execute('''CREATE TABLE meta (pmid integer, title text, journal_id text)''')
c.execute('''CREATE TABLE authors (pmid integer, authnum integer, fn text, ln text, afil text)''')
c.execute('''CREATE TABLE keywords (pmid integer, keyword text)''')
c.execute('''CREATE TABLE abstracts (pmid integer, abstract text)''')

#import records
totalrecordscount=0
added=0
n=len(fileslist) #269548
#n=500
offset = 0 #for testing purposes
for fnumin, fnin in enumerate(fileslist[(0+offset):(n+offset-1)]):
    with open(fnin, 'r') as filehandle:
        infile = filehandle.read()
    if infile.find("<ref id=") != -1:    
        soup = BeautifulSoup(infile, "lxml-xml")
        entry = EFetchExtractor(soup)
        if infile.find("The publisher of this article does not allow downloading of the full text in XML") != -1:
            pass
        elif len(entry.referencespmidsonly()) == 0:
            pass
        elif entry.pmid() == None:
            pass
        else:
            if (soup.find('pub-date', {"pub-type" : "epub"}) != None) & \
            (soup.find('article-meta').find('contrib-group') != None):
                #test that all functions run without errors:
                try:
                    refpmids = entry.referencespmidsonly()
                    entrypmid = entry.pmid()
                    entrytitle = entry.title()
                    entrypmcid = entry.pmcid()
                    entryauthorsAfil = entry.authorsAfil()
                    entryabstract = entry.abstract()
                    entrykeywords = entry.keywords()
                    entryjournal_id = entry.journal_id()
                    #building refs table
                    for ref in refpmids:
                        c.execute("INSERT INTO refs (pmid, refpmid) VALUES (?, ?);", 
                                  (entrypmid, int(ref)))
                    #building pmcidmap table
                    c.execute("INSERT INTO pmcidmap (pmid, pmcid) VALUES (?, ?);", 
                              (entrypmid, entrypmcid))
                    #building meta table
                    c.execute("INSERT INTO meta (pmid, title, journal_id) VALUES (?, ?, ?);", 
                              (entrypmid, entrytitle, entryjournal_id))
                    #building authors table
                    #print fnumin
                    for i, author in enumerate(entryauthorsAfil):
                        try:
                            c.execute("INSERT INTO authors (pmid, authnum, fn, ln, afil) VALUES (?, ?, ?, ?, ?);", 
                                  (entrypmid, i, author[0], author[1], author[2]))
                        except:
                            print "author entry failure num {}, {}, {}, {}, {}".format(fnumin, entrypmid, i, author[0], author[1])
                    #building keywords table
                    for keyword in entrykeywords:
                        try:
                            c.execute("INSERT INTO keywords (pmid, keyword) VALUES (?, ?);", 
                                      (entrypmid, keyword))
                        except: #most likely failure due to fomatting tags e.g. <bold> </bold>
                            print "keyword entry failure num {}, {}, {}".format(fnumin, entrypmid, keyword)
                    #building abstracts table
                    try:
                        c.execute("INSERT INTO abstracts (pmid, abstract) VALUES (?, ?);", 
                                  (entrypmid, entryabstract))
                    except:
                        print "abstract entry failure num {},{}, {}".format(fnumin, entrypmid, entryabstract)
                    added += 1
                except:
                    print "record {} skipped due to error".format(fnumin)
        if (totalrecordscount % 500 == 0): print "{0} added of {1} records".format(added, (totalrecordscount))
    totalrecordscount += 1

c.execute('''COMMIT''')
c.close()