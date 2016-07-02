from graph_tool.all import *
import cPickle as pickle
import sqlite3

### citation graph network to SQL
g = graph_tool.load_graph('full_graph.gt')
pmid_vertex_dict = pickle.load(open("full_graph_pmid_vertex_dict.p", "rb"))
rev_pmid_vertex_dict = {v: k for k, v in pmid_vertex_dict.items()}

c = sqlite3.connect('pmcv1-graph.db') 
c.execute('''CREATE TABLE cites (pmid integer, incites text, outcites text)''')
i=0
for node in g.vertices():
    incites = ""
    for neigh in g.vertex(node).in_neighbours(): 
        incites += str(rev_pmid_vertex_dict[neigh]) + ","
    outcites = ""
    for neigh in g.vertex(node).out_neighbours(): 
        outcites += str(rev_pmid_vertex_dict[neigh]) + ","
    i += 1
    c.execute("INSERT INTO cites (pmid, incites, outcites) VALUES (?, ?, ?)", 
              (rev_pmid_vertex_dict[node], incites[:-1], outcites[:-1]))
    if i % 50000 == 0: print i #to monitor status
    c.execute('''COMMIT''')
c.close()


### co-authorship graph network to SQL
g = graph_tool.load_graph("authors_full_graph.gt")
author_vertex_dict = pickle.load(open("authors_vertex_dict.p", "rb"))
rev_auth_vertex_dict = {v: k for k, v in author_vertex_dict.items()}
c.execute('''CREATE TABLE coauthors (author text, coauthors text)''')
i=0
for node in g.vertices():
    coauthors = ""
    coauthcount = 0
    for neigh in g.vertex(node).all_neighbours(): 
        coauthors += unicode(rev_auth_vertex_dict[neigh]) + ","
        coauthcount += 1
        if coauthcount > 1000: break #stop if over 1000 co-authors
    i += 1
    #if not (coauthcount > 1000): #for those authors who's co-authorship lists have been truncated, do not add
    c.execute("INSERT INTO coauthors (author, coauthors) VALUES (?, ?)", 
              (rev_auth_vertex_dict[node], coauthors[:-1]))
    if i % 50000 == 0: print i #to monitor status

c.execute('''COMMIT''')
c.close()
