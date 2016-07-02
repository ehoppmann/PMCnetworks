import sqlite3
import cPickle as pickle
import graph_tool as gt
import itertools

conn = sqlite3.connect('pmcv1-full.db')
c = conn.cursor()
g = gt.Graph(directed = True)
pmid_vertex_dict = dict()
for pair in c.execute('''SELECT pmid, refpmid FROM refs'''):
    if pair[0] not in pmid_vertex_dict:
        v = g.add_vertex()
        pmid_vertex_dict[pair[0]] = int(v)
    if pair[1] not in pmid_vertex_dict:
        v = g.add_vertex()
        pmid_vertex_dict[pair[1]] = int(v)
    g.add_edge(pmid_vertex_dict[pair[0]], pmid_vertex_dict[pair[1]])

pickle.dump(g, open("full_graph.p", "wb"))
pickle.dump(pmid_vertex_dict, open("full_graph_pmid_vertex_dict.p", "wb"))

g = gt.Graph(directed = False)
author_vertex_dict = dict()
author_full_name_dict = dict()
c.execute('''SELECT pmid, fn, ln FROM authors''')
authors = c.fetchall()
authorspaper = []
currpaper = authors[0][0]
for entry in authors:
    #accumulate by paper
    authorcat = unicode(entry[1]+entry[2]).replace(" ", "").lower()
    authorspaper.append(authorcat)
    author_full_name_dict[authorcat] = (entry[1],entry[2])
    if entry[0] != currpaper:
        #add author nodes and edges
        for comb in itertools.combinations(authorspaper, 2):
            addedge(g, comb[0], comb[1], author_vertex_dict)
        #reset and begin accumulating again
        currpaper = entry[0]
        authorspaper = []

pickle.dump(g, open("authors_full_graph.p", "wb"))
pickle.dump(author_vertex_dict, open("authors_vertex_dict.p", "wb"))
pickle.dump(author_full_name_dict, open("authors_full_name_dict.p", "wb"))
