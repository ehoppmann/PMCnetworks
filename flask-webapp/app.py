import flask
from flask import g, send_from_directory
import os
import sqlite3
from io import BytesIO
import base64
import Queue
import heapq
import random
import networkx as nx
from networkx.readwrite import json_graph
import json
import struct
import colorbrewer

DBfull = 'db/pmcv1-full.db'
DBgraph = 'db/pmcv1-graph.db'

def get_db_full():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._dbfull = sqlite3.connect(DBfull)
    return db

def get_db_graph():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._dbgraph = sqlite3.connect(DBgraph)
    return db

def query_db_full(query, args=(), one=False):
    cur = get_db_full().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv

def query_db_graph(query, args=(), one=False):
    cur = get_db_graph().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv

app = flask.Flask(__name__, static_url_path='')

def getitem(obj, item, default):
    if item not in obj:
        return default
    else:
        return obj[item]

def buildfnauthortree(rootnode, mastergraphcursor, fulldbcursor, depth = 2, maxelements = 600):
    _g =nx.DiGraph()
    h = []
    heapq.heappush(h, (0, rootnode))
    nodes = 0
    while h != []:
        node = heapq.heappop(h)
        if node[0] < depth:
            coauthors = mastergraphcursor('SELECT coauthors FROM coauthors WHERE author = ?', 
                                          [node[1]], one = True)[0].split(',')
            for author in coauthors:
                if lookupfn(author, fulldbcursor) not in _g.nodes():
                    _g.add_edge(lookupfn(node[1], fulldbcursor), lookupfn(author, fulldbcursor))
                    heapq.heappush(h, (node[0]+1, author))
                    nodes += 1
        if nodes > maxelements:
            return buildfnauthortree(rootnode, mastergraphcursor, fulldbcursor, depth-1, maxelements)
    return _g

def lookupfn(shortname, fulldbcursor):
    try: 
        fn = fulldbcursor('SELECT authorfn FROM authorfndict WHERE authorabbr = ?', [shortname], one=True)[0]
    except TypeError:
        fn = shortname
    return fn

def buildcitenetwork(rootnode, mastergraphcursor, authcursor, indepth = 0, outdepth = 2, 
                     colorscheme = colorbrewer.RdBu):
    _g =nx.DiGraph()
    q = Queue.Queue()
    #set up colors
    _colors = (colorscheme[max(outdepth,indepth)*2+1])
    #_colors.reverse() keeps reversing every page render...why?
    #first go in out direction
    q.put((rootnode, 0))
    try:
        lastname = authcursor('SELECT ln FROM authors WHERE pmid = ? AND authnum = 0', [rootnode], one=True)[0]
    except TypeError:
        lastname = rootnode
    _g.add_node(rootnode, color = rgbtohex(_colors[(len(_colors)-1)/2]), ln = lastname, 
                meta = citetooltip(rootnode, authcursor))
    while not q.empty():
        node = q.get()
        if node[1] < outdepth:
            citestr = mastergraphcursor('SELECT outcites FROM cites WHERE pmid = ?', [node[0]], one=True)[0]
            try:
                cites = map(int, citestr.split(','))
                for cite in cites:
                    if cite not in _g.nodes():
                        try:
                            lastname = authcursor('SELECT ln FROM authors WHERE pmid = ? AND authnum = 0', [cite], one=True)[0]
                        except TypeError:
                            lastname = cite
                        _g.add_node(cite, color = rgbtohex(_colors[(len(_colors)-1)/2+node[1]+1]), ln = lastname,
                                   meta = citetooltip(cite, authcursor))
                        _g.add_edge(node[0], cite)
                        q.put((cite, node[1]+1))
            except ValueError: #when there are none
                pass
    #now go in in direction
    q.put((rootnode, 0))
    while not q.empty():
        node = q.get()
        if node[1] < indepth:
            citestr = mastergraphcursor('SELECT incites FROM cites WHERE pmid = ?', [node[0]], one=True)[0]
            try:
                cites = map(int, citestr.split(','))
                for cite in cites:
                    if cite not in _g.nodes():
                        try:
                            lastname = authcursor('SELECT ln FROM authors WHERE pmid = ? AND authnum = 0', [cite], one=True)[0]
                        except TypeError:
                            lastname = cite
                        _g.add_node(cite, color = rgbtohex(_colors[(len(_colors)-1)/2-node[1]-1]), ln = lastname,
                                   meta = citetooltip(cite, authcursor))
                        _g.add_edge(cite, node[0])
                        q.put((cite, node[1]+1))
            except ValueError:
                pass
    return _g

def citetooltip(cite, authcursor):
    returntext = ""
    try:
        returntext += "Title: " + authcursor('SELECT title from meta WHERE pmid = ?', [cite], one=True)[0] + "<br>"
    except TypeError:
        pass
    try:
        dbreply = authcursor('SELECT fn, ln from authors WHERE pmid = ?', [cite])
        authors = authorstostring(dbreply)
        if authors != "": returntext += "Authors: " + authors + "<br>"
    except TypeError:
        pass
    return returntext

def authorstostring(dbreply, links = False):
    if links:
        authors = ""
        lenreply = len(dbreply)
        for i, entry in enumerate(dbreply):
            if i != lenreply-1:
                authors += "<a href = " + flask.url_for('show_author', authname = entry[0] + " " + entry[1]) + " > " + entry[0] + " " + entry[1] + "</a>" + ", "
            else:
                authors += "and " + "<a href = " + flask.url_for('show_author', authname = entry[0] + " " + entry[1]) + " > " + entry[0] + " " + entry[1] + "</a>"
        return authors
    else:
        authors = ""
        lenreply = len(dbreply)
        for i, entry in enumerate(dbreply):
            if i != lenreply-1:
                authors += entry[0] + " " + entry[1] + ", "
            else:
                authors += "and " + entry[0] + " " + entry[1]
        return authors

def rgbtohex(rgbtupleorlistoftuples):
    if type(rgbtupleorlistoftuples) == list:
        returnlist = []
        for tup in rgbtupleorlistoftuples:
            returnlist.append(struct.pack('BBB',*tup).encode('hex'))
        return returnlist
    else:
        return struct.pack('BBB',*rgbtupleorlistoftuples).encode('hex')

@app.route('/')
def main():
  return flask.redirect('/index')

@app.route('/index')
def index():
    args = flask.request.args
    randompmid = int(getitem(args, 'randompmid', '0'))
    if (randompmid == 1):
        PMID = query_db_full('SELECT * FROM highlycitedpmids ORDER BY RANDOM() LIMIT 1',  one=True)[0]
        title = query_db_full('SELECT title FROM meta WHERE pmid = ?', [PMID], one=True)[0]
    else:
        PMID = getitem(args, 'PMID', '')
        #check that it exists, if not set PMID to "PMID_DOES_NOT_EXIST_IN_DB"
        try:
            title = query_db_full('SELECT title FROM meta WHERE pmid = ?', [PMID], one=True)[0]
        except TypeError:
            PMID = "PMID_DOES_NOT_EXIST_IN_DB"
    if PMID != '' and PMID != "PMID_DOES_NOT_EXIST_IN_DB":
        citegraph = buildcitenetwork(PMID, query_db_graph, query_db_full, 2, 2)
        citenetwork = unicode(json.dumps(json_graph.node_link_data(citegraph, attrs={'source': 'source', 
                                                                                     'target': 'target', 
                                                                                     'key': 'key', 
                                                                                     'id': 'name',
                                                                                     'color': 'color',
                                                                                     'ln': 'ln'
                                                                      })))
        authorsstring = authorstostring(query_db_full('SELECT fn, ln FROM authors WHERE pmid = ?', [PMID], one=False), links = True)
        authorkey = query_db_full('SELECT keyword FROM keywords WHERE pmid = ?', [PMID], one=False)
        tfidfkey = query_db_full('SELECT keywords FROM tfidf WHERE pmid = ?', [PMID], one=True)[0]
        journal = query_db_full('SELECT journal_id FROM meta WHERE pmid = ?', [PMID], one=True)[0]
        
        html = flask.render_template(
            'index.html',
            PMID = PMID,
            JSONCITENETWORK = citenetwork,
            authors = authorsstring,
            title = title,
            authorkey = authorkey,
            journal = journal,
            tfidfkey = tfidfkey
        )
        
    else:
        html = flask.render_template(
            'index.html',
            PMID = '',
            )
    return html

@app.route('/author/<authname>')
def show_author(authname):
    #try:
    authname = authname.lower().replace(" ", "")
    authortree = buildfnauthortree(authname, query_db_graph, query_db_full, 2)
    authornetwork = unicode(json.dumps(json_graph.tree_data(authortree, lookupfn(authname, query_db_full), 
                                                            attrs={'children': 'children', 'id': 'name'})))
    #except TypeError:
    #    authornetwork = ''
    html = flask.render_template(
        'authors.html',
        JSONAUTHORNETWORK = authornetwork)
    return html

@app.route('/js/<path:path>')
def send_js(path):
    return send_from_directory('js', path)
    
@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_dbfull', None)
    if db is not None:
        db.close()
    db = getattr(g, '_dbgraph', None)
    if db is not None:
        db.close()

if __name__ == '__main__':
    app.run(debug=False,host='127.0.0.1',port=8000)
