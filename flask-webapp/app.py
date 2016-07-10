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
import numpy as np
import datetime

DBfull = 'db/pmcv3-full.db'
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

def incitepercentile(pmid, fulldb, graphdb): 
    counts = [13027, 34108, 33621, 34843, 37047, 30559, 22854, 18040, 16354, 13309, 11530, 8719, 6640, 5484, 5449, 4666, 5021, 3330, 3152, 2689, 3257, 2887, 1743, 1852, 1591, 1778, 1336, 773, 1851, 491, 396, 1063, 1589, 338, 407, 922, 623, 406, 128, 457, 999, 677, 410, 190, 73, 492, 535, 423, 601, 47, 449, 175, 53, 43, 363, 572, 38, 497, 507, 39, 406, 27, 150, 22, 139, 30, 182, 24, 410, 24, 22, 553, 19, 18, 499, 12, 397, 20, 14, 19, 25, 15, 19, 18, 9, 14, 250, 9, 7, 10, 381, 9, 10, 7, 5, 327, 13, 7, 9, 7, 389, 9, 7, 6, 12, 277, 8, 6, 4, 6, 7, 72, 1, 5, 6, 5, 5, 38, 5, 4, 4, 6, 7, 5, 1, 144, 4, 6, 0, 6, 1, 4, 1, 179, 3, 3, 7, 1, 2, 2, 2, 1, 135, 1, 4, 4, 3, 2, 5, 5, 1, 5, 1, 101, 4, 3, 2, 2, 0, 1, 3, 3, 2, 3, 4, 1, 133, 3, 1, 3, 0, 0, 2, 2, 3, 3, 2, 1, 3, 3, 3, 99, 3, 1, 2, 1, 2, 5, 1, 0, 2, 3, 2, 0, 2, 1, 3, 2, 0, 1]
    sumcounts = 347499.
    div = [ 0. , 0.0005, 0.001 , 0.0015, 0.002 , 0.0025, 0.003 , 0.0035, 0.004 , 0.0045, 0.005 , 0.0055, 0.006 , 0.0065, 0.007 , 0.0075, 0.008 , 0.0085, 0.009 , 0.0095, 0.01 , 0.0105, 0.011 , 0.0115, 0.012 , 0.0125, 0.013 , 0.0135, 0.014 , 0.0145, 0.015 , 0.0155, 0.016 , 0.0165, 0.017 , 0.0175, 0.018 , 0.0185, 0.019 , 0.0195, 0.02 , 0.0205, 0.021 , 0.0215, 0.022 , 0.0225, 0.023 , 0.0235, 0.024 , 0.0245, 0.025 , 0.0255, 0.026 , 0.0265, 0.027 , 0.0275, 0.028 , 0.0285, 0.029 , 0.0295, 0.03 , 0.0305, 0.031 , 0.0315, 0.032 , 0.0325, 0.033 , 0.0335, 0.034 , 0.0345, 0.035 , 0.0355, 0.036 , 0.0365, 0.037 , 0.0375, 0.038 , 0.0385, 0.039 , 0.0395, 0.04 , 0.0405, 0.041 , 0.0415, 0.042 , 0.0425, 0.043 , 0.0435, 0.044 , 0.0445, 0.045 , 0.0455, 0.046 , 0.0465, 0.047 , 0.0475, 0.048 , 0.0485, 0.049 , 0.0495, 0.05 , 0.0505, 0.051 , 0.0515, 0.052 , 0.0525, 0.053 , 0.0535, 0.054 , 0.0545, 0.055 , 0.0555, 0.056 , 0.0565, 0.057 , 0.0575, 0.058 , 0.0585, 0.059 , 0.0595, 0.06 , 0.0605, 0.061 , 0.0615, 0.062 , 0.0625, 0.063 , 0.0635, 0.064 , 0.0645, 0.065 , 0.0655, 0.066 , 0.0665, 0.067 , 0.0675, 0.068 , 0.0685, 0.069 , 0.0695, 0.07 , 0.0705, 0.071 , 0.0715, 0.072 , 0.0725, 0.073 , 0.0735, 0.074 , 0.0745, 0.075 , 0.0755, 0.076 , 0.0765, 0.077 , 0.0775, 0.078 , 0.0785, 0.079 , 0.0795, 0.08 , 0.0805, 0.081 , 0.0815, 0.082 , 0.0825, 0.083 , 0.0835, 0.084 , 0.0845, 0.085 , 0.0855, 0.086 , 0.0865, 0.087 , 0.0875, 0.088 , 0.0885, 0.089 , 0.0895, 0.09 , 0.0905, 0.091 , 0.0915, 0.092 , 0.0925, 0.093 , 0.0935, 0.094 , 0.0945, 0.095 , 0.0955, 0.096 , 0.0965, 0.097 , 0.0975, 0.098 , 0.0985, 0.099 , 0.0995, 0.1 ]
    date = fulldb('''SELECT pubdate FROM meta WHERE pmid = ? ''', [pmid], one = True)[0]
    date = datetime.date(int(date[0:4]), int(date[5:7]), int(date[8:10]))
    daysout = abs((date - datetime.date(2016,6,1)).days) # days between pub and data being fetched
    if daysout <= 0:
        return 0
    else:
        incites = len(graphdb('''SELECT incites FROM cites WHERE pmid = ? ''', [pmid], one=True)[0].split(','))
        citeratio = incites / float(daysout)
    # lookup in histogram
    for i, division in enumerate(div):
        if citeratio < division:
            return np.array(counts[0:i+1]).sum() / sumcounts * 100

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
        try:
            title = query_db_full('SELECT title FROM meta WHERE pmid = ?', [PMID], one=True)[0]
        except TypeError:
            PMID = ""
    if PMID != '':
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
        authorkeytemp = ''
        for key in authorkey:
            authorkeytemp += key[0] + ", "
        authorkey = authorkeytemp.rstrip(", ")
        journal = query_db_full('SELECT journal_id FROM meta WHERE pmid = ?', [PMID], one=True)[0]
        barcolorscheme = map(rgbtohex, colorbrewer.RdYlGn[10])
        try:
            incitep = int(incitepercentile(PMID, query_db_full, query_db_graph))
            if incitep > 0 and incitep < 100:
                incitecol = barcolorscheme[incitep/10]
            else:
                incitecol = 'f7f7f7'
            inciteplab = 'Influence'
        except TypeError: #date missing for entry
            incitep = 0
            inciteplab = 'Influence Unavailable (pubdate missing for record)'
            incitecol = 'f7f7f7'
        try: #fetch top 1 most similar pubs by cosine similarity of tf-idf
            response = query_db_full('SELECT similar FROM similarpubs WHERE pmid = ?', [PMID], one=True)[0]
            similarpmids = []
            for item in response.replace(" ","").split(","):
                similarpmids.append(int(item))
            similarlist = '<ol style="line-height: 160%;" >'
            for item in similarpmids:
                title = query_db_full('SELECT title FROM meta WHERE pmid = ?', [item], one=True)[0]
                journal = query_db_full('SELECT journal_id FROM meta WHERE pmid = ?', [item], one=True)[0]
                similarauths = authorstostring(query_db_full('SELECT fn, ln FROM authors WHERE pmid = ?', [item], one=False), links = False)
                similarlist = similarlist + '<li>' +  "<a href = " + flask.url_for('index', PMID = item) + " > " + title + '</a>, <em>' + journal + '</em>' + '<br>' + similarauths + '</li>'
            similarlist = similarlist + '</ol>'
        except:
            similarlist = '<p class="bg-warning">Not computed for this PMID</p>'
        try: #fetch abstract
            abstract = query_db_full('SELECT abstract FROM abstracts WHERE pmid = ?', [PMID], one=True)[0]
        except:
            abstract = ''

        html = flask.render_template(
            'index.html',
            PMID = PMID,
            JSONCITENETWORK = citenetwork,
            authors = authorsstring,
            title = title,
            authorkey = authorkey,
            journal = journal,
            incitep = incitep,
            incitecol = incitecol,
            inciteplab = inciteplab,
            similarlist = similarlist,
            abstract = abstract
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
    try:
        authortree = buildfnauthortree(authname, query_db_graph, query_db_full, 2)
        authornetwork = unicode(json.dumps(json_graph.tree_data(authortree, lookupfn(authname, query_db_full), 
                                                                attrs={'children': 'children', 'id': 'name'})))
    except TypeError:
        authornetwork = ''
    html = flask.render_template(
        'authors.html',
        JSONAUTHORNETWORK = authornetwork)
    return html

@app.route('/js/<path:path>')
def send_js(path):
    return send_from_directory('js', path)

@app.route('/css/<path:path>')
def send_css(path):
    return send_from_directory('css', path)
    
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
