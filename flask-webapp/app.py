import flask 
import os
import sqlite3
from flask import g
from io import BytesIO
import base64
import graph_tool.all as gt
import Queue
import random

DATABASE = '../pmcv1-full.db'

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db

def query_db(query, args=(), one=False):
    cur = get_db().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv

app = flask.Flask(__name__)

def getitem(obj, item, default):
    if item not in obj:
        return default
    else:
        return obj[item]

import cPickle as pickle
#g = pickle.load(open("../full_graph.p", "rb"))
g = gt.load_graph("../full_graph.gt")
pmid_vertex_dict = pickle.load(open("../full_graph_pmid_vertex_dict.p", "rb"))
rev_pmid_vertex_dict = {v: k for k, v in pmid_vertex_dict.items()}
randpmids = pickle.load(open("../random_choice_pmids.p", "rb"))
#g = pickle.load(open("authors_full_graph.p", "rb"))
gauth = gt.load_graph("../authors_full_graph.gt")
author_vertex_dict = pickle.load(open("../authors_vertex_dict.p", "rb"))
author_full_name_dict = pickle.load(open("../authors_full_name_dict.p", "rb"))
rev_author_vertex_dict = {v: k for k, v in author_vertex_dict.items()}

def addedge(graphobject, source, dest, vertexdict):
    if source not in vertexdict:
        v = graphobject.add_vertex()
        vertexdict[source] = int(v)
    if dest not in vertexdict:
        v = graphobject.add_vertex()
        vertexdict[dest] = int(v)
    graphobject.add_edge(vertexdict[source], vertexdict[dest])
    return graphobject, vertexdict

                
def buildlocalgraph(rootnode, mastergraph, indepth = 0, outdepth = 2):
    _g =gt.Graph()
    _vertexdict = dict()
    q = Queue.Queue()
    #first go in out direction
    q.put((rootnode, 0))
    while not q.empty():
        node = q.get()
        if node[1] < outdepth:
            try:
                for neigh in mastergraph.vertex(node[0]).out_neighbours():
                    _g, _vertexdict = addedge(_g, node[0], neigh, _vertexdict)
                    q.put((neigh, node[1]+1))
            except KeyError:
                "{} degree node {} not in graph g".format(node[1], node[0])
    #now go in in direction
    q.put((rootnode, 0))
    while not q.empty():
        node = q.get()
        if node[1] < indepth:
            try:
                for neigh in mastergraph.vertex(node[0]).in_neighbours():
                    _g, _vertexdict = addedge(_g, neigh, node[0], _vertexdict)
                    q.put((neigh, node[1]+1))
            except KeyError:
                "{} degree node {} not in graph g".format(node[1], node[0])
    return _g, _vertexdict

def addedgelabeledvertex(graphobject, source, dest, vertexdict, v_label):
    if source not in vertexdict:
        v = graphobject.add_vertex()
        vertexdict[source] = int(v)
        v_label[v] = str(rev_pmid_vertex_dict[dest])
    if dest not in vertexdict:
        v = graphobject.add_vertex()
        vertexdict[dest] = int(v)
        v_label[v] = str(rev_pmid_vertex_dict[dest])
    graphobject.add_edge(vertexdict[source], vertexdict[dest])
    return graphobject, vertexdict, v_label

def buildlabeledlocalgraph(rootnode, mastergraph, indepth = 0, outdepth = 2):
    _g =gt.Graph()
    _vertexdict = dict()
    q = Queue.Queue()
    v_label = _g.new_vertex_property("string")
    #first go in out direction
    q.put((rootnode, 0))
    while not q.empty():
        node = q.get()
        if node[1] < outdepth:
            try:
                for neigh in mastergraph.vertex(node[0]).out_neighbours():
                    _g, _vertexdict, v_label = addedgelabeledvertex(_g, node[0], neigh, _vertexdict, v_label)
                    q.put((neigh, node[1]+1))
            except KeyError:
                print "{} degree node {} not in graph g".format(node[1], node[0])
    #now go in in direction
    q.put((rootnode, 0))
    while not q.empty():
        node = q.get()
        if node[1] < indepth:
            try:
                for neigh in mastergraph.vertex(node[0]).in_neighbours():
                    _g, _vertexdict, v_label = addedgelabeledvertex(_g, neigh, node[0], _vertexdict, v_label)
                    q.put((neigh, node[1]+1))
            except KeyError:
                "{} degree node {} not in graph g".format(node[1], node[0])
    return _g, _vertexdict, v_label

def authoraddedge(graphobject, source, dest, vertexdict, v_label):
    if source not in vertexdict:
        v = graphobject.add_vertex()
        vertexdict[source] = int(v)
        v_label[v] = str(author_full_name_dict[rev_author_vertex_dict[source]]).strip('()').strip("'").replace("u'", "").replace("',", "")
    if dest not in vertexdict:
        v = graphobject.add_vertex()
        vertexdict[dest] = int(v) 
        #author_full_name_dict[rev_author_vertex_dict[150083]]
        v_label[v] = str(author_full_name_dict[rev_author_vertex_dict[dest]]).strip('()').strip("'").replace("u'", "").replace("',", "")
    graphobject.add_edge(vertexdict[source], vertexdict[dest])
    return graphobject, vertexdict, v_label

def buildlocalgraphundirectedauthor(rootnode, mastergraph, depth, limit = 100):
    _g =gt.Graph(directed = False)
    _v_label = _g.new_vertex_property("string")
    _vertexdict = dict()
    q = Queue.Queue()
    q.put((rootnode, 0))
    nodecount = 0
    while not q.empty():
        node = q.get()
        if node[1] < depth:
            try:
                for neigh in mastergraph.vertex(node[0]).all_neighbours():
                    _g, _vertexdict, _v_label = authoraddedge(_g, node[0], neigh, _vertexdict, _v_label)
                    q.put((neigh, node[1]+1))
                    nodecount += 1
                    if nodecount == limit: break
            except KeyError:
                "{} degree node {} not in graph g".format(node[1], node[0])
        if nodecount == limit: break
    return _g, _vertexdict, _v_label

def countneigh(g, node):
    count = 0
    for neigh in g.vertex(node).all_neighbours(): count+= 1
    return count

@app.route('/')
def main():
  return flask.redirect('/index')

@app.route('/index')
def index():
    cur = get_db().cursor()
    args = flask.request.args
    randompmid = int(getitem(args, 'randompmid', '0'))
    if randompmid == 1:
        PMID = random.choice(randpmids) #21406116 #select random
    else:
        PMID = getitem(args, 'PMID', '')
    #    PMID = int(getitem(args, 'PMID', '21406116'))
    #AUTHOR = getitem(args, 'AUTHOR', '')
    if (PMID != ''):
        PMID = int(PMID)
        #fetch title
        meta = query_db('SELECT title, journal_id from meta WHERE pmid = ?', [PMID], one=True)
        metastring = u"<br>Title: {} <br>Journal Abbreviation: {}".format(meta[0], meta[1])
        authorsprint = unicode()
        authors = query_db('SELECT fn, ln from authors WHERE pmid = ?', [PMID])
        for i, author in enumerate(authors):
            if i < len(authors)-1:
                authorsprint = authorsprint + "<a href = " + flask.url_for('show_author', authname = author[0] + " " + author[1]) + " > " + author[0] + " " + author[1] + "</a>" + ", "
            else:
                #authorsprint = authorsprint + "and " + author[0] + " " + author[1]
                authorsprint = authorsprint + "and " + "<a href = " + flask.url_for('show_author', authname = author[0] + " " + author[1]) + " > " + author[0] + " " + author[1] + "</a>"
        
        metastring = u"<br>Title: {} <br>Journal Abbreviation: {} <br>Authors: {}".format(meta[0], meta[1], authorsprint)

        authorkeywords = query_db('SELECT keyword from keywords WHERE pmid = ?', [PMID])
        tfidfkeywords = query_db('SELECT keywords from tfidf WHERE pmid = ?', [PMID])
        tfidfkeywords = tfidfkeywords[0][0].replace(".", "").replace(",", "").replace(" ", ", ")
        metastring += "<br>" + u"Author's Keywords: {} <br>TF-IDF top 5 keywords: {}".format(authorkeywords, tfidfkeywords)

        # GRAPHING CITE NETWORK
        citegraph, citevertexdict = buildlocalgraph(pmid_vertex_dict[PMID], g, 2,2)
        deg = citegraph.degree_property_map("in") #would be better to color by distance from origin. easy to do with extra property
        pos = gt.sfdp_layout(citegraph)
        pngfigfile = BytesIO()
        gt.graph_draw(citegraph, vertex_fill_color=deg, pos=pos, output_size=(600,600), 
            fmt = 'png', output=pngfigfile)
        pngfigfile.seek(0)
        figdata_png = base64.b64encode(pngfigfile.getvalue())
        #DONE
    else:
        figdata_png = ''
        metastring = ''

    html = flask.render_template(
        'index.html',
        PMID = PMID,
        citefig = figdata_png,
        meta = metastring)
    return html

@app.route('/author/<authname>')
def show_author(authname):
    try:
        #GRAPHING AUTHOR
        pngauthfile = BytesIO()
        author = unicode(authname)
        #author = u"Daniel M. Morobadi"
        author = author.replace(" ","").lower()
        #if countneigh(gauth, author_vertex_dict[author]) <= 15: #ideally should CATCH KeyError: u'soroushyazdi'
        #    degrees = 2
        #else:
        #    degrees = 1
        degrees = 2 #this is fine, now that we are limiting total plotted nodes
        authorgraph, authorvertexdict, v_label = buildlocalgraphundirectedauthor(author_vertex_dict[author], gauth, degrees, limit = 120)
        deg = authorgraph.degree_property_map("total") #out AND in how? #TOTAL!
        state = gt.minimize_nested_blockmodel_dl(authorgraph, deg_corr=True)
        bstack = state.get_bstack()
        t = gt.get_hierarchy_tree(state)[0]
        tpos = pos = gt.radial_tree_layout(t, t.vertex(t.num_vertices() - 1), weighted=True)#
        cts = gt.get_hierarchy_control_points(authorgraph, t, tpos)#crashing here? ##BECAUSE OF G!!!
        pos = authorgraph.own_property(tpos)
        b = bstack[0].vp["b"]
        #labels
        import math
        text_rot = authorgraph.new_vertex_property('double')
        authorgraph.vertex_properties['text_rot'] = text_rot
        for v in authorgraph.vertices():
            if pos[v][0] >0:
                text_rot[v] = math.atan(pos[v][1]/pos[v][0])
            else:
                text_rot[v] = math.pi + math.atan(pos[v][1]/pos[v][0])         
        #text color
        text_col = authorgraph.new_vertex_property("vector<double>")
        for i, v in enumerate(authorgraph.vertices()):
            if i == 0:
                text_col[v] = [1,0,.1,1]
            else:
                text_col[v] = [0,0,0,1]
        gt.graph_draw(authorgraph, pos=pos, 
                    edge_control_points=cts,
                    vertex_size=10,
                    vertex_text=v_label,
                    #vertex_text_rotation=authorgraph.vertex_properties['text_rot'],
                    vertex_text_rotation=text_rot,  
                    vertex_text_position=1,
                    vertex_font_size=14,
                    vertex_anchor=0,
                    vertex_fill_color=deg,
                    vertex_text_color=text_col,
                    #vertex_text_color=[.5,0,.1,1],
                    #bg_color=[0,0,0,1],
                    output_size=[600,600],
                    fmt = 'png',
                    output = pngauthfile)
        pngauthfile.seek(0)
        authfigdata_png = base64.b64encode(pngauthfile.getvalue())
        #DONE
    except KeyError:
        authfigdata_png = ''

    html = flask.render_template(
        'authors.html',
        authfig = authfigdata_png)
    return html
    
#@app.teardown_appcontext
#def close_connection(exception):
#    db = getattr(g, '_database', None)
#    if db is not None:
#        db.close()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    if port == 5000: 
        #debug = True
        debug = False
    else:
        debug = False
    app.run(debug=debug,host='0.0.0.0',port=port)   
