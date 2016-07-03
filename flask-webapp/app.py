import flask
from flask import g
import os
import sqlite3
from io import BytesIO
import base64
import Queue
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

app = flask.Flask(__name__)

def getitem(obj, item, default):
    if item not in obj:
        return default
    else:
        return obj[item]

def buildfnauthortree(rootnode, mastergraphcursor, fndict, depth = 2):
    _g =nx.DiGraph()
    q = Queue.Queue()
    q.put((rootnode, 0))
    while not q.empty():
        node = q.get()
        if node[1] < depth:
            mastergraphcursor.execute('''SELECT coauthors FROM coauthors WHERE author = ?''', [node[0]])
            coauthors = cg.fetchone()[0].split(',')
            for author in coauthors:
                if unicode(fndict[author][0]+" "+fndict[author][1]) not in _g.nodes():
                    _g.add_edge(unicode(fndict[node[0]][0]+ " "+fndict[node[0]][1]), 
                                unicode(fndict[author][0]+" "+fndict[author][1]))
                    q.put((author, node[1]+1))
    return _g

def buildcitenetwork(rootnode, mastergraphcursor, authcursor, indepth = 0, outdepth = 2, 
                     colorscheme = colorbrewer.RdBu):
    _g =nx.DiGraph()
    q = Queue.Queue()
    #set up colors
    _colors = colorscheme[max(outdepth,indepth)*2+1]
    _colors.reverse()
    #first go in out direction
    q.put((rootnode, 0))
    try:
        lastname = authcursor('SELECT ln FROM authors WHERE pmid = ? AND authnum = 0', [rootnode], one=True)[0]
    except TypeError:
        lastname = rootnode
    _g.add_node(rootnode, color = rgbtohex(_colors[(len(_colors)-1)/2]), ln = lastname)
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
                        _g.add_node(cite, color = rgbtohex(_colors[(len(_colors)-1)/2+node[1]+1]), ln = lastname)
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
                        _g.add_node(cite, color = rgbtohex(_colors[(len(_colors)-1)/2-node[1]-1]), ln = lastname)
                        _g.add_edge(cite, node[0])
                        q.put((cite, node[1]+1))
            except ValueError:
                pass
    return _g

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
    else:
        PMID = getitem(args, 'PMID', '')
    if PMID != '':
        citegraph = buildcitenetwork(PMID, query_db_graph, query_db_full, 2, 2)
        citenetwork = unicode(json.dumps(json_graph.node_link_data(citegraph, attrs={'source': 'source', 
                                                                                     'target': 'target', 
                                                                                     'key': 'key', 
                                                                                     'id': 'name',
                                                                                     'color': 'color',
                                                                                     'ln': 'ln'
                                                                      })))
    else:
        citenetwork = ''
    metastring = 'meta'
    html = flask.render_template(
        'index.html',
        PMID = PMID,
        JSONCITENETWORK = citenetwork,
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
    
@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_dbfull', None)
    if db is not None:
        db.close()
    db = getattr(g, '_dbgraph', None)
    if db is not None:
        db.close()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    if port == 5000: 
        debug = True
    else:
        debug = False
    app.run(debug=debug,host='0.0.0.0',port=port)   
