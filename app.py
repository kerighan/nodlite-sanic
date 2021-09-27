import os

from aiocache import cached
from nodlite import Graph
from sanic import Sanic, response
from sanic_cors import CORS

from database import ThreadedGraph
from utils import json_task
from variables import DATABASE, TTL, VERSION

folder = os.path.dirname(DATABASE)
if folder != "" and not os.path.exists(folder):
    os.makedirs(folder)


# graphs
G = ThreadedGraph(Graph(DATABASE))
H = G.G

# create flask app
app = Sanic(__name__, strict_slashes=False)
CORS(app)


# =============================================================================
# GENERAL FUNCTIONS
# =============================================================================

@app.get("")
async def home(_):
    return response.json({
        "version": {
            "server": VERSION
        }
    })


@app.get("/_count")
async def count(_):
    return G.count()


# =============================================================================
# EDGE
# =============================================================================

@app.post("/edge/<u>/<v>")
@json_task
async def add_edge(_, u, v):
    return G.add_edge(u, v)


@app.delete("/edge/<u>/<v>")
@json_task
async def remove_edge(_, u, v):
    return G.remove_edge(u, v)


@app.get("/edge/<u>/<v>")
@cached(ttl=TTL)
@json_task
async def edge(_, u, v):
    return G.edge(u, v)


@app.get("/edge")
@json_task
async def batch_get_edges(request):
    batch_size = int(request.args.get("size", 100))
    page = int(request.args.get("page", 0))
    return G.batch_get_edges(batch_size=batch_size, page=page)


@app.post("/subgraph")
@cached(ttl=TTL)
async def subgraph(request):
    try:
        _nodes = request.json["nodes"]
    except Exception:
        _nodes = [it.strip() for it in request.form["nodes"][0].split(",")]
    return G.subgraph(_nodes)


# =============================================================================
# NODE
# =============================================================================

@app.post("/node/<u>")
@json_task
async def add_node(_, u):
    return G.add_node(u)


@app.delete("/node/<u>")
@json_task
async def remove_node(_, u):
    return G.remove_node(u)


@app.get("/node/<u>")
@cached(ttl=TTL)
@json_task
async def node(_, u):
    return G.node(u)


@app.get("/node")
@json_task
async def batch_get_nodes(request):
    batch_size = int(request.args.get("size", 100))
    page = int(request.args.get("page", 0))
    return G.batch_get_nodes(batch_size=batch_size, page=page)


# =============================================================================
# NEIGHBORHOOD
# =============================================================================

@app.post("/neighbors")
@cached(ttl=TTL)
async def neighbors_from(request):
    try:
        _nodes = request.json["nodes"]
    except Exception:
        _nodes = [it.strip() for it in request.form["nodes"][0].split(",")]
    return G.neighbors_from(_nodes)


@app.get("/neighbors/<u>")
@cached(ttl=TTL)
async def neighbors(_, u):
    return G.neighbors(u)


@app.post("/neighbors/<u>")
@cached(ttl=TTL)
@json_task
async def set_neighbors(request, u):
    nbs = request.json["nodes"]
    return G.set_neighbors(u, nbs)


@app.post("/predecessors")
@cached(ttl=TTL)
async def predecessors_from(request):
    try:
        _nodes = request.json["nodes"]
    except Exception:
        _nodes = [it.strip() for it in request.form["nodes"][0].split(",")]
    return G.predecessors_from(_nodes)


@app.get("/predecessors/<u>")
@cached(ttl=TTL)
async def predecessors(_, u):
    return G.predecessors(u)


@app.post("/predecessors/<u>")
@cached(ttl=TTL)
@json_task
async def set_predecessors(request, u):
    nbs = request.json["nodes"]
    return G.set_predecessors(u, nbs)


if __name__ == "__main__":
    app.run("0.0.0.0", port=3200, debug=True)
