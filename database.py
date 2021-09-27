import asyncio
import queue
import random
import threading
import time
from dataclasses import dataclass
from enum import Enum
from functools import wraps

from sanic import response


def error(f):
    @wraps(f)
    def wrapper(*arg, **kwargs):
        try:
            f(*arg, **kwargs)
        except KeyError:
            arg[1].done({"error": "resource not found"}, 404)
        except Exception as e:
            arg[1].done({"error": str(e)}, 500)
    return wrapper


def direct_error(f):
    @wraps(f)
    def wrapper(*arg, **kwargs):
        try:
            return f(*arg, **kwargs)
        except Exception as e:
            return response.json({"error": str(e)}, 404)
    return wrapper


class Action(Enum):
    CLOSE = 0
    COUNT = 1
    ADD_EDGE = 2
    ADD_NODE = 3
    REMOVE_EDGE = 4
    REMOVE_NODE = 5
    GET_EDGE = 6
    GET_NODE = 7
    GET_BATCH_EDGES = 8
    GET_BATCH_NODES = 9
    GET_NEIGHBORS = 10
    SET_NEIGHBORS = 11
    GET_PREDECESSORS = 12
    SET_PREDECESSORS = 13
    GET_NEIGHBORS_FROM = 14
    GET_PREDECESSORS_FROM = 15


class ThreadedGraph(threading.Thread):
    def __init__(self, G, daemon=True):
        super().__init__()
        self.G = G
        self.q = queue.PriorityQueue()
        self.setDaemon(daemon)
        self.start()

    def run(self):
        action_function = {
            Action.ADD_EDGE: self.handle_add_edge,
            Action.ADD_NODE: self.handle_add_node,
            Action.GET_EDGE: self.handle_get_edge,
            Action.GET_NODE: self.handle_get_node,
            Action.GET_BATCH_EDGES: self.handle_get_batch_edges,
            Action.GET_BATCH_NODES: self.handle_get_batch_nodes,
            Action.GET_NEIGHBORS: self.handle_get_neighbors,
            Action.GET_PREDECESSORS: self.handle_get_predecessors,
            Action.SET_NEIGHBORS: self.handle_set_neighbors,
            Action.SET_PREDECESSORS: self.handle_set_predecessors,
            Action.REMOVE_EDGE: self.handle_remove_edge,
            Action.REMOVE_NODE: self.handle_remove_node,
            Action.COUNT: self.handle_count,
            Action.CLOSE: self.handle_close
        }
        self.loop = True
        while self.loop:
            _, (task, action, arg) = self.q.get()
            action_function[action](task, arg)
            self.q.task_done()
        print("closing...")

    def put(self, priority, action, arg=None):
        task = Task()
        self.q.put((priority, (task, action, arg)))
        return task

    def close(self):
        return self.put(2, Action.CLOSE)

    def add_edge(self, u, v):
        return self.put(1, Action.ADD_EDGE, (u, v))

    def add_node(self, u):
        return self.put(1, Action.ADD_NODE, u)

    def remove_edge(self, u, v):
        return self.put(1, Action.REMOVE_EDGE, (u, v))

    def remove_node(self, u):
        return self.put(1, Action.REMOVE_NODE, u)

    def edge(self, u, v):
        return self.put(0, Action.GET_EDGE, (u, v))

    def node(self, u):
        return self.put(0, Action.GET_NODE, u)

    def batch_get_edges(self, batch_size=100, page=0):
        return self.put(0, Action.GET_BATCH_EDGES, (batch_size, page))

    def batch_get_nodes(self, batch_size=100, page=0):
        return self.put(0, Action.GET_BATCH_NODES, (batch_size, page))

    @direct_error
    def count(self):
        n_nodes = self.G.n_nodes
        n_edges = self.G.n_edges
        avg_deg = round(n_edges / n_nodes, 1) if n_nodes != 0 else 0
        data = {
            "nodes_count": self.G.n_nodes,
            "edges_count": self.G.n_edges,
            "avg_degree": avg_deg}
        return response.json(data, 200)

    def neighbors(self, u):
        data = {
            "node": u,
            "neighbors": list(self.G.neighbors(u))
        }
        return response.json(data, 200)

    def neighbors_from(self, _nodes):
        data = {
            "neighbors": self.G.neighbors_from(_nodes)
        }
        return response.json(data, 200)

    def predecessors(self, u):
        data = {
            "node": u,
            "predecessors": list(self.G.predecessors(u))
        }
        return response.json(data, 200)

    def predecessors_from(self, _nodes):
        data = {
            "predecessors": self.G.predecessors_from(_nodes)
        }
        return response.json(data, 200)

    def set_neighbors(self, u, nodes):
        return self.put(1, Action.SET_NEIGHBORS, (u, nodes))

    def set_predecessors(self, v, nodes):
        return self.put(1, Action.SET_PREDECESSORS, (v, nodes))

    def subgraph(self, _nodes):
        data = {
            "edges": list(self.G.subgraph(_nodes)),
            "_nodes": _nodes
        }
        return response.json(data, 200)

    def join(self):
        self.close()
        super().join()

    def handle_close(self, *_):
        self.loop = False

    @error
    def handle_add_edge(self, task, arg):
        src, tgt = arg
        self.G.add_edge(src, tgt)
        couple = {"source": src, "target": tgt}
        data = {"edge": couple, "created": True}
        task.done(data, 200)

    @error
    def handle_add_node(self, task, arg):
        self.G.add_node(arg)
        data = {"node": arg, "created": True}
        task.done(data, 200)

    @error
    def handle_remove_edge(self, task, arg):
        src, tgt = arg
        self.G.remove_edge(src, tgt)
        couple = {"source": src, "target": tgt}
        data = {"edge": couple, "removed": True}
        task.done(data, 200)

    @error
    def handle_get_edge(self, task, arg):
        src, tgt = arg
        self.G.edge(src, tgt)
        couple = {"source": src, "target": tgt}
        data = {"edge": couple, "found": True}
        task.done(data, 200)

    @error
    def handle_get_node(self, task, u):
        self.G.node(u)
        data = {
            "node": u,
            "found": True
        }
        task.done(data, 200)

    @error
    def handle_remove_node(self, task, u):
        self.G.remove_node(u)
        data = {"node": u, "removed": True}
        task.done(data, 200)

    @error
    def handle_get_batch_edges(self, task, arg):
        batch_size, page = arg
        edges = list(self.G.batch_get_edges(
            batch_size=batch_size, page=page))
        task.done({"edges": edges, "page": page}, 200)

    @error
    def handle_get_batch_nodes(self, task, arg):
        batch_size, page = arg
        nodes = list(self.G.batch_get_nodes(
            batch_size=batch_size, page=page))
        task.done({
            "nodes": nodes,
            "page": page}, 200)

    @error
    def handle_count(self, task, _):
        n_nodes = self.G.n_nodes
        n_edges = self.G.n_edges
        avg_deg = round(n_edges / n_nodes, 1) if n_nodes != 0 else 0
        task.done({"nodes_count": self.G.n_nodes,
                   "edges_count": self.G.n_edges,
                   "avg_degree": avg_deg}, 200)

    @error
    def handle_get_neighbors(self, task, arg):
        data = {
            "node": arg,
            "neighbors": list(self.G.neighbors(arg))
        }
        task.done(data, 200)

    @error
    def handle_set_neighbors(self, task, arg):
        self.G.set_neighbors(arg[0], arg[1])
        data = {"success": True}
        task.done(data, 200)

    @error
    def handle_get_predecessors(self, task, arg):
        data = {
            "node": arg,
            "predecessors": list(self.G.predecessors(arg))
        }
        task.done(data, 200)

    @error
    def handle_set_predecessors(self, task, arg):
        self.G.set_predecessors(arg[0], arg[1])
        data = {"success": True}
        task.done(data, 200)


@dataclass
class Task:
    __slots__ = "pending", "timestamp", "data", "status"

    def __init__(self):
        self.pending = True
        self.timestamp = time.time() + random.random()

    def done(self, data=None, status=None):
        self.pending = False
        if data is not None:
            self.data = data
        if status is not None:
            self.status = status

    async def wait(self):
        while self.pending:
            await asyncio.sleep(1e-6)
