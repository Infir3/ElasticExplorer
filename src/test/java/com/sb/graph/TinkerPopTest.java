package com.sb.graph;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

public class TinkerPopTest {

    @Test
    public void basicGraphMutations() {
        Graph graph = TinkerGraph.open();
        // add a software vertex with a name property
        Vertex gremlin = graph.addVertex(T.label, "software",
            "name", "gremlin"); //1
        // only one vertex should exist
        assert (IteratorUtils.count(graph.vertices()) == 1);
        // no edges should exist as none have been created
        assert (IteratorUtils.count(graph.edges()) == 0);
        // add a new property
        gremlin.property("created", 2009); //2
        // add a new software vertex to the graph
        Vertex blueprints = graph.addVertex(T.label, "software",
            "name", "blueprints"); //3
        // connect gremlin to blueprints via a dependsOn-edge
        gremlin.addEdge("dependsOn", blueprints); //4
        // now there are two vertices and one edge
        assert (IteratorUtils.count(graph.vertices()) == 2);
        assert (IteratorUtils.count(graph.edges()) == 1);
        // add a property to blueprints
        blueprints.property("created", 2010); //5
        // remove that property
        blueprints.property("created").remove(); //6
        // connect gremlin to blueprints via encapsulates
        gremlin.addEdge("encapsulates", blueprints); //7
        assert (IteratorUtils.count(graph.vertices()) == 2);
        assert (IteratorUtils.count(graph.edges()) == 2);
        // removing a vertex removes all its incident edges as well
        blueprints.remove(); //8
        gremlin.remove(); //9
        // the graph is now empty
        assert (IteratorUtils.count(graph.vertices()) == 0);
        assert (IteratorUtils.count(graph.edges()) == 0);
        // tada!
    }

    @Test
    public void traverseModernGraph() {
        Graph graph = TinkerFactory.createModern();
        GraphTraversalSource g = graph.traversal();
        Vertex marko = g.V(1).next();
        System.out.println((String) marko.value("name"));

        marko.vertices(Direction.OUT, "knows").forEachRemaining(vertex -> {
            System.out.println((String) vertex.value("name"));
        });

        marko = g.V().has("name", "marko").next();
        System.out.println((Integer) marko.value("age"));

        g.V().has("name", P.within("vadas", "marko")).forEachRemaining(vertex -> {
            System.out.println((Integer) vertex.value("age"));
        });

        g.V().has("name", P.within("vadas", "marko")).values("age").mean().forEachRemaining(
            (Number vertex) -> {
                System.out.println(vertex);
            });

        // find out who created the software that marko created
        g.V().has("name", "marko").out("created").in("created").forEachRemaining(vertex -> {
            System.out.println((String) vertex.value("name"));
        });

        // find out marko's collaborators
        g.V().has("name", "marko").as("exclude")
            .out("created").in("created")
            .where(P.neq("exclude"))
            .forEachRemaining(vertex -> {
                System.out.println((String) vertex.value("name"));
            });

        // group all the vertices in the graph by their vertex label
        g.V().group().by(T.label).forEachRemaining(objectObjectMap -> {
            System.out.println(objectObjectMap.toString());
        });

        // group all the vertices in the graph by their vertex label
        g.V().group().by(T.label).by("name").forEachRemaining(objectObjectMap -> {
            System.out.println(objectObjectMap.toString());
        });
    }

    @Test
    public void createHugeGraph() {
        final int numberOfEdges = 100000;
        final int numberOfVertices = 1000000;
        Graph graph = TinkerGraph.open();
        GraphTraversalSource g = graph.traversal();
        Map<Object, Vertex> vertexMap = new HashMap<>(numberOfVertices);
        for (int i = 0; i < numberOfVertices; i++) {
            Vertex vertex = graph.addVertex(String.valueOf(i));
            vertexMap.put(vertex.id(), vertex);
        }
        Random random = new Random();
        for (int i = 0; i < numberOfEdges; i++) {
            Vertex outVertex = vertexMap.get((long) random.nextInt(numberOfEdges));
            Vertex inVertex = vertexMap.get((long) random.nextInt(numberOfEdges));
            outVertex.addEdge(String.valueOf(i), inVertex);
        }
    }

}
