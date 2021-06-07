package edu.buffalo.cse.cse486586.simpledynamo;

public class Node implements Comparable<Node> {

    private String nodeId;
    private String nodeKey;
    private String nodePort;
    private Node predecessor1;
    private Node predecessor2;
    private Node successor1;
    private Node successor2;

    public Node(String nodeId, String nodeKey, String nodePort, Node predecessor1, Node predecessor2, Node successor1, Node successor2) {
        this.nodeId = nodeId;
        this.nodeKey = nodeKey;
        this.nodePort = nodePort;
        this.predecessor1 = predecessor1;
        this.predecessor2 = predecessor2;
        this.successor1 = successor1;
        this.successor2 = successor2;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeKey() {
        return nodeKey;
    }

    public void setNodeKey(String nodeKey) {
        this.nodeKey = nodeKey;
    }

    public String getNodePort() {
        return nodePort;
    }

    public void setNodePort(String nodePort) {
        this.nodePort = nodePort;
    }

    public Node getPredecessor1() {
        return predecessor1;
    }

    public void setPredecessor1(Node predecessor1) {
        this.predecessor1 = predecessor1;
    }

    public Node getPredecessor2() {
        return predecessor2;
    }

    public void setPredecessor2(Node predecessor2) {
        this.predecessor2 = predecessor2;
    }

    public Node getSuccessor1() {
        return successor1;
    }

    public void setSuccessor1(Node successor1) {
        this.successor1 = successor1;
    }

    public Node getSuccessor2() {
        return successor2;
    }

    public void setSuccessor2(Node successor2) {
        this.successor2 = successor2;
    }

    @Override
    public int compareTo(Node another) {
        return this.nodeKey.compareTo(another.nodeKey);
    }

    @Override
    public String toString() {
        return "Node{" +
                "nodeId='" + nodeId + '\'' +
                ", nodeKey='" + nodeKey + '\'' +
                ", nodePort='" + nodePort + '\'' +
                ", predecessor1=" + predecessor1.getNodeId() +
                ", predecessor2=" + predecessor2.getNodeId() +
                ", successor1=" + successor1.getNodeId() +
                ", successor2=" + successor2.getNodeId() +
                '}';
    }
}

