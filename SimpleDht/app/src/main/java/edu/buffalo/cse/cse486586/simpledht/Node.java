package edu.buffalo.cse.cse486586.simpledht;

public class Node implements Comparable<Node> {

    private String nodeId;
    private String nodeKey;
    private String nodePort;
    private Node predecessor;
    private Node successor;

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

    public Node getPredecessor() {
        return predecessor;
    }

    public void setPredecessor(Node predecessor) {
        this.predecessor = predecessor;
    }

    public Node getSuccessor() {
        return successor;
    }

    public void setSuccessor(Node successor) {
        this.successor = successor;
    }

    @Override
    public int compareTo(Node another) {
        return this.nodeKey.compareTo(another.nodeKey);
    }
}
