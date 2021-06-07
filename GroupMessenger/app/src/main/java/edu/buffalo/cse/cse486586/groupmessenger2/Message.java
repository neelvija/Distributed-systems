package edu.buffalo.cse.cse486586.groupmessenger2;

public class Message implements Comparable<Message>{
    private String msg;
    private int portNumber;
    private int proposedSequenceNumber;
    private boolean isDeliverable;

    public Message(String msg, int portNumber, int proposedSequenceNumber) {
        this.msg = msg;
        this.portNumber = portNumber;
        this.proposedSequenceNumber = proposedSequenceNumber;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public int getPortNumber() {
        return portNumber;
    }

    public void setPortNumber(int portNumber) {
        this.portNumber = portNumber;
    }

    public int getProposedSequenceNumber() {
        return proposedSequenceNumber;
    }

    public void setProposedSequenceNumber(int proposedSequenceNumber) {
        this.proposedSequenceNumber = proposedSequenceNumber;
    }

    public boolean isDeliverable() {
        return isDeliverable;
    }

    public void setDeliverable(boolean deliverable) {
        isDeliverable = deliverable;
    }

    @Override
    public String toString() {
        return  msg +
                ":" + portNumber +
                ":" + proposedSequenceNumber +
                ":" + isDeliverable;
    }

    @Override
    public int compareTo(Message another) {
        if(this.getProposedSequenceNumber() == another.getProposedSequenceNumber())  {
            return this.getPortNumber()-another.getPortNumber();
        }
        else {
            return this.getProposedSequenceNumber()-another.getProposedSequenceNumber();
        }
    }
}
