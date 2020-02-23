package edu.buffalo.cse.cse486586.simpledynamo;

public class Node implements Comparable<Node>{
    String node_id;
    String port_num;
    String emulator_num;

    String Predecessor_1_port_num = null;
    String Predecessor_1_node_id = null;
    String Predecessor_2_port_num = null;
    String Predecessor_2_node_id = null;

    String Successor_1_port_num = null;
    String Successor_1_node_id = null;
    String Successor_2_port_num = null;
    String Successor_2_node_id = null;

    public Node(String node_id, String port_num, String emulator_num, String predecessor_1_port_num,
                String predecessor_1_node_id, String predecessor_2_port_num, String predecessor_2_node_id,
                String successor_1_port_num, String successor_1_node_id, String successor_2_port_num,
                String successor_2_node_id) {
        this.node_id = node_id;
        this.port_num = port_num;
        this.emulator_num = emulator_num;
        Predecessor_1_port_num = predecessor_1_port_num;
        Predecessor_1_node_id = predecessor_1_node_id;
        Predecessor_2_port_num = predecessor_2_port_num;
        Predecessor_2_node_id = predecessor_2_node_id;
        Successor_1_port_num = successor_1_port_num;
        Successor_1_node_id = successor_1_node_id;
        Successor_2_port_num = successor_2_port_num;
        Successor_2_node_id = successor_2_node_id;
    }

    public String getNode_id() {
        return node_id;
    }

    public void setNode_id(String node_id) {
        this.node_id = node_id;
    }

    public String getPort_num() {
        return port_num;
    }

    public void setPort_num(String port_num) {
        this.port_num = port_num;
    }

    public String getEmulator_num() {
        return emulator_num;
    }

    public void setEmulator_num(String emulator_num) {
        this.emulator_num = emulator_num;
    }

    public String getPredecessor_1_port_num() {
        return Predecessor_1_port_num;
    }

    public void setPredecessor_1_port_num(String predecessor_1_port_num) {
        Predecessor_1_port_num = predecessor_1_port_num;
    }

    public String getPredecessor_1_node_id() {
        return Predecessor_1_node_id;
    }

    public void setPredecessor_1_node_id(String predecessor_1_node_id) {
        Predecessor_1_node_id = predecessor_1_node_id;
    }

    public String getPredecessor_2_port_num() {
        return Predecessor_2_port_num;
    }

    public void setPredecessor_2_port_num(String predecessor_2_port_num) {
        Predecessor_2_port_num = predecessor_2_port_num;
    }

    public String getPredecessor_2_node_id() {
        return Predecessor_2_node_id;
    }

    public void setPredecessor_2_node_id(String predecessor_2_node_id) {
        Predecessor_2_node_id = predecessor_2_node_id;
    }

    public String getSuccessor_1_port_num() {
        return Successor_1_port_num;
    }

    public void setSuccessor_1_port_num(String successor_1_port_num) {
        Successor_1_port_num = successor_1_port_num;
    }

    public String getSuccessor_1_node_id() {
        return Successor_1_node_id;
    }

    public void setSuccessor_1_node_id(String successor_1_node_id) {
        Successor_1_node_id = successor_1_node_id;
    }

    public String getSuccessor_2_port_num() {
        return Successor_2_port_num;
    }

    public void setSuccessor_2_port_num(String successor_2_port_num) {
        Successor_2_port_num = successor_2_port_num;
    }

    public String getSuccessor_2_node_id() {
        return Successor_2_node_id;
    }

    public void setSuccessor_2_node_id(String successor_2_node_id) {
        Successor_2_node_id = successor_2_node_id;
    }

    public int compareTo(Node t) {
        return this.node_id.compareTo(t.getNode_id());
    }
}
