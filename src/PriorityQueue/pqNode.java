package PriorityQueue;

public class pqNode<T extends Comparable> {
    public int sno;
    public T value;

    public pqNode(int sno, T value) {
        this.sno = sno;
        this.value = value;
    }

    int compareTo(pqNode<T> element) {
        return (this.value.compareTo(element.value) != 0) ? this.value.compareTo(element.value)
                : element.sno - this.sno;
    }
}