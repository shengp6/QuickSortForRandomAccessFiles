/**
 * node class
 * a node has one data field and two pointers
 * Used for linked list
 * @param <E> Generic type of the data store in the node
 * @author Sheng Peng <shengp6> & Wenjia Song <wenjia7>
 * @version 09/26/2016
 */
public class LinkedListNode<E> {

    private LinkedListNode<E> next;
    private LinkedListNode<E> previous;
    private E data;


    /**
     * Creates a new node with the given data
     *
     * @param d
     *            the data to put inside the node
     */
    public LinkedListNode(E d) {
        data = d;
    }

    /**
     * Sets the node after this node
     *
     * @param n
     *            the node after this one
     */
    public void setNext(LinkedListNode<E> n) {
        next = n;
    }

    /**
     * Sets the node before this one
     *
     * @param n
     *            the node before this one
     */
    public void setPrevious(LinkedListNode<E> n) {
        previous = n;
    }

    /**
     * Gets the next node
     *
     * @return the next node
     */
    public LinkedListNode<E> next() {
        return next;
    }

    /**
     * Gets the node before this one
     *
     * @return the node before this one
     */
    public LinkedListNode<E> previous() {
        return previous;
    }

    /**
     * Gets the data in the node
     *
     * @return the data in the node
     */
    public E getData() {
        return data;
    }
    /**
     * set the data in the node
     *
     * @param data the new data to put inside the node
     */
    public void setData(E data) {
        this.data = data;
    }


}
