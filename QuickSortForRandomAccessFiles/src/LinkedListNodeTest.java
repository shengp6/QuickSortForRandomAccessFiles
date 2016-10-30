import student.TestCase;

/**
 * test node class 
 * @author Sheng Peng <shengp6> & Wenjia Song <wenjia7>
 * @version 09/26/2016
 */
public class LinkedListNodeTest extends TestCase {

    /**
     * set up the test
     */
    public void setUp() {
        // nothing here...
    }
    
    /**
     * test every method in node class
     */
    public void testEverything() {
        LinkedListNode<String> zero = new LinkedListNode<>("zero");
        LinkedListNode<String> one = new LinkedListNode<>("one");
        LinkedListNode<String> two = new LinkedListNode<>("two");
        zero.setNext(one);
        assertEquals("one", zero.next().getData());
        zero.setPrevious(two);
        assertEquals("two", zero.previous().getData());
        zero.setData("000");
        assertEquals("000", zero.getData());
    }
}
