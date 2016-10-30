import student.TestCase;


/**
 * Test all the functions in the project
 * @author Sheng Peng <shengp6> & Wenjia Song <wenjia7>
 * @version 10/24/2016
 */

public class QuicksortTest extends TestCase {
    /**
     * Sets up the tests that follow. In general, used for initialization.
     */
    @Override
    public void setUp() {
        // Nothing Here
    }

    /**
     * Test sort binary file
     * @throws Exception Exceptions of I/O
     */
    public void testCase1() throws Exception {
        FileGenerator fg = new FileGenerator();
        String[] arr = {"-b", "input1.txt", "200"};
        fg.generateFile(arr);
        CheckFile check = new CheckFile();
        String[] args = {"input1.txt", "10", "output.txt"};
        Quicksort.main(args);
        assertTrue(check.checkFile("input1.txt"));
    }
    /**
     * Test sort ASCII file
     * @throws Exception
     */
    public void testCase2() throws Exception {
        FileGenerator fg = new FileGenerator();
        String[] arr = {"-a", "input2.txt", "200"};
        fg.generateFile(arr);
        CheckFile check = new CheckFile();
        String[] args = {"input2.txt", "10", "output.txt"};
        Quicksort.main(args);
        assertTrue(check.checkFile("input2.txt"));
    }
    /**
     * Test not sort
     * @throws Exception
     */
    public void testCase3() throws Exception {
        FileGenerator fg = new FileGenerator();
        String[] arr = {"-a", "input3.txt", "10"};
        fg.generateFile(arr);
        CheckFile check = new CheckFile();
        assertFalse(check.checkFile("input3.txt"));
    }
    /**
     * Test not generate any data in file
     * Get code coverage from web-cat
     * @throws Exception
     */
    public void testCase4() throws Exception {
        FileGenerator fg = new FileGenerator();
        String[] arr = {"--", "input.txt", "100"};
        fg.generateFile(arr);
        CheckFile check = new CheckFile();
        Exception exception = null;
        try {
            check.checkFile("input.txt");
        }
        catch (Exception e) {
            exception = e;
        }
        assertNotNull(exception);
        //To get web-cat test code coverage
        new Quicksort();
    }
}
