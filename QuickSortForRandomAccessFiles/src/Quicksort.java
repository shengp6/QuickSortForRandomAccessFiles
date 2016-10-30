import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;

/**
 * This program is to sort data files in the disk
 * Read a file from disk, and sort it
 * Using buffer pool to reduce the times of read and write
 * Using 3 way partition quick sort to sort the data
 */

/**
 * The class containing the main method.
 *
 * @author Sheng Peng<shengp6>, Wenjia Song<wenjia7>
 * @version 10/22/2016
 */

// On my honor:
//
// - I have not used source code obtained from another student,
// or any other unauthorized source, either modified or
// unmodified.
//
// - All source code and documentation used in my program is
// either my original work, or was derived by me from the
// source code published in the textbook for this course.
//
// - I have not discussed coding details about this project with
// anyone other than my partner (in the case of a joint
// submission), instructor, ACM/UPE tutors or the TAs assigned
// to this course. I understand that I may discuss the concepts
// of this program with other students, and that another student
// may help me debug my program so long as neither of us writes
// anything during the discussion or modifies any computer file
// during the discussion. I have violated neither the spirit nor
// letter of this restriction.

public class Quicksort {

    /**
     * @param args
     *      Command line parameters.
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        // This is the main file for the program.
        RandomAccessFile input = new RandomAccessFile(args[0], "rw");
        int size = (int) input.length();
        BufferPool pool = new BufferPool(input, Integer.parseInt(args[1]));
        long before = System.currentTimeMillis();
        Sorter sort = new Sorter(pool);
        sort.sort(0, size - 4);
        pool.flush();
        long after = System.currentTimeMillis();
        long timeUsage = after - before;
        input.close();
        PrintWriter writer = null;
        File file = new File(args[2]);
        if (file.exists()) {
            writer = new PrintWriter(new FileOutputStream(
                    new File(args[2]), true));
        }
        else {
            writer = new PrintWriter(args[2]);
        }
        writer.append("Sort on " + args[0] + "\n");
        writer.append("Cache Hits: " + pool.getCacheHits() + "\n");
        writer.append("Disk Reads: " + pool.getNumOfReads() + "\n");
        writer.append("Disk Writes: " + pool.getNumOfWrites() + "\n");
        writer.append("Time is " + timeUsage + "\n\n");
        writer.close();
    }
}
