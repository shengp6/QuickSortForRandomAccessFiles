import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * Sort class
 * Use 3 way partition quick sort
 * retrive data from buffer pool
 * @author Sheng Peng <shengp6> & Wenjia Song <wenjia7>
 * @version 10/24/2016
 */
public class Sorter {

    static private BufferPool pool;

    /**
     * Create a new Sorter
     * @param buffpool the buffer pool to sort
     */
    public Sorter(BufferPool buffpool) {
        pool = buffpool;
    }

    /**
     * Sort the data
     * 3-way partition quick sort
     * @param left start position of the first record in the
     *               array/subarray to sort
     * @param right start position of the last record in the
     *               array/subarray to sort
     * @throws IOException
     */
    public void sort(int left, int right) throws IOException {
        if (right - left < 120) {
            insertion(left, right);
            return;
        }
        int low = left;
        int high = right;
        int pivotValue = getKey(low);
        int i = low;
        while (i <= high) {
            int vi = getKey(i); // value of i
            if (vi < pivotValue) {
                swap(low, i);
                low += 4;
                i  += 4;
            }
            else if (vi > pivotValue) {
                swap(high, i);
                high -= 4;
            }
            else {
                i += 4;
            }
        }
        sort(left, low - 4);
        sort(high + 4, right);
    }
//        //change it to 400 when optimize
//        if (right - left <= 400) {
//            insertion(left, right);
//            return;
//        }
//        int pivot = ((left + right) / 8) * 4;
//        swap(right, pivot);
//        pivot = right;
//        right -= 4;
//        short pivotValue = getKey(pivot);
//        int pos = partition(left, right, pivotValue);
//        swap(pivot, pos);
//        sort(left, pos - 4);
//        sort(pos + 4, right + 4);
//    }
//
//    /**
//     * parition method, help quicksort
//     * @param left start position of the first record in the
//     *                  array/subarray to sort
//     * @param right start position of the last record in the
//     *                  array/subarray to sort
//     * @param pivotVal the value of the pivot
//     * @return the position where pivot should go
//     * @throws IOException
//     */
//    private int partition(int left, int right, short pivotVal)
//            throws IOException {
//        while (left <= right) {
//            while (getKey(left) < pivotVal) {
//                left += 4;
//            }
//            while (right >= left &&
//                    getKey(right) >= pivotVal) {
//                right -= 4;
//            }
//            if (right > left) {
//                swap(left, right);
//            }
//        }
//        return left;
//    }

    /**
     * insertion sort, used when the amount of data to sort is
     * small
     * @param left start position of the first record in the
     *               array/subarray to sort
     * @param right start position of the last record in the
     *               array/subarray to sort
     * @throws IOException
     */
    private void insertion(int left, int right) throws IOException {
        for (int i = 1; i < (right - left) / 4 + 1; i++) {
            for (int j = i; j > 0; j--) {
                if (getKey(left + j * 4) < getKey(left + (j - 1) * 4)) {
                    swap(left + j * 4, left + (j - 1) * 4);
                }
                else {
                    break;
                }
            }
        }
    }

    /**
     * swap two record in buffer pool
     * @param first start position of the first data
     * @param second start position of the second data
     * @throws IOException
     */
    private void swap(int first, int second) throws IOException {
        byte[] temp = new byte[4];
        pool.getBytes(temp, 4, first);
        byte[] temp2 = new byte[4];
        pool.getBytes(temp2, 4, second);
        pool.insert(temp, 4, second);
        pool.insert(temp2, 4, first);
    }

    /**
     * get the key value of a record
     * @param pos the start position of the record
     * @return the key value of the record
     * @throws IOException
     */
    public short getKey(int pos) throws IOException {
        byte[] bytes = new byte[4];
        pool.getBytes(bytes, 4, pos);
        byte[] temp = new byte[2];
        temp[0] = bytes[0];
        temp[1] = bytes[1];
        ByteBuffer wrapped = ByteBuffer.wrap(temp);
        short key = wrapped.getShort();
        return key;
    }

}
