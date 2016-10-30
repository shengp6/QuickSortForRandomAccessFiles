import java.io.IOException;
import java.io.RandomAccessFile;
/**
* The buffer pool, read & write form random access file
* All the buffers are in a buffer list
* Client can get bytes and insert bytes from buffer pool
* Use least recent used to manage the buffers
* The size of a buffer is 4096
* @author Sheng Peng <shengp6> & Wenjia Song <wenjia7>
* @version 10/24/2016
*/
public class BufferPool {
    private DLList<Buffer> bufferList;
    private int numOfBuffers;
    private RandomAccessFile input;
    //The fixed size of each block (buffer)
    private final static int BLOCK_SIZE = 4096;
    private int numOfDiskReads;
    private int numOfDiskWrites;
    private int numOfCacheHits;
    /**
     * Constructor
     * @param file The source file
     * @param numOfBuff The number of buffers in the pool
     */
    public BufferPool(RandomAccessFile file, int numOfBuff) {
        input = file;
        numOfBuffers = numOfBuff;
        numOfDiskReads = 0;
        numOfDiskWrites = 0;
        numOfCacheHits = 0;
        bufferList = new DLList<Buffer>();
        //add all the buffers
        //buffers are empty right now
        for (int i = 0; i < numOfBuff; i++) {
            bufferList.add(new Buffer(BLOCK_SIZE));
        }
    }
    /**
     * Insert bytes into a buffer
     * If the target position is not in the buffer pool,
     * it will read a new block which contains the target
     * @param space The byte array which client want to insert
     * @param size The size of byte array
     * @param pos The target position
     * @throws IOException Exception when read file
     */
    public void insert(byte[] space, int size, int pos) throws IOException {
        int blockIndex = pos / BLOCK_SIZE;
        int posInBuf = pos % BLOCK_SIZE;
        int index = dataInPool(blockIndex);
        Buffer buf = null;
        //If the block is in the pool
        if (index != -1) {
            buf = bufferList.get(index);
            buf.insert(space, size, posInBuf);
            //move the most recent used buffer to the
            //first of the list
            bufferList.moveToFirst(index);
        }
        //If the block is not in the pool
        else {
            buf = addBlockToBuffer(blockIndex);
            buf.insert(space, size, posInBuf);
        }
        //buffer is changed, set it dirty
        buf.setDirty();
    }
    /**
     * Help method
     * Read a new block into the buffer pool
     * If not all the buffers are occupied, just put the data
     * into the first not occupied buffer
     * If all the buffers are occupied, find the least recent used
     * buffer. Check if any data was changed, write it back to the
     * file. Then, read the data into the buffer to replace the original
     * data array
     * @param blockIndex The index of block
     * @return The buffer contains the data array we read from the file
     * @throws IOException Exception when read file
     */
    private Buffer addBlockToBuffer(int blockIndex) throws IOException {
        Buffer buf;
        //If there is a buffer not occupied
        if (numOfDiskReads < numOfBuffers) {
            //Find the first not occupied buffer
            buf = bufferList.get(numOfDiskReads);
            //set the point to the start of this block
            input.seek(blockIndex * BLOCK_SIZE);
            //read one block of data
            input.read(buf.data);
            //set the block index
            buf.setIndexOfBlock(blockIndex);
            //move the node to the first position
            bufferList.moveToFirst(numOfDiskReads);

        }
        //If all the buffers are occupied
        else {
            //get the last buffer
            buf = bufferList.getLastEntry();
            if (buf.isDirty()) {
                //get the block index of this buffer
                int removedIndex = buf.getIndexOfBlock();
                //find the correct position to write
                input.seek(removedIndex * BLOCK_SIZE);
                //write the data of last buffer to the disk
                input.write(buf.data);
                numOfDiskWrites++;
                buf.resetDirty();
            }
            //find the correct position to read
            input.seek(blockIndex * BLOCK_SIZE);
            //read one block of data
            input.read(buf.data);
            //set the block index
            buf.setIndexOfBlock(blockIndex);
            //move the most recent used buffer to the first position
            bufferList.moveToFirst(bufferList.size() - 1);
        }
        numOfDiskReads++;
        return buf;
    }
    /**
     * Get bytes from buffer pool
     * If the target data is not in the buffer pool, read
     * a new block which contains the target data.
     * @param space Copy the target byte array to this array
     * @param size The size of required data
     * @param pos The position of target data
     * @throws IOException Exception when read file
     */
    public void getBytes(byte[] space, int size, int pos) throws IOException {
        int blockIndex = pos / BLOCK_SIZE;
        int posInBuf = pos % BLOCK_SIZE;
        int index = dataInPool(blockIndex);
        Buffer buf;
        //If the block is in the pool
        if (index != -1) {
            buf = bufferList.get(index);
            buf.getBytes(space, size, posInBuf);
            bufferList.moveToFirst(index);
            numOfCacheHits++;
        }
        //If the block is not in the pool
        else {
            buf = addBlockToBuffer(blockIndex);
            buf.getBytes(space, size, posInBuf);
        }
    }
    /**
     * Check if the block is in the buffer pool
     * @param blockIndex The index of block
     * @return The index of buffer in the list
     *          -1 if not found
     */
    private int dataInPool(int blockIndex) {
        for (int i = 0; i < bufferList.size(); i++) {
            Buffer buf = bufferList.get(i);
            if (buf.getIndexOfBlock() == blockIndex) {
                return i;
            }
        }
        return -1;
    }
    /**
     * Remove all the buffers in the buffer pool
     * And write back to the file if the data was changed
     * @throws IOException Exceptions when read file
     */
    public void flush() throws IOException {
        int size = bufferList.size();
        for (int i = 0; i < size; i++) {
            Buffer buf = bufferList.get(0);
            if (buf.isDirty()) {
                int blockIndex = buf.getIndexOfBlock();
                input.seek(blockIndex * BLOCK_SIZE);
                input.write(buf.data);
                numOfDiskWrites++;
            }
            bufferList.remove(0);
        }
    }
    /**
     * Return how many times the buffer reads from file
     * @return The number of reads
     */
    public int getNumOfReads() {
        return numOfDiskReads;
    }
    /**
     * Return how many times the buffer write to file
     * @return The number of writes
     */
    public int getNumOfWrites() {
        return numOfDiskWrites;
    }
    /**
     * Return the cache hits
     * How many times client get data from buffer pool
     * instead of from file
     * @return number of cache hits
     */
    public int getCacheHits() {
        return numOfCacheHits;
    }
    /**
     * Stores a block of bytes *
     * @author Sheng Peng <shengp6> & Wenjia Song <wenjia7>
     * @version 10/24/2016
     */
    private class Buffer {
        private byte[] data;
        private int indexOfBlock;
        private boolean dirty;
        /**
         * Constructor
         * @param size The size of data array
         */
        private Buffer(int size) {
            data = new byte[size];
            indexOfBlock = -1;
            dirty = false;
        }

        /**
         * Indicate which block the buffer holds
         * @return The index of block
         */
        private int getIndexOfBlock() {
            return indexOfBlock;
        }
        /**
         * Set the index of block
         * @param index The index of block
         */
        private void setIndexOfBlock(int index) {
            indexOfBlock = index;
        }
        /**
         * Get bytes from buffer
         * @param space copy the target byte array to space
         * @param size the size of the array to copy
         * @param pos the start position of the array to copy
         */
        private void getBytes(byte[] space, int size, int pos) {
            System.arraycopy(data, pos, space, 0, size);
        }
        /**
         * insert bytes to buffer
         * @param space copy the target byte array to space
         * @param size the size of the array to copy
         * @param pos the start position of the array to copy
         */
        private void insert(byte[] space, int size, int pos) {
            System.arraycopy(space, 0, data, pos, size);
        }
        /**
         * set dirty to true, used when the data in the buffer
         * has changed
         */
        private void setDirty() {
            dirty = true;
        }
        /**
         * reset dirty to false
         */
        private void resetDirty() {
            dirty = false;
        }
        /**
         * check if the data in the buffer is dirty
         * @return true if the data in the buffer has changed,
         *          false otherwise.
         */
        private boolean isDirty() {
            return dirty;
        }
//        //
//        public int compareTo(Buffer buf) {
//            return this.indexOfBlock - buf.indexOfBlock;
//        }

    }

}
