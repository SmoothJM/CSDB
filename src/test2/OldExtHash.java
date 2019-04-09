package test2;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class OldExtHash {

	RandomAccessFile buckets;
	RandomAccessFile directory;
	int bucketSize;
	int directoryBits; // indicates how many bits of the hash function are used by the directory
	// add instance variables as needed.

	private class Bucket {
		private int bucketBits; // the number of hash function bits used by this bucket
		private int count; // the number of keys are in the bucket
		private int keys[];
		private long rowAddrs[];
		// constructors and other method
		public int getBucketBits() {
			return bucketBits;
		}
		public void setBucketBits(int bucketBits) {
			this.bucketBits = bucketBits;
		}
		public int getCount() {
			return count;
		}
		public void setCount(int count) {
			this.count = count;
		}
		public int[] getKeys() {
			return keys;
		}
		public void setKeys(int[] keys) {
			this.keys = keys;
		}
		public long[] getRowAddrs() {
			return rowAddrs;
		}
		public void setRowAddrs(long[] rowAddrs) {
			this.rowAddrs = rowAddrs;
		}
		
		
	}

	public OldExtHash(String filename, int bsize) throws IOException {
		File bucketsFile = new File(filename+"buckets");
		File directoryFile = new File(filename+"dir");
		if(bucketsFile.exists()){
			bucketsFile.delete();
		}	
		if(directoryFile.exists()){
			directoryFile.delete();
		}
		buckets = new RandomAccessFile(bucketsFile, "rw");
		directory = new RandomAccessFile(directoryFile, "rw");									
		buckets.writeInt(bsize);
		// when I initial the first two bucket I dont't know their addr
		
		bucketSize = bsize;
		directoryBits = 1;	
		directory.writeInt(directoryBits);
		
		Bucket bucket = new Bucket();
		long[] addrs = new long[bsize];
		int[] keys = new int[bsize];
		bucket.setBucketBits(1);
		bucket.setCount(0);
		bucket.setKeys(keys);
		bucket.setRowAddrs(addrs);
		long tempBkAddr0 = buckets.getFilePointer();
		writeBucket(buckets, tempBkAddr0, bucket);
		long tempBkAddr1 = buckets.getFilePointer();
		writeBucket(buckets, tempBkAddr1, bucket);
		
		directory.writeLong(tempBkAddr0);
		directory.writeLong(tempBkAddr1);
		
		// bsize is the bucket size.
		// creates a new hash index
		// the filename is the name of the file that contains the table rows
		// the directory file should be named filename+��dir��
		// the bucket file should be named filename+��buckets��
		// if any of the files exists the should be deleted before new ones are
		// made
	}

	public OldExtHash(String filename) throws IOException {
		buckets = new RandomAccessFile(filename+"buckets", "rw");
		directory = new RandomAccessFile(filename+"dir", "rw");	
		bucketSize = buckets.readInt();
		directoryBits = directory.readInt();
		
		// open an exisgng hash index
		// the associated directory file is named filename+��dir��
		// the associated bucket file is named filename+��buckets��
		// both files should already exists when this method is used
	}
	public OldExtHash(){
		
	}
	public boolean insert(int key, long addr) throws IOException {
		boolean result = false;
		if(search(key) == 0){
			result = true;
			int hash = hash(key);
			long hashAddr = 4 + 8*hash;
			directory.seek(hashAddr);
			long bkAddr = directory.readLong();			
			Bucket bk = readBucket(buckets, bkAddr);
			if(bk.getCount() < bucketSize){
				int count = bk.getCount();
				int[] keys = new int[bucketSize];
				for(int i=0; i<count; i++){
					keys[i] = bk.getKeys()[i];
				}
				keys[count] = key;
				bk.setKeys(keys);
				long[] rowAddrs = new long[bucketSize];
				for(int i=0; i<count; i++){
					rowAddrs[i] = bk.getRowAddrs()[i];
				}
				rowAddrs[count] = addr;
				bk.setRowAddrs(rowAddrs);
				bk.setCount(bk.getCount()+1);
				writeBucket(buckets, bkAddr, bk);
			}else if(bk.getBucketBits()<directoryBits){//rehash
				//new an empty bucket
				long[] emptyAddr = new long[bucketSize];
				int[] emptyKeys = new int[bucketSize];
				Bucket bucket = new Bucket();
				bucket.setBucketBits(directoryBits);
				bucket.setCount(0);
				bucket.setKeys(emptyKeys);
				bucket.setRowAddrs(emptyAddr);
				long tempBkAddr0 = buckets.length();
				writeBucket(buckets, tempBkAddr0, bucket);
				//clear the origin bucket
				Bucket tempbk = readBucket(buckets, bkAddr);
				tempbk.setBucketBits(directoryBits);
				tempbk.setCount(0);
				tempbk.setKeys(emptyKeys);
				tempbk.setRowAddrs(emptyAddr);
				writeBucket(buckets, bkAddr, tempbk);
				//split the bucket
				
				int originalHash = (int) (key % Math.pow(2, directoryBits-1));
				long matchingAddr = 4 + 8 * getMatching(originalHash,directoryBits);
				directory.seek(matchingAddr);
				directory.writeLong(tempBkAddr0);
				
				//rehash all the values
				int tempKey = 0;
				long tempAddr = 0;				
				for(int i=0; i<bk.getCount(); i++){
					tempKey = bk.getKeys()[i];
					tempAddr = bk.getRowAddrs()[i];
					insert(tempKey, tempAddr);
				}
				insert(key, addr);				
			}else{//double directory			
				long length = directory.length() - 4;
				long halflength = (directory.length() - 4)/2 + 4;
				long[] emptyAddr = new long[bucketSize];
				int[] emptyKeys = new int[bucketSize];
				Bucket emptyBucket = new Bucket();
				emptyBucket.setBucketBits(directoryBits);
				emptyBucket.setCount(0);
				emptyBucket.setKeys(emptyKeys);
				emptyBucket.setRowAddrs(emptyAddr);
				for(int i= (int)halflength; i< length; i=i+8){
					directory.seek(i);
					long copyBkAddr = directory.readLong();		
					Bucket BitBucket = readBucket(buckets, copyBkAddr);
					if(BitBucket.getBucketBits() < directoryBits){
						long emptyBucketAddr = buckets.length();
						directory.seek(i);
						directory.writeLong(emptyBucketAddr);
						writeBucket(buckets, emptyBucketAddr, BitBucket);
					}
				}
				for(int i=4; i<length; i = i+8){
					directory.seek(i);
					long copyBkAddr = directory.readLong();					
					Bucket BitBucket = readBucket(buckets, copyBkAddr);
					if(BitBucket.getBucketBits() < directoryBits){
						BitBucket.setBucketBits(directoryBits);
						
						int tempKey = 0;
						long tempAddr = 0;				
						for(int j=0; j<BitBucket.getCount(); j++){
							tempKey = BitBucket.getKeys()[j];
							tempAddr = BitBucket.getRowAddrs()[j];
							insert(tempKey, tempAddr);
						}
						
					}
					writeBucket(buckets, copyBkAddr, BitBucket);
					directory.seek(i + length);
					directory.writeLong(copyBkAddr);
				
				}
				directoryBits = directoryBits + 1;
				directory.seek(0);
				directory.writeInt(directoryBits);
				insert(key, addr);			
			}
			return result;
		}
			return result;
		
		
		/*
		 * If key is not a duplicate add key to the hash index addr is the
		 * address of the row that contains the key return true if the key is
		 * added return false if the key is a duplicate
		 */
	}

	private int getMatching(int hash, int bits) {
		
		int halfDirectory = (int)Math.pow(2, directoryBits-1);
		int matchingHash;
		if(hash < halfDirectory){
			matchingHash = hash + halfDirectory;
		}else{
			matchingHash = hash - halfDirectory;
		}
		return matchingHash;
	}



	public long remove(int key) throws IOException {
			
		long addr = 0;
		int hash = hash(key);
		long hashAddr = 4 + 8*hash;
		directory.seek(hashAddr);
		long bkAddr = directory.readLong();
		Bucket bk = readBucket(buckets, bkAddr);
		int count = bk.getCount();
		int[] keys = bk.getKeys();
		long[] addrs = bk.getRowAddrs();
		//find matching bucket
		int matchingBkHash = getMatching(hash, directoryBits);
		long matchingBkHashAddr = 4 + 8 * matchingBkHash;
		directory.seek(matchingBkHashAddr);
		long matchingBkAddr = directory.readLong();
		Bucket matchingBk = readBucket(buckets, matchingBkAddr);
		int matchingCount = matchingBk.getCount();
		int[] matchingKeys = matchingBk.getKeys();
		long[] matchingAddrs = matchingBk.getRowAddrs();
				
		//begin to delete 
		for(int i=0; i<count; i++){
			if(keys[i] == key){//just delete
				
				for(int j=i; j< count-1; j++){
					keys[j] = keys[j+1];										
				}
				keys[count-1] = 0;
				addr = bk.getRowAddrs()[i];
				for(int j=i; j< count-1; j++){
					addrs[i] = addrs[i+1];									
				}
				addrs[count-1] = 0L;
				count--;
				bk.setCount(count);
				bk.setKeys(keys);
				bk.setRowAddrs(addrs);
				writeBucket(buckets, bkAddr, bk);
				//
				if(matchingBk.getCount() + bk.getCount() == bucketSize){//combine buckets
					if(hash < matchingBkHash){
						for(int j=count,k=0; j<bucketSize && k<matchingCount; j++,k++){
							keys[j] = matchingKeys[k];
							addrs[j] = matchingAddrs[k];
						}
						bk.setKeys(keys);
						bk.setRowAddrs(addrs);
						bk.setCount(matchingBk.getCount() + bk.getCount());
						
						bk.setBucketBits(directoryBits-1);
						writeBucket(buckets, bkAddr, bk);
						//change pointer
						directory.seek(matchingBkHashAddr);
						directory.writeLong(bkAddr);
						
					}else{
						for(int j=matchingCount,k=0; j<bucketSize && k<count; j++,k++){
							matchingKeys[j] = keys[k];
							matchingAddrs[j] = addrs[k];
						}
						matchingBk.setKeys(matchingKeys);
						matchingBk.setRowAddrs(matchingAddrs);
						matchingBk.setCount(matchingBk.getCount() + bk.getCount());
						matchingBk.setBucketBits(directoryBits-1);
						writeBucket(buckets, matchingBkAddr, matchingBk);
						directory.seek(hashAddr);
						directory.writeLong(matchingBkAddr);
					}					
				}
				if(isShrink()){//shrink directory size
					long length = (directory.length()-4)/2 + 4;
					directory.setLength(length);
					directoryBits = directoryBits - 1;
					directory.seek(0);
					directory.writeInt(directoryBits);
				
				}
				
				return addr;
			}
		}		
		
		writeBucket(buckets, bkAddr, bk);		
		return addr;
		/*
		 * If the key is in the hash index, remove the key and return the
		 * address of the row return 0 if the key is not found in the hash index
		 */
	}

	private boolean isShrink() throws IOException {
		boolean result = false;		
		long halfLength = (directory.length()-4)/2 + 4;
		long halfDrictory = (directory.length()-4)/2;
		for(int i=4; i< halfLength; i = i+8){
			directory.seek(i);
			long bkAddr1 = directory.readLong();
			directory.seek(i+halfDrictory);
			long bkAddr2 = directory.readLong();
			if(bkAddr1 == bkAddr2){
				result = true;			
			}else{
				result = false;
				return result;
			}
		}
		return result;
	}

	public long search(int k) throws IOException {
			
		long addr = 0;
		int hash = hash(k);	
		long hashAddr = 4 + 8*hash;
		directory.seek(hashAddr);
		long bkAddr = directory.readLong();
		Bucket bk = readBucket(buckets, bkAddr);
		for(int i=0; i<bucketSize; i++){
			if(bk.getKeys()[i] == k){
				addr = bk.getRowAddrs()[i];
				return addr;
			}
		}
		return addr;
		/*
		 * If the key is found return the address of the row with the key
		 * otherwise return 0
		 */
	}

	public int hash(int key) {
		//return the hash value
		//In general not a very good hash function but it will be good enough for this
		//homework
		return key % (1 << this.directoryBits); //calculates 2^directoryBits		
		}

	public void close() {
		try {
			directory.close();
			buckets.close();
		} catch (IOException e) {			
			e.printStackTrace();
		}
		// close the hash index. The tree should not be accessed aler close is
		// called
	}
	
	public Bucket readBucket(RandomAccessFile raf, long addr) throws IOException{
		Bucket bucket = new Bucket();
		raf.seek(addr);
		int x = raf.readInt();
		bucket.setBucketBits(x);
		int y = raf.readInt();
		bucket.setCount(y);
		int[] keys = new int[bucketSize];
		long[] addrs = new long[bucketSize];
		if(y == 0){
			bucket.setKeys(keys);
			bucket.setRowAddrs(addrs);
			return bucket;
		}else{
			for(int i=0; i<bucketSize; i++){
				keys[i] = raf.readInt();			
			}
			bucket.setKeys(keys);			
			for(int i=0; i<bucketSize; i++){
				addrs[i] = raf.readLong();
			}
			bucket.setRowAddrs(addrs);
		}
  		
		return bucket;		
	}

	public void writeBucket(RandomAccessFile raf, long addr, Bucket bucket) throws IOException{
		raf.seek(addr);
		raf.writeInt(bucket.getBucketBits());
		raf.writeInt(bucket.getCount());
		for(int i=0; i<bucketSize; i++){
			 raf.writeInt(bucket.keys[i]);
		}
		for(int i=0; i<bucketSize; i++){
			 raf.writeLong(bucket.rowAddrs[i]);
		}
	}
	

}
