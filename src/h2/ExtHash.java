package h2;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;


public class ExtHash {
	RandomAccessFile buckets;
	RandomAccessFile directory;
	int bucketSize;
	int directoryBIts; // indicates how many bits of the hash function are used
						// by the directory
	// add instance variables as needed.

	private class Bucket {
		private int bucketBits; // the number of hash function bits used by this
								// bucket
		private int count; // the number of keys are in the bucket
		private int keys[];
		private long rowAddrs[];
		// overflow bucket?
		// constructors and other method
		public Bucket(int bucketBits, int count, int[] keys, long[] rowAddrs) {
			this.bucketBits = bucketBits;
			this.count = count;
			this.keys = keys;
			this.rowAddrs = rowAddrs;
		}
		
		public Bucket(){
			
		}
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
		public void writeBucket(){
			
		}
		
		
	}

	public ExtHash(String filename, int bsize) throws IOException {
		/*
		 * bsize is the bucket size. Creates a new hash index the filename is
		 * the name of the file that contains the table rows The directory file
		 * should be named filename+”dir” The bucket file should be named
		 * filename+”buckets” If any of the files exists the should be deleted
		 * before new ones are made
		 */
		File fileBucket = new File(filename + "_buckets");
		File fileDir = new File(filename + "_dir");
		Bucket bucket = new Bucket();
		long firstBucketAddr =0;
		long secondBucketAddr =0;
		int[] keysInDB =new int[bsize];
		long[] tableAddrs =new long[bsize];
		if (fileBucket.exists()) {
			fileBucket.delete();
			System.out.println("deleted: " + filename + "_dir");
		} else {
			System.out.println("created: " + filename + "_dir");
		}
		if (fileDir.exists()) {
			fileDir.delete();
			System.out.println("deleted: " + filename + "_buckets");
		} else {
			System.out.println("created: " + filename + "_buckets");
		}
		buckets = new RandomAccessFile(filename + "_buckets", "rw");
		directory = new RandomAccessFile(filename + "_dir", "rw");
		this.bucketSize = bsize;
		this.directoryBIts = 1;
		buckets.seek(0);
		directory.seek(0);
		buckets.writeInt(this.bucketSize);
		directory.writeInt(this.directoryBIts);
		
		bucket.setBucketBits(1);
		bucket.setCount(0);
		bucket.setKeys(keysInDB);
		bucket.setRowAddrs(tableAddrs);
		firstBucketAddr = buckets.getFilePointer();
		writeBucket(buckets, firstBucketAddr, bucket);
		secondBucketAddr = buckets.getFilePointer();
		writeBucket(buckets, secondBucketAddr, bucket);
		
		directory.writeLong(firstBucketAddr);
		directory.writeLong(secondBucketAddr);
		
	}

	public void writeBucket(RandomAccessFile raf, long addr, Bucket bucket) throws IOException{
		raf.seek(addr);
		raf.writeInt(bucket.getBucketBits());
		raf.writeInt(bucket.getCount());
		for(int i=0; i<this.bucketSize; i++){
			 raf.writeInt(bucket.keys[i]);
		}
		for(int i=0; i<this.bucketSize; i++){
			 raf.writeLong(bucket.rowAddrs[i]);
		}
	}
	public ExtHash(String filename) throws IOException {
		// open an existing hash index
		// the associated directory file is named filename+”dir”
		// the associated bucket file is named filename+”buckets”
		// both files should already exists when this method is used
		buckets = new RandomAccessFile(filename + "_buckets", "rw");
		directory = new RandomAccessFile(filename + "_dir", "rw");
		buckets.seek(0);
		directory.seek(0);
		this.bucketSize = buckets.readInt();
		this.directoryBIts = directory.readInt();
	}

	public boolean insert(int key, long addr) throws IOException {
		/*
		 * If key is not duplicated then add key to the hash index. addr is the
		 * address of the row that contains the key. return true if the key is
		 * added, return false if the key is a duplicate
		 */
		if(search(key)==0){
			
			return true;
		}else{
			return false;
		}
		
		
		
		
	}

	public long remove(int key) {

		/*
		 * If the key is in the hash index, remove the key and return the
		 * address of the row. return 0 if the key is not found in the hash
		 * index
		 */
		long addr = 0;
		return addr;
	}

	public long search(int k) throws IOException {
		/*
		 * If the key is found return the address of the row with the key
		 * otherwise return 0
		 */
		long loopNum=0;
		int[] keyInFile;
		long[] keyAddr;
		int keyNum=0;
		boolean flag=false;
		int record=0;
		loopNum=calcLoop(1);
		for(int i=0;i<loopNum;i++){
			buckets.readInt();
			keyNum = buckets.readInt();
			keyInFile=new int[keyNum];
			keyAddr=new long[keyNum];
			for (int j = 0; j < keyNum; j++) { 
				keyInFile[j] = buckets.readInt();
				if(k==keyInFile[j]){
					flag=true;
					record=j;
				}
			}
			for(int j=0;j<keyNum;j++){
				keyAddr[j]=buckets.readLong();
			}
			if(flag){
				return keyAddr[record];
			}
		}
		return 0;
	}

	public int hash(int key) {
		int hashValue = 0;
		// return the hash value
		return hashValue;
	}

	public long calcLoop(int i) throws IOException {
		long loopNum = 0;
		long fileLength = 0;
		if (i == 1) {
			fileLength = buckets.length();
			loopNum = (fileLength-4)/32;
		} else {
			fileLength = directory.length();
			loopNum = (fileLength-4)/8;
		}

		return loopNum;
	}

	public void bucketDisplay() throws IOException {
		int bS = 0;
		long loopNum=calcLoop(1);
		int nb=0;
		int nk=0;
		int[] k;
		long[] a;
		buckets.seek(0);
		bS = buckets.readInt();
		System.out.println("Bucket Size:\t" + bS);
		System.out.println("nBits\t"+"nKeys\t"+"Keys\t"+"Addr");
		for(int i=0;i<loopNum;i++){
			nb=buckets.readInt();
			nk=buckets.readInt();
			System.out.print(nb+"\t");
			System.out.print(nk+"\t");
			k=new int[nk];
			a=new long[nk];
			for (int j = 0; j < nk; j++) {
				k[j]=buckets.readInt();
				System.out.print(k[j]+" ");
			}
			System.out.print("\t");
			for (int j = 0; j < nk; j++) {
				a[j]=buckets.readLong();
				System.out.print(a[j]+" ");
			}
			System.out.println();
		}
	}

	public void dirDisplay() throws IOException {
		int hB = 0;
		directory.seek(0);
		hB = directory.readInt();
		System.out.println("Hash Bits:\t" + hB);
	}

	public void close() throws IOException {
		// close the hash index. The tree should not be accessed after close is
		// called
		directory.close();
		buckets.close();
	}

}
