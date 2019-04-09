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

		public Bucket() {

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
		long firstBucketAddr = 0;
		long secondBucketAddr = 0;
		int[] keysInDB = new int[bsize];
		long[] tableAddrs = new long[bsize];
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
		// initial dir
		firstBucketAddr = buckets.getFilePointer();
		secondBucketAddr = 4 + 4 + 4 + 4 * bsize + 8 * bsize;
		directory.writeLong(firstBucketAddr);
		directory.writeLong(secondBucketAddr);
		// initial bucket
		bucket.setBucketBits(1);
		bucket.setCount(0);
		bucket.setKeys(keysInDB);
		bucket.setRowAddrs(tableAddrs);
		writeBucket(buckets.length(), bucket);
		writeBucket(buckets.length(), bucket);

	}

	public void writeBucket(long addr, Bucket bucket) throws IOException {
		buckets.seek(addr);
		buckets.writeInt(bucket.getBucketBits());
		buckets.writeInt(bucket.getCount());
		for (int i = 0; i < this.bucketSize; i++) {
			buckets.writeInt(bucket.keys[i]);
		}
		for (int i = 0; i < this.bucketSize; i++) {
			buckets.writeLong(bucket.rowAddrs[i]);
		}
	}

	public Bucket readBucket(long addr) throws IOException {
		int nb = 0;
		int nk = 0;
		int[] keys = new int[this.bucketSize];
		long[] addrs = new long[this.bucketSize];
		Bucket bucket = new Bucket();
		buckets.seek(addr);
		nb = buckets.readInt();
		nk = buckets.readInt();
		bucket.setBucketBits(nb);
		bucket.setCount(nk);
		if (nk == 0) {
			bucket.setKeys(keys);
			bucket.setRowAddrs(addrs);
			return bucket;
		} else {
			for (int i = 0; i < this.bucketSize; i++) {
				keys[i] = buckets.readInt();
			}
			bucket.setKeys(keys);
			for (int i = 0; i < this.bucketSize; i++) {
				addrs[i] = buckets.readLong();
			}
			bucket.setRowAddrs(addrs);
		}

		return bucket;
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

	public int findnKeys(int key) throws IOException {
		int nKeys = 0;
		long addr = searchBucketAddrInDir(key);
		buckets.seek(addr + 4);
		nKeys = buckets.readInt();
		return nKeys;
	}

	public void doubleAll() throws IOException {
		// int nKeys = 0;
		int length = 0;
		int loopNum = 0;
		length = calcBucketEachLength();
		// nKeys = findnKeys(key);
		// if (nKeys == this.bucketSize) {
		// double dir
		this.directoryBIts++;
		directory.seek(0);
		directory.writeInt(this.directoryBIts);
		for (int i = 0; i < (Math.pow(2, this.directoryBIts)); i++) {
			directory.writeLong(length * i + 4);
		}
		// double bucket
		Bucket bucket = new Bucket();
		bucket.setBucketBits(1);
		bucket.setCount(0);
		bucket.setKeys(new int[this.bucketSize]);
		bucket.setRowAddrs(new long[this.bucketSize]);
		loopNum = (int) calcLoop(1);
		for (int i = 0; i < loopNum; i++) {
			writeBucket(buckets.length(), bucket);
		}

		// return true;
		// }
		// return false;
	}

	public void deDoubleAll() throws IOException {
		long length = 0;
		length = ((directory.length() - 4) / 2) + 4;
		directory.setLength(length);
	}

	public boolean insert(int key, long addr) throws IOException {
		/*
		 * If key is not duplicated then add key to the hash index. addr is the
		 * address of the row that contains the key. return true if the key is
		 * added, return false if the key is a duplicate
		 */
		long keyAddr = 0;
		long nk = 0;
		long[] emptyAddr = new long[bucketSize];
		int[] emptyKeys = new int[bucketSize];
		
		keyAddr = searchBucketAddrInDir(key);
		buckets.seek(keyAddr);
		Bucket bucket = readBucket(keyAddr);
		nk = bucket.getCount();
		if (nk < this.bucketSize) {// insert directly
			int[] keys = new int[this.bucketSize];
			long[] tableAddrs = new long[this.bucketSize];
			for (int i = 0; i < nk; i++) {
				keys[i] = bucket.getKeys()[i];
			}
			keys[(int) nk] = key;
			for (int i = 0; i < nk; i++) {
				tableAddrs[i] = bucket.getRowAddrs()[i];
			}
			tableAddrs[(int) nk] = addr;
			bucket.setKeys(keys);
			bucket.setRowAddrs(tableAddrs);
			bucket.setCount((int) nk + 1);
			writeBucket(keyAddr, bucket);
		} else if (bucket.getBucketBits() < this.directoryBIts) {
			
		} else {
			doubleAll();
			Bucket emptyBucket = new Bucket();
			emptyBucket.setBucketBits(directoryBIts);
			emptyBucket.setCount(0);
			emptyBucket.setKeys(emptyKeys);
			emptyBucket.setRowAddrs(emptyAddr);
			writeBucket(keyAddr, emptyBucket);
			int oldKey = 0;
			long oldAddr = 0;				
			for(int i=0; i<bucket.getCount(); i++){
				oldKey = bucket.getKeys()[i];
				oldAddr = bucket.getRowAddrs()[i];
				insert(oldKey, oldAddr);
			}
			
			long newKeyAddr=searchBucketAddrInDir(key);
			insert(key, addr);	
			buckets.seek(newKeyAddr);
			buckets.writeInt(this.directoryBIts);
		}

		return true;

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
		// long loopNum=0;
		long bucketAddr = 0;
		int[] keyInFile;
		long[] keyAddr;
		// int keyNum = 0;
		boolean flag = false;
		int record = 0;
		// loopNum=calcLoop(1);
		// for(int i=0;i<loopNum;i++){
		bucketAddr = searchBucketAddrInDir(k);
		System.out.println("The key should insert into " + bucketAddr);
		buckets.seek(bucketAddr);
		buckets.readInt();
		// keyNum = buckets.readInt();
		buckets.readInt();
		keyInFile = new int[this.bucketSize];
		keyAddr = new long[this.bucketSize];
		for (int j = 0; j < this.bucketSize; j++) {
			keyInFile[j] = buckets.readInt();
			if (k == keyInFile[j]) {
				flag = true;
				record = j;
			}
		}
		for (int j = 0; j < this.bucketSize; j++) {
			keyAddr[j] = buckets.readLong();
		}
		if (flag) {
			return keyAddr[record];
		}
		// }
		return 0;
	}

	public int hash(int key) {
		// return the hash value
		int hashValue = 0;
		int sum = 0;
		String result = "";
		for (int i = key; i >= 1; i = i / 2) {
			if (i % 2 == 0) {
				sum = 0;
			} else {
				sum = 1;
			}
			result = sum + result;
		}
		result = result.substring(result.length() - this.directoryBIts, result.length());
		hashValue = Integer.parseInt(result, 2);
		// hashValue = key % (1 << this.directoryBIts);
		return hashValue;
	}

	public long searchBucketAddrInDir(int key) throws IOException {
		long bucketAddr = 0;
		int hashValue = 0;
		long dirAddr = 0;
		hashValue = hash(key);
		dirAddr = hashValue * 8 + 4;
		directory.seek(dirAddr);
		bucketAddr = directory.readLong();
		return bucketAddr;
	}

	public int calcBucketEachLength() {
		int length = 0;
		length = 4 + 4 + 4 * this.bucketSize + 8 * this.bucketSize;
		return length;
	}

	public long calcLoop(int i) throws IOException {
		long loopNum = 0;
		long fileLength = 0;
		int length = 0;
		length = calcBucketEachLength();
		if (i == 1) {
			fileLength = buckets.length();
			loopNum = (fileLength - 4) / length;
		} else {
			fileLength = directory.length();
			loopNum = (fileLength - 4) / 8;
		}
		return loopNum;
	}

	public void bucketDisplay() throws IOException {
		int bS = 0;
		long loopNum = calcLoop(1);
		int nb = 0;
		int nk = 0;
		int[] k;
		long[] a;
		buckets.seek(0);
		bS = buckets.readInt();
		System.out.println("Bucket Size:\t" + bS);
		System.out.println("nBits\t" + "nKeys\t" + "Keys\t" + "Addr");
		for (int i = 0; i < loopNum; i++) {
			nb = buckets.readInt();
			nk = buckets.readInt();
			System.out.print(nb + "\t");
			System.out.print(nk + "\t");
			k = new int[this.bucketSize];
			a = new long[this.bucketSize];
			for (int j = 0; j < this.bucketSize; j++) {
				k[j] = buckets.readInt();
				System.out.print(k[j] + " ");
			}
			System.out.print("\t");
			for (int j = 0; j < this.bucketSize; j++) {
				a[j] = buckets.readLong();
				System.out.print(a[j] + " ");
			}
			System.out.println();
		}
	}

	public void dirDisplay() throws IOException {
		int hB = 0;
		long loopNum = 0;
		long addrOfBucket = 0;
		loopNum = calcLoop(2);
		directory.seek(0);
		hB = directory.readInt();
		System.out.println("Hash Bits:\t" + hB);
		System.out.println("Bucket Address:");
		for (int i = 0; i < loopNum; i++) {
			addrOfBucket = directory.readLong();
			System.out.println(addrOfBucket);
		}

	}

	public void close() throws IOException {
		// close the hash index. The tree should not be accessed after close is
		// called
		directory.close();
		buckets.close();
	}

}
