package h2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;

public class DBTable {
	RandomAccessFile rows; // the file that stores the rows in the table
	long free; // head of the free list space for rows
	int numOtherFields;
	int otherFieldLengths[];
	ExtHash extHash;
	// add other instance variables as needed

	private class Row {
		private int keyField;
		private char otherFields[][];

		/*
		 * Each row consists of unique key and one or more character array
		 * fields. Each character array field is a fixed length field (for
		 * example 10 characters). Each field can have a different length.
		 * Fields are padded with null characters so a field with a length of of
		 * x characters always uses space for x characters.
		 */
		// Constructors and other Row methods
		public Row(int keyField, char[][] otherFields) throws IOException {
			this.keyField = keyField;
			this.otherFields = otherFields;
		}

		/**
		 * This method is to write the row into the DBTable file.
		 * 
		 * @param addr:
		 *            The address of the new row inserted.
		 * @throws IOException
		 */
		public void writeRow(long addr) throws IOException {
			rows.seek(addr);
			rows.writeInt(keyField);
			for (int i = 0; i < numOtherFields; i++) {
				for (int j = 0; j < otherFieldLengths[i]; j++) {
					if (j < otherFields[i].length) {
						rows.writeChar(otherFields[i][j]);
					} else {
						rows.writeChar(0);
					}
				}
			}
		}
		// public Row(long addr) throws IOException {
		// int key = 0;
		// long fileLength = rows.length();
		// int num = 0;
		//
		// for (int n = 0; n < numOtherFields; n++) {
		// num = num + otherFieldLengths[n] * 2;
		// }
		// num = num + 4;
		// long numLoop = (fileLength - addr) / num;
		// char[][] rowContent = new char[numOtherFields][];
		// rows.seek(addr);
		// System.out.println("key\t\tField 1\t\t Field 2");
		// for (int m = 0; m < numLoop; m++) {
		// key = rows.readInt();
		// System.out.print(key + "\t\t");
		// for (int i = 0; i < numOtherFields; i++) {
		// rowContent[i] = new char[otherFieldLengths[i]];
		// for (int j = 0; j < rowContent[i].length; j++) {
		// rowContent[i][j] = rows.readChar();
		// System.out.print(rowContent[i][j]);
		// }
		// System.out.print("\t");
		// }
		// System.out.println();
		// }
		//
		// }

	}

	public DBTable(String filename, int fL[], int bsize) throws IOException {
		/*
		 * Use this constructor to create a new DBTable. filename is the name of
		 * the file used to store the table; fL is the lengths of the
		 * otherFields; fL.length indicates how many other fields are part of
		 * the row ; bsize is the bucket size used by the hash index ; A ExtHash
		 * object must be created for the key field in the table If a file with
		 * name "filename" exists, the file should be deleted before the new
		 * file is created.
		 */
		File file = new File(filename);
		this.extHash = new ExtHash(filename, bsize);
		if (file.exists()) {
			file.delete();
			System.out.println("deleted: " + filename);
		} else {
			System.out.println("created: " + filename);
		}
		this.otherFieldLengths = fL;
		rows = new RandomAccessFile(filename, "rw");
		this.free = 0;
		this.numOtherFields = fL.length;
		rows.writeInt(this.numOtherFields); // write INT numOtherFields 4 - addr
											// is 0
		for (int i = 0; i < this.numOtherFields; i++) {
			rows.writeInt(fL[i]); // Write LONG length 4*numOtherFields
		}
		rows.writeLong(this.free); // Write LONG free 8
	}

	public DBTable(String filename) throws IOException {
		// Use this constructor to open an existing DBTable
		rows = new RandomAccessFile(filename, "rw");
		extHash = new ExtHash(filename);
		rows.seek(0);
		this.numOtherFields = rows.readInt();
		this.otherFieldLengths = new int[this.numOtherFields];
		for (int i = 0; i < this.numOtherFields; i++) {
			this.otherFieldLengths[i] = rows.readInt();
		}
		this.free = rows.readLong();
	}

	public boolean insert(int key, char fields[][]) throws IOException {
		// PRE: the length of each row in fields matches the expected length
		/*
		 * If a row with the key is not in the table, the row is added and the
		 * method returns true otherwise the row is not added and the method
		 * returns false. The method must use the hash index to determine if a
		 * row with the key exists. If the row is added the key is also added
		 * into the hash index.
		 */
		
		long lastFreeAddr = 0;
		long oldFileLength = 0;
		long keyAddr=0;
		long fkaddr=0;
		Row newRow = new Row(key, fields);
		lastFreeAddr = getFree(this.free);
		oldFileLength = rows.length();
		keyAddr = extHash.search(key);
		if(keyAddr==0){
			newRow.writeRow(lastFreeAddr);//404
			extHash.insert(key, lastFreeAddr);
			if (lastFreeAddr != oldFileLength) {
				fkaddr = findKeyAddr(lastFreeAddr);
				if (fkaddr!=0) {
					changeFree(fkaddr);
				}else {
					this.free=0;
					rows.seek(4+4*numOtherFields);
					rows.writeLong(0);
				}
				
			}
			return true;
		}else{
			System.out.println("Key existed......");
			return false;
		}
		
	}

	/**
	 * Obtain the address of the last free slot.
	 * 
	 * @param addr:
	 *            The value of free in the DBTable.
	 * @return The address of the last free slot.
	 * @throws IOException
	 */
	public long getFree(long addr) throws IOException {
		long currKey = 0;
		if (this.free == 0) {
			return rows.length();
		}
		rows.seek(addr);
		currKey = rows.readInt();
		if (currKey == 0) {
			return addr;
		}
		return getFree(currKey);
	}

	/**
	 * This method is to change the address of the last free slot.
	 * 
	 * @param addr:
	 *            The previous address of the last free slot.
	 * @throws IOException
	 */
	public void changeFree(long addr) throws IOException {
		rows.seek(addr);
		rows.writeInt(0);
	}

	// public void addFree() throws IOException{
	// long fAddr = 0;
	// long freeValueInTable = 0;
	// fAddr = freeAddr();
	// rows.seek(fAddr);
	// freeValueInTable = rows.readLong();
	// }
	/**
	 * This method is to obtain the address of statement free in DBTable.
	 * 
	 * @return Address of free
	 * @throws IOException
	 */
	public long freeAddr() throws IOException {
		long fAddr = 0;
		fAddr = findHeadOfRow() - 8;
		return fAddr;
	}

	public boolean remove(int key) throws IOException {
		/*
		 * If a row with the key is in the table it is removed and true is
		 * returned otherwise false is returned. The method must use the hash
		 * index to determine if a row with the key exists. If the row is
		 * deleted the key must be deleted from the hash index
		 */
		long addrDelete = 0;
		long addrNewFree = 0;
		addrDelete=extHash.search(key);
		if(addrDelete==0){
			System.out.println("Key is not existed......");
			return false;
		}else{
			//addrDelete = findKeyAddr(key);
			extHash.remove(key);
			if (this.free == 0) {
				rows.seek(addrDelete);
				rows.writeInt(0);
				rows.seek(freeAddr());
				rows.writeLong(addrDelete);
				this.free = addrDelete;
			} else {
				addrNewFree = findKeyAddr(0);
				rows.seek(addrDelete);
				rows.writeInt(0);
				rows.seek(addrNewFree);
				rows.writeInt((int) addrDelete);
			}
			return true;
		}
		
	}

	public LinkedList<String> search(int key) throws IOException {
		/*
		 * If a row with the key is found in the table return a list of the
		 * other fields in the row. The string values in the list should not
		 * include the null characters used for padding. If a row with the key
		 * is not found return an empty list. The method must use the hash index
		 * to determine if a row with the key exists
		 */
		LinkedList<String> resutList = new LinkedList<String>();
		String subList = "";
		long keyAddr=0;
		//long rowBegin = findHeadOfRow();
		int keySearched = 0;
		String c = " ";
		// rows.seek(0);
		//long numLoop = calcLoopNum(rowBegin);
		//for (int m = 0; m < numLoop; m++) {
		keyAddr=extHash.search(key);
		if(keyAddr!=0){
			rows.seek(keyAddr);
			keySearched = rows.readInt();
			for (int i = 0; i < this.numOtherFields; i++) {
				for (int j = 0; j < this.otherFieldLengths[i]; j++) {
					subList = subList + rows.readChar();
				}
//				if (key == keySearched) {
				subList = subList.replace((char) 0 + "", "");
				resutList.add(subList);
				subList = "";
//				} else {
//					subList = "";
//				}
			}
		}
			
		//}
		return resutList;
	}

	/**
	 * Print the whole DB Table.
	 * 
	 * @throws IOException
	 */
	public void tableRead() throws IOException {
		int nOF = 0;// numOtherFields
		int l = 0;// length 1 2 3 ...
		long f = 0;// free
		long pos = 0;
		int key = 0;
		long rowBegin = findHeadOfRow();
		long numLoop = calcLoopNum(rowBegin);
		// rows = new RandomAccessFile(filename, "rw");
		rows.seek(0);
		nOF = rows.readInt();
		System.out.println("numOtherFields:\t" + nOF);
		for (int i = 0; i < nOF; i++) {
			l = rows.readInt();
			System.out.println("length " + (i + 1) + ":\t" + l);
		}
		f = rows.readLong();
		System.out.println("free:\t\t" + f);
		pos = rows.getFilePointer();
		// Row row = new Row(pos);
		char[][] rowContent = new char[numOtherFields][];
		rows.seek(pos);
		System.out.println("key\t\tField 1\t\t Field 2");
		for (int m = 0; m < numLoop; m++) {
			key = rows.readInt();
			System.out.print(key + "\t\t");
			for (int i = 0; i < numOtherFields; i++) {
				rowContent[i] = new char[otherFieldLengths[i]];
				for (int j = 0; j < rowContent[i].length; j++) {
					rowContent[i][j] = rows.readChar();
					System.out.print(rowContent[i][j]);
				}
				System.out.print("\t");
			}
			System.out.println();
		}
	}

	/**
	 * This method is to find the address of the begin of row.
	 * 
	 * @return Address of head of row.
	 * @throws IOException
	 */
	public long findHeadOfRow() throws IOException {
		long rowHead = 0;
		rows.seek(0);
		rows.readInt();// numOtherFields
		for (int i = 0; i < this.numOtherFields; i++) {
			rows.readInt();// Length 1 2 3 ...
		}
		rows.readLong();// free
		rowHead = rows.getFilePointer();
		return rowHead;
	}

	/**
	 * This is a method to calculate the number of loop. In other word, the
	 * number of rows.
	 * 
	 * @param rowBegin:
	 *            the begin address of row
	 * @return loopNum
	 * @throws IOException
	 */
	public long calcLoopNum(long rowBegin) throws IOException {
		long fileLength = rows.length();
		int count = 0;
		for (int n = 0; n < numOtherFields; n++) {
			count = count + otherFieldLengths[n] * 2;// 一个char两字节写入
		}
		count = count + 4;// 一个int四字节写入
		long numLoop = (fileLength - rowBegin) / count;

		return numLoop;
	}

	/**
	 * This method is to find if the key existed and return the address of the
	 * row with the key.
	 * 
	 * @param key:
	 *            the key you want to find.
	 * @return addr: not 0 means the key exists; 0 means the key not exists.
	 * @throws IOException
	 */
	public long findKeyAddr(long key) throws IOException {
		long rowBegin = findHeadOfRow();
		long keySearched = 0;
		long numLoop = calcLoopNum(rowBegin);
		long addr = 0;
		rows.seek(rowBegin);
		outer: for (int m = 0; m < numLoop; m++) {
			keySearched = rows.readInt();
			for (int i = 0; i < this.numOtherFields; i++) {
				if (key == keySearched) {
					addr = rows.getFilePointer() - 4;
					break outer;
				}
				for (int j = 0; j < this.otherFieldLengths[i]; j++) {
					rows.readChar();
				}

			}
		}
		return addr;
	}

	public void close() throws IOException {
		// close the DBTable. The table should not be used after it is closed
		rows.close();
	}
}