package test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;

public class OldDBTable {
	RandomAccessFile rows; // the file that stores the rows in the table
	long free; // head of the free list space for rows
	int numOtherFields;
	int otherFieldLengths[];
	long rootaddr;// address of first data
	int rowlength;
	BTree Btree;

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
		private Row(int key, char fields[][]) {
			this.keyField = key;
			this.otherFields = fields;
		}

		private void writeRow(long addr) throws IOException {
			rows.seek(addr);
			rows.writeInt(keyField);
			for (int i = 0; i < numOtherFields; i++) {
				for (int j = 0; j < otherFieldLengths[i]; j++) {
					if (i < otherFields.length && j < otherFields[i].length) {
						rows.writeChar(otherFields[i][j]);
					} else {
						rows.writeChar(0);
					}
				}
			}
		}

		private Row(long addr) throws IOException {// 读某一行
			rows.seek(addr);
			keyField = rows.readInt();
			otherFields = new char[numOtherFields][];
			for (int i = 0; i < numOtherFields; i++) {
				otherFields[i] = new char[otherFieldLengths[i]];
				for (int j = 0; j < otherFieldLengths[i]; j++) {
					otherFields[i][j] = rows.readChar();
				}
			}
		}
	}

	public OldDBTable(String filename, int fL[], int bsize) throws IOException {
		/*
		 * Use this constructor to create a new DBTable. filename is the name of
		 * the file used to store the table fL is the lengths of the otherFields
		 * fL.length indicates how many other fields are part of the row bsize
		 * is the block size. It is used to calculate the order of the B+Tree A
		 * B+Tree must be created for the key field in the table If a file with
		 * name filename exists, the file should be deleted before the new file
		 * is created.
		 */
		File path = new File(filename);
		if (path.exists()) {
			path.delete();
		}
		Btree = new BTree("BTreeTable", bsize);
		rows = new RandomAccessFile(path, "rw");
		numOtherFields = fL.length;
		otherFieldLengths = fL;// 10&20
		free = 0;
		rows.writeInt(numOtherFields);
		for (int i = 0; i < numOtherFields; i++) {
			rows.writeInt(otherFieldLengths[i]);
		}
		rows.writeLong(free);
		rootaddr = (numOtherFields + 1) * 4 + 8;
		for (int i = 0; i < numOtherFields; i++) {
			rowlength += (otherFieldLengths[i] * 2);
		}
		rowlength += 4;
	}

	public OldDBTable(String filename) throws IOException {
		// Use this constructor to open an existing DBTable
		rows = new RandomAccessFile(filename, "rw");
		rows.seek(0);
		numOtherFields = rows.readInt();
		otherFieldLengths = new int[numOtherFields];
		for (int i = 0; i < numOtherFields; i++) {
			otherFieldLengths[i] = rows.readInt();
		}
		free = rows.readLong();
		rootaddr = (numOtherFields + 1) * 4 + 8;
		for (int i = 0; i < numOtherFields; i++) {
			rowlength += (otherFieldLengths[i] * 2);
		}
		rowlength += 4;
	}

	public boolean insert(int key, char fields[][]) throws IOException {
		// PRE: the length of each row is fields matches the expected length
		/*
		 * If a row with the key is not in the table, the row is added and the
		 * method returns true otherwise the row is not added and the method
		 * returns false. The method must use the B+tree to determine if a row
		 * with the key exists. If the row is added the key is also added into
		 * the B+tree.
		 */
		if (search(key).isEmpty() == false) {
			return false;
		}
		Row newRow = new Row(key, fields);
		newRow.writeRow(getFree());
		return true;
	}

	long searchaddr;

	public boolean remove(int key) throws IOException {
		/*
		 * If a row with the key is in the table it is removed and true is
		 * returned otherwise false is returned. The method must use the B+Tree
		 * to determine if a row with the key exists.
		 * 
		 * If the row is deleted the key must be deleted from the B+Tree
		 */
		if (search(key).isEmpty()) {
			return false;
		}
		addfree(searchaddr);
		return true;
	}

	public LinkedList<String> search(int key) throws IOException {
		/*
		 * If a row with the key is found in the table return a list of the
		 * other fields in the row. The string values in the list should not
		 * include the null characters. If a row with the key is not found
		 * return an empty list The method must use the equality search in
		 * B+Tree
		 */
		LinkedList<String> out = new LinkedList<>();
		String temp = "";
		long curraddr = rootaddr;
		if ((curraddr + rowlength) > rows.length()) {
			return out;
		}
		Row currRow = new Row(curraddr);
		while (currRow.keyField > -1) {
			if (currRow.keyField == key) {
				for (int i = 0; i < numOtherFields; i++) {
					for (int j = 0; j < otherFieldLengths[i]; j++) {
						if (currRow.otherFields[i][j] == 0) {
							break;
						}
						temp += currRow.otherFields[i][j];
					}
					out.add(temp);
					temp = "";
				}
				searchaddr = curraddr;
			}
			curraddr += rowlength;
			if ((curraddr + rowlength) > rows.length()) {
				break;
			}
			currRow = new Row(curraddr);
		}
		return out;
	}

	public long getFree() throws IOException {
		if (free == 0) {
			return rows.length();
		}
		long temp = free;
		rows.seek(free);
		free = rows.readLong();
		return temp;
	}

	private void addfree(long addr) {
		try {
			rows.seek(addr);
			rows.writeLong(free);
			free = addr;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public LinkedList<LinkedList<String>> rangeSearch(int low, int high) throws IOException {
		// PRE: low <= high
		/*
		 * For each row with a key that is in the range low to high inclusive a
		 * list of the fields (including the key) in the row is added to the
		 * list returned by the call. If there are no rows with a key in the
		 * range return an empty list The method must use the range search in
		 * B+Tree
		 */
		LinkedList<String> rowlist = new LinkedList<>();
		LinkedList<LinkedList<String>> out = new LinkedList<LinkedList<String>>();
		String temp = "";
		long curraddr = rootaddr;
		if ((curraddr + rowlength) > rows.length()) {
			return out;
		}
		Row currRow = new Row(curraddr);
		while (currRow.keyField > -1) {
			if (currRow.keyField >= low && currRow.keyField <= high) {
				rowlist.add(currRow.keyField + "");
				for (int i = 0; i < numOtherFields; i++) {
					for (int j = 0; j < otherFieldLengths[i]; j++) {
						if (currRow.otherFields[i][j] == 0) {
							break;
						}
						temp += currRow.otherFields[i][j];
					}
					rowlist.add(temp);
					temp = "";
				}
				out.add(rowlist);
				rowlist = new LinkedList<>();
			}
			curraddr += rowlength;
			if ((curraddr + rowlength) > rows.length()) {
				break;
			}
			currRow = new Row(curraddr);
		}
		return out;
	}

	public void print() {
		// Print the rows to standard output is ascending order (based on the
		// keys)
		// One row per line
	}

	public void close() throws IOException {
		// close the DBTable. The table should not be used after it is closed
		rows.close();
	}

	public static void main(String args[]) throws IOException {
		int fl[] = { 10, 20 };
		char fields[][] = { { 'a', 'b' }, { 'c', 'd' } };
		char fields2[][] = { { 'E', 'r', 'i', 'c' }, { 'Q', 'u', 'e' } };
		// System.out.println(fields2[0][4]);
		OldDBTable a = new OldDBTable("DBtest", fl, 48);
		// a.insert(29, fields);
		// a.insert(44, fields2);
		// a.insert(77, fields);
		// a.insert(30, fields2);
		// System.out.println(a.remove(44));
		// System.out.println(a.search(44));
		// System.out.println(a.free);
		// System.out.println(a.rows.length());
		// System.out.println(a.rangeSearch(1, 77));
	}
}