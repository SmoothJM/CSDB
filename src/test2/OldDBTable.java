package test2;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;

public class OldDBTable {

	RandomAccessFile rows; // the file that stores the rows in the table
	long free; // head of the free list space for rows
	int numOtherFields;
	int otherFieldLengths[];
	// add other instance variables as needed
	OldExtHash exHash;
	long freeAddr;

	private class Row {
		private int keyField;
		private char otherFields[][];		
		
		
		public char[][] getOtherFields() {
			return otherFields;
		}

		public void setOtherFields(char[][] otherFields) {
			this.otherFields = otherFields;
		}

		public void setKeyField(int keyField) {
			this.keyField = keyField;
		}

		public int getKeyField() {
			return keyField;
		}
		/*
		 * Each row consists of unique key and one or more character array
		 * fields. Each character array field is a fixed length field (for
		 * example 10 characters). Each field can have a different length.
		 * Fields are padded with null characters so a field with a length of of
		 * x characters always uses space for x characters.
		 */
		// Constructors and other Row methods
	}

	public OldDBTable(String filename, int fL[], int bsize) throws IOException {
		File file = new File(filename);
		if(file.exists()){
			file.delete();
		}		
		rows = new RandomAccessFile(file, "rw");
		rows.writeInt(fL.length);
		otherFieldLengths = new int[fL.length]; 
		for(int i=0; i<fL.length; i++){
			otherFieldLengths[i] = fL[i];
			rows.writeInt(fL[i]);
		}
		free = 0;
		freeAddr = rows.getFilePointer();
		rows.writeLong(free);
		numOtherFields = fL.length;
		
		exHash = new OldExtHash(filename,bsize);
		
		
		/*
		 * Use this constructor to create a new DBTable. filename is the name of
		 * the file used to store the table fL is the lengths of the otherFields
		 * fL.length indicates how many other fields are part of the row bsize
		 * is the bucket size used by the hash index A ExtHash object must be
		 * created for the key field in the table If a file with name filename
		 * exists, the file should be deleted before the new file is created.
		 */
	}

	public OldDBTable(String filename) throws IOException {
		rows = new RandomAccessFile(filename, "rw");		
		numOtherFields = rows.readInt();
		otherFieldLengths = new int[numOtherFields];
		for(int i=0; i<numOtherFields; i++){
			otherFieldLengths[i] = rows.readInt();
		}
		freeAddr = rows.getFilePointer();
		free = rows.readLong();
		exHash = new OldExtHash(filename);
		// Use this constructor to open an exisgng DBTable
	}

	public boolean insert(int key, char fields[][]) throws IOException {
		boolean result = false;		
		char[][] otherFields = new char[numOtherFields][];
		for(int i=0; i< numOtherFields; i++){
			otherFields[i] = new char[otherFieldLengths[i]];
		}
		for(int i=0; i<numOtherFields; i++){
			for(int j=0; j<otherFields[i].length; j++){
				otherFields[i][j] = fields[i][j];
			}
			
		}
		
		//check not duplicate
		LinkedList<String> list = search(key);		
		if(list.isEmpty()){
			long tempFree = free;
			Row row = new Row();
			//--------------------------------------------------------------------			
			row.setKeyField(key);
			row.setOtherFields(otherFields);
			
			//set free
			if(free == 0){
				long rowAddr = rows.length();
				writeRow(rows, rowAddr, row);
			//	long testAddr = rows.length();
			//	System.out.println(testAddr);
				exHash.insert(key, rowAddr);
			}else{
				rows.seek(free);
				free = rows.readLong();
				rows.seek(freeAddr);
				rows.writeLong(free);
				writeRow(rows, tempFree, row);
				exHash.insert(key, tempFree);				
			}						
			result = true;
		}
		// PRE: the length of each row in fields matches the expected length
		/*
		 * If a row with the key is not in the table, the row is added and the
		 * method returns true otherwise the row is not added and the method
		 * returns false. The method must use the hash index to determine if a
		 * row with the key exists. If the row is added the key is also added
		 * into the hash index.
		 */

		return result;
	}

	public boolean remove(int key) throws IOException {
		boolean result = false;
		long rowAddr = exHash.search(key);
		long tempFree = free;
		if(rowAddr != 0){
			//set free
			free = rowAddr;	
			if(free != 0){				
				rows.seek(rowAddr);
				rows.writeLong(tempFree);
			}				
			rows.seek(freeAddr);
			rows.writeLong(free);	
			exHash.remove(key);
			result = true;
		}
		
		return result;
		/*
     	 * If a row with the key is in the table it is removed and true is
		 * returned otherwise false is returned. The method must use the hash
		 * index to determine if a row with the key exists. If the row is
		 * deleted the key must be deleted from the hash index
		 */
	}

	public LinkedList<String> search(int key) throws IOException {
		
		LinkedList<String> result = new LinkedList<String>();
		long rowAddr = exHash.search(key);
		if(rowAddr != 0){
			rows.seek(rowAddr);
			Row row = readRow(rows, rowAddr);
			String field = "";
			char otherFields[][] = new char[numOtherFields][];
			for(int i=0; i< numOtherFields; i++){
				otherFields[i] = new char[otherFieldLengths[i]];
			}			
			otherFields = row.getOtherFields();
			for(int i=0; i<otherFields.length; i++){
				for(int j=0; j<otherFields[i].length; j++){
					field = field + otherFields[i][j];
				}
				result.add(field);
				field = "";
			}
			return result;
		}
		return result;
		/*
		If a row with the key is found in the table return a list of the other fields in the row.
		The string values in the list should not include the null characters used for padding.
		If a row with the key is not found return an empty list
		The method must use the hash index index to determine if a row with the key exists*/		 
	}
	
	public void close() {
		try {
			rows.close();
			exHash.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//close the DBTable. The table should not be used aler it is closed
	}
	public Row readRow(RandomAccessFile rows, long addr) throws IOException{
		Row row = new Row();
		rows.seek(addr);		
		int keyField = rows.readInt();
		row.setKeyField(keyField);		
		char otherFields[][] = new char[numOtherFields][];
		for(int i=0; i< numOtherFields; i++){
			otherFields[i] = new char[otherFieldLengths[i]];
		}			
		for(int i=0; i<numOtherFields; i++){
			for(int j=0; j<otherFieldLengths[i]; j++){
				otherFields[i][j] = rows.readChar();
			}
		}
		row.setOtherFields(otherFields);		
		return row;
		
	}

	public void writeRow(RandomAccessFile rows, long addr, Row row) throws IOException{
		rows.seek(addr);
		int key = row.getKeyField();
		rows.writeInt(key);		
		char otherFields[][] = new char[numOtherFields][];
		for(int i=0; i< numOtherFields; i++){
			otherFields[i] = new char[otherFieldLengths[i]];
		}		
		
		for(int i=0; i<numOtherFields; i++){
			for(int j=0; j<otherFields[i].length; j++){
				otherFields[i][j] = row.getOtherFields()[i][j];
			}
			
		}
		
		
		
		for(int i=0; i<otherFields.length; i++){
			for(int j=0; j<otherFields[i].length; j++){
				rows.writeChar(otherFields[i][j]);				
			}
		}
	}	
}
