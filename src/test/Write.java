package test;

import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

public class Write {

	public static void main(String[] args) throws Exception {
		RandomAccessFile raf = new RandomAccessFile("c:/Users/14534/Desktop/data.txt", "rw");
		raf.seek(raf.length());
		String first = "Yali";
		String last = "Wang";
		int age = 23;
		raf.writeUTF(first);
		for (int i = 0; i < 20 - first.length(); i++) {
			raf.writeByte(32);
		}
		raf.writeUTF(last);
		for (int i = 0; i < 20 - last.length(); i++) {
			raf.writeByte(32);
		}
		raf.writeInt(age);
	}

}
