package test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.junit.Test;

public class RAFTest {
	@Test
	public void wTest() throws Exception {
		RandomAccessFile raf = new RandomAccessFile("c:/Users/14534/Desktop/data2.txt", "rw");
		Person p1 = new Person(1, "Jianmu", 1.81d);
		Person p2 = new Person(2, "Yali", 1.82d);
		Person p3 = new Person(3, "Doe", 1.83d);
		Person p4 = new Person(4, "Nick", 1.84d);
		Person p5 = new Person(5, "Pheno", 1.85d);
		p1.write(raf);
		p2.write(raf);
		p3.write(raf);
		p4.write(raf);
		p5.write(raf);
		raf.close();
	}

	@Test
	public void rTest() throws Exception {
		RandomAccessFile raf = new RandomAccessFile("c:/Users/14534/Desktop/data2.txt", "rw");
		long loopNum = raf.length() / 38;
		//System.out.println(raf.length() + "=" + loopNum);
		Integer id = 0;
		String name = null;
		Double height = 0.0;
		// String searchByName = "Jianmu";
		raf.seek(0);
		for (int i = 0; i < loopNum; i++) {
			System.out.println(raf.getFilePointer());
			id = raf.readInt();
			System.out.println(raf.getFilePointer());
			for (int j = 0; j < 5 - id.toString().length(); j++) {
				raf.readByte();
			}
			System.out.println(raf.getFilePointer());
			name = raf.readUTF();
			System.out.println(raf.getFilePointer());
			for (int j = 0; j < 20 - name.length(); j++) {
				raf.readByte();
			}
			System.out.println(raf.getFilePointer());
			height = raf.readDouble();
			System.out.println(raf.getFilePointer());
			// if (searchByName.equals(name)) {
			System.out.println(id + "--" + name + "--" + height);
			// }
		}

		raf.close();
	}

	@Test
	public void overwriteString() throws IOException {
		RandomAccessFile raf = new RandomAccessFile("c:/Users/14534/Desktop/data2.txt", "rw");
		// 找到Doe,將其姓名改為Dongyi
		// 1.找到doe
		String newName = "Dongyii";
		Person pNew = new Person(3,newName,1.83d);
		Integer id = 0;
		String name = null;
		Double height = 0.0;
		long loopNum = raf.length() / 38;
		long posN = 0;
		long pos = 0;
		raf.seek(0);
		for (int i = 0; i < loopNum; i++) {
			id = raf.readInt();
			for (int j = 0; j < 5 - id.toString().length(); j++) {
				raf.readByte();
			}
			name = raf.readUTF();
			for (int j = 0; j < 20 - name.length(); j++) {
				raf.readByte();
			}
			height = raf.readDouble();
			pos=raf.getFilePointer();
			posN=pos-33;
			if (name.equals("Doe")) {// 2.替換
				raf.seek(38*i+8);
				raf.writeUTF(newName);
				break;
			}
		}
	}
	@Test
	public void delete() throws IOException {
		RandomAccessFile raf = new RandomAccessFile("c:/Users/14534/Desktop/data2.txt", "rw");
		String nameWanted="Nick";
		char[] empty=new char[nameWanted.length()];
		String str1 = new String(empty);
		Integer id = 0;
		String name = null;
		Double height = 0.0;
		long loopNum = raf.length() / 38;
		raf.seek(0);
		for (int i = 0; i < loopNum; i++) {
			System.out.println(raf.getFilePointer());
			id = raf.readInt();
			for (int j = 0; j < 5 - id.toString().length(); j++) {
				raf.readByte();
			}
			System.out.println(raf.getFilePointer());
			name = raf.readUTF();
			System.out.println(raf.getFilePointer());
			for (int j = 0; j < 20 - name.length(); j++) {
				raf.readByte();
			}
			System.out.println(raf.getFilePointer());
			height = raf.readDouble();
			System.out.println(raf.getFilePointer());
			System.out.println(id + "--" + name + "--" + height);
			if (name.equals(nameWanted)) {
				raf.seek(38*i+8);
				raf.writeUTF(str1);
				break;
			}
		}
	}
	@Test
	public void charToString() throws IOException {
		String nameWanted="Nick";
		char[] empty=new char[nameWanted.length()];
		System.out.println(empty);
		String str1 = new String(empty);
		System.out.println(str1.length());
	}
	@Test
	public void closeTest() throws IOException {
		RandomAccessFile raf = new RandomAccessFile("c:/Users/14534/Desktop/data2.txt", "rw");
	}
	@Test
	public void h2Test() throws IOException {
		RandomAccessFile raf = new RandomAccessFile("src/f1", "r");
//		RandomAccessFile raf = new RandomAccessFile("src/h2/allFiles/DBTable_01", "rw");
//		RandomAccessFile raf = new RandomAccessFile("src/h2/allFiles/01", "rw");
		raf.seek(0);
		int i = 0;
		i=raf.readInt();
		raf.close();
	}
}
