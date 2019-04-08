package test;

import java.io.RandomAccessFile;

public class Read {

	public static void main(String[] args) throws Exception {
		RandomAccessFile raf = new RandomAccessFile("c:/Users/14534/Desktop/data.txt", "rw");
		raf.seek(0);
		String first = null;
		String last = null;
		int age = 0;
		int record = 48;
		long rafLength = raf.length();
		long loop = rafLength / record;
		String name = "Yali";
		for (int i = 0; i < loop; i++) {
			first = raf.readUTF();
			for (int j = 0; j < 20 - first.length(); j++) {
				raf.readByte();
			}
			last = raf.readUTF();
			for (int j = 0; j < 20 - last.length(); j++) {
				raf.readByte();
			}
			age = raf.readInt();
//			System.out.println(raf.getFilePointer());
			System.out.println(first + "--" + last + "--" + age);
//			if(name.equals(first)){
//				System.out.println(first + "--" + last + "--" + age);
//				raf.seek(record*i+44);
//				raf.writeInt(24);
//				break;
//			}
		}
		
	}

}
