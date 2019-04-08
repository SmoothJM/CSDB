package test;

import java.io.IOException;
import java.io.RandomAccessFile;

public class Person {
	Integer id;
	String name;
	Double height;

	public Person(int id, String name, double height) {
		this.id = id;
		this.name = name;
		this.height = height;
	};

	public Person() {
	}

	public void write(RandomAccessFile raf) throws IOException {
		raf.seek(raf.length());
		raf.writeInt(this.id);// 写4字节（一个1，三个空）多了3
//		// System.out.println(this.id.toString().length());
//		for (int i = 0; i < 5 - this.id.toString().length(); i++) {
//			raf.writeByte(20);
//		}
//		raf.writeUTF(this.name);// 多写2字节
//		for (int i = 0; i < 20 - this.name.length(); i++) {
//			raf.writeByte(20);
//		}
//		raf.writeDouble(this.height);// 写八字节（4个字节是值，4个字节是空）多了4
//		System.out.println(this.height.toString().length());
		// raf.writeBytes("\r\n");

	}
	// public void read (RandomAccessFile raf) throws IOException{
	// long loopNum = raf.length()/33;
	// raf.seek(0);
	// for(int i =0;i<loopNum;i++){
	// this.id=raf.readInt();
	// for(int j=0;j<5-this.id.toString().length();j++){
	// raf.readByte();
	// }
	// this.name=raf.readUTF();
	// for(int j=0;j<20-this.name.length();j++){
	// raf.readByte();
	// }
	// this.height=raf.readDouble();
	// }
	// }

}