package h2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import javax.print.attribute.HashAttributeSet;

public class DriveTest {

	public static void main(String[] args) throws IOException {
		//ExtHash e1 = new ExtHash("allFiles/Directory_01");
		int[] fl ={10,20};
		char[][] fl2 ={{'A','n','t','o','n'},{'C','h','e','k','h','o','v'}};
		char[][] fl3 ={{'V','l','a','d','i','m','i','r'},{'N','a','b','o','k','o','v'}};
		char[][] fl4 ={{'A','l','o','n','z','o'},{'C','h','u','r','c','h'}};
		char[][] fl1 = {{},{}};
		char[][] fl6 ={{'M','a','r','k'},{'T','w','a','i','n'}};
		char[][] fl5 ={{'G','o','t','t','t','l','o','b'},{'F','r','e','g','e'}};
		char[][] fl7 ={{'H','a','n','n','a','h'},{'A','r','e','n','d','t'}};
		char[][] fl8 ={{'G','e','o','r','g','e'},{'E','l','i','o','t'}};
		char[][] fl9 ={{'J','i','a','n','m','u'},{'D','e','n','g'}};
//		DBTable dbR = new DBTable("src/h2/allFiles/DBTable_01",fl,2);
		DBTable dbR = new DBTable("src/h2/allFiles/DBTable_01");
//		DBTable dbR = new DBTable("src/f1");
//		dbR.insert(50, fl2);
//		dbR.insert(10, fl3);
//		dbR.insert(61, fl4);
//		dbR.insert(21, fl5);
//		dbR.insert(40, fl6);
//		dbR.insert(70, fl7);
		dbR.insert(31, fl8);
		
//		dbR.remove(31);
		dbR.tableRead();
		System.out.println("------------");
//		System.out.println(dbR.search(999));
//		System.out.println(dbR.search(10));
//		System.out.println(dbR.findKey(60));
//		System.out.println(dbR.findKey(190));
		//Sys/m.out.println(raf.length());
		//System.out.println(dbR.getFree(0));
		//System.out.println(dbR.findKeyAddr(0));
		//System.out.println(dbR.findKeyAddr(999));
		//dbR.close();
		
//		ExtHash extHash = new ExtHash("src/h2/allFiles/DBTable_01",2);
		ExtHash extHash = new ExtHash("src/h2/allFiles/DBTable_01");
//		ExtHash extHash = new ExtHash("src/f1");
		extHash.dirDisplay();
		System.out.println("-------------");
		extHash.bucketDisplay();
//		int[] k1 = {60,10};
//		int[] k2 = {40,50};
//		int[] k3 = {70,30};
//		int[] k4 = {20,0};
//		long[] a1 ={212,148};
//		long[] a2 ={532,20};
//		long[] a3 ={404,596};
//		long[] a4 ={340,0};
//		insertTest(2,2,k2,a2);
//		insertTest(2,2,k3,a3);
//		insertTest(2,2,k4,a4);
		//extHash.bucketDisplay();
		//System.out.println(extHash.search(10));
//		int h = hash(10);
//		System.out.println(h);
		//insertDir();
		for (int i = 0; i < 24; i++) {
			dbR.search(i);
		}
	}
	public static void insertTest(int nBits, int nKeys, int[] keys,long[] addr) throws IOException{
		RandomAccessFile raf = new RandomAccessFile("src/h2/allFiles/DBTable_01_buckets", "rw");
		raf.seek(raf.length());
		raf.writeInt(nBits);
		raf.writeInt(nKeys);
		for(int i=0;i<nKeys;i++){
			raf.writeInt(keys[i]);
		}
		for(int i=0;i<nKeys;i++){
			raf.writeLong(addr[i]);
		}
	}
	public static int hash(int key) {

		
		// return the hash value
		int hashValue = 0;
		int sum =0;
		String result = "";
		for (int i = key; i >= 1; i = i / 2) {
			if (i % 2 == 0) {
				sum = 0;
			} else {
				sum = 1;
			}
			result = sum + result;
		}
		result = result.substring(result.length()-2, result.length());
		hashValue = Integer.parseInt(result,2);
		return hashValue;
	}
	
	public static void insertDir() throws IOException{
		RandomAccessFile raf = new RandomAccessFile("src/h2/allFiles/DBTable_01_dir", "rw");
		raf.seek(raf.length());
		raf.writeLong(4);
		raf.writeLong(36);
		raf.writeLong(68);
		raf.writeLong(100);
		raf.close();
	}
}
