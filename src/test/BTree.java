package test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;
import java.util.Stack;

public class BTree {
	RandomAccessFile f;
	int order;
	int blockSize;
	long root;
	long free;

	// add instance variables as needed.
	private class BTreeNode {
		private int count;
		private int keys[];
		private long children[];

		// constructors and other method
		private BTreeNode(int c, int[] key, long[] child) {
			count = c;
			keys = key;
			children = child;
		}

		private void writeNode(long addr) throws IOException {
			f.seek(addr);
			f.writeInt(count);
			for (int i = 0; i < keys.length; i++) {
				f.writeInt(keys[i]);
			}
			for (int i = 0; i < children.length; i++) {
				f.writeLong(children[i]);
			}
		}

		private BTreeNode(long addr) throws IOException {
			f.seek(addr);
			count = f.readInt();
			keys = new int[blockSize / 12 - 1];
			children = new long[blockSize / 12];
			for (int i = 0; i < keys.length; i++) {
				keys[i] = f.readInt();
			}
			for (int i = 0; i < children.length; i++) {
				children[i] = f.readLong();
			}
		}
	}

	public BTree(String filename, int bsize) throws IOException {
		// bsize is the block size. This value is used to calculate the order
		// of the B+Tree
		// all B+Tree nodes will use bsize bytes
		// makes a new B+tree
		File path = new File(filename);
		if (path.exists()) {
			path.delete();
		}
		f = new RandomAccessFile(path, "rw");
		root = 0;
		free = 0;
		blockSize = bsize;
		f.writeLong(root);
		f.writeLong(free);
		f.writeInt(blockSize);
	}

	public BTree(String filename) throws IOException {
		// open an existing B+Tree
		f = new RandomAccessFile(filename, "rw");
		f.seek(0);
		root = f.readLong();
		free = f.readLong();
		blockSize = f.readInt();
	}

	public boolean insert(int key, long addr) throws IOException {
		/*
		 * If key is not a duplicate add, key to the B+tree addr (in DBTable) is
		 * the address of the row that contains the key return true if the key
		 * is added return false if the key is a duplicate
		 */
		if (root == 0)// first insert
		{
			int[] currkeys = new int[blockSize / 12 - 1];
			long[] currchildren = new long[blockSize / 12];
			currkeys[0] = key;
			currchildren[0] = addr;
			BTreeNode newNode = new BTreeNode(-1, currkeys, currchildren);
			newNode.writeNode(20);
			root = 20;
			return true;
		}
		if (search(key) != 0) {
			return false;
		}
		long curraddr = Path.peek();
		BTreeNode currNode = new BTreeNode(Path.pop());
		int keycount = -currNode.count;
		if (keycount < blockSize / 12 - 1)// in bound in leaf
		{
			addkey(key, curraddr, addr, currNode);
			return true;
		} else// out bound in leaf
		{
			BTreeNode temp = addoverkey(key, curraddr, addr, currNode);
			int push = temp.count;// push key
			long curraddr2 = temp.children[0];// new Node address
			if (curraddr == root)// root is leaf
			{
				int[] keys = new int[blockSize / 12 - 1];
				long[] children = new long[blockSize / 12];
				keys[0] = push;
				children[0] = curraddr;
				children[1] = curraddr2;
				BTreeNode newroot = new BTreeNode(1, keys, children);
				root = getFree();
				f.seek(0);
				f.writeLong(root);
				newroot.writeNode(root);
				return true;
			} else {
				Long pushTo;
				pushTo = Path.pop();
				BTreeNode pushNode = new BTreeNode(pushTo);
				if (pushNode.count < blockSize / 12 - 1) {
					pushNode.keys[pushNode.count] = push;
					pushNode.children[pushNode.count + 1] = curraddr2;
					pushNode.count++;
					pushNode.writeNode(pushTo);
					return true;
				} else {
					while (pushNode.count == blockSize / 12 - 1) {
						temp = addNoleaf(push, pushTo, curraddr2, pushNode);
						push = temp.count;// push key
						curraddr2 = temp.children[0];// new Node address
						if (Path.size() == 0)// the out bound Non-Leaf node is
												// root, create new root
						{
							int[] currkeys = new int[blockSize / 12 - 1];
							long[] currchildren = new long[blockSize / 12];
							currkeys[0] = push;
							currchildren[0] = pushTo;
							currchildren[1] = curraddr2;
							long newrootad = getFree();
							root = newrootad;
							BTreeNode newrootNode = new BTreeNode(1, currkeys, currchildren);
							newrootNode.writeNode(newrootad);
							return true;
						} else {
							pushTo = Path.pop();
							pushNode = new BTreeNode(pushTo);
						}
					}
					addboundNodleaf(push, pushTo, curraddr2, pushNode);
				}
				return true;
			}
		}
	}

	private void addboundNodleaf(int key, long Nodeaddr, long dataaddr, BTreeNode currNode) throws IOException {
		currNode.children[currNode.count + 1] = currNode.children[currNode.count];
		for (int i = (currNode.count) - 1; i >= 0; i--) {
			if (currNode.keys[i] > key) {
				currNode.keys[i + 1] = currNode.keys[i];
				currNode.children[i + 1] = currNode.children[i];
			} else {
				currNode.keys[i + 1] = key;
				currNode.children[i + 2] = dataaddr;
				currNode.count++;
				break;
			}
		}
		currNode.writeNode(Nodeaddr);
	}

	// add the push key to a out bound Non-leaf node, return the next push key
	private BTreeNode addNoleaf(int key, long Nodeaddr, long dataaddr, BTreeNode currNode) throws IOException {
		int[] tempkeys = new int[blockSize / 12];
		long[] tempchild = new long[blockSize / 12 + 1];
		System.arraycopy(currNode.keys, 0, tempkeys, 0, currNode.keys.length);
		System.arraycopy(currNode.children, 0, tempchild, 0, currNode.children.length);
		tempchild[blockSize / 12] = tempchild[blockSize / 12 - 1];
		int out = -1;
		for (int i = (currNode.count) - 1; i >= 0; i--) {
			if (tempkeys[i] > key) {
				tempkeys[i + 1] = tempkeys[i];
				tempchild[i + 1] = tempchild[i];
			} else {
				tempkeys[i + 1] = key;
				tempchild[i + 2] = dataaddr;
				break;
			}
		}
		int[] newkeys1 = new int[blockSize / 12 - 1];
		int[] newkeys2 = new int[blockSize / 12 - 1];
		long[] newchild1 = new long[blockSize / 12];
		long[] newchild2 = new long[blockSize / 12];
		System.arraycopy(tempkeys, 0, newkeys1, 0, blockSize / 24);
		System.arraycopy(tempkeys, blockSize / 24 + 1, newkeys2, 0, blockSize / 12 - blockSize / 24 - 1);
		System.arraycopy(tempchild, 0, newchild1, 0, blockSize / 24 + 1);
		System.arraycopy(tempchild, blockSize / 24 + 1, newchild2, 0, blockSize / 12 - blockSize / 24);
		out = tempkeys[blockSize / 24];
		long newNodeaddr = getFree();
		BTreeNode newNode1 = new BTreeNode(blockSize / 24, newkeys1, newchild1);
		BTreeNode newNode2 = new BTreeNode(blockSize / 12 - blockSize / 24 - 1, newkeys2, newchild2);
		newNode1.writeNode(Nodeaddr);
		newNode2.writeNode(newNodeaddr);
		long[] outaddr = new long[2];
		outaddr[0] = newNodeaddr;
		return new BTreeNode(out, newkeys1, outaddr);
	}

	// add key to a out bound leaf, return the value to push to the parent
	private BTreeNode addoverkey(int key, long Nodeaddr, long dataaddr, BTreeNode currNode) throws IOException {
		int[] tempkeys = new int[blockSize / 12];
		long[] tempchild = new long[blockSize / 12 + 1];
		System.arraycopy(currNode.keys, 0, tempkeys, 0, currNode.keys.length);
		System.arraycopy(currNode.children, 0, tempchild, 0, currNode.children.length);
		tempchild[blockSize / 12] = tempchild[blockSize / 12 - 1];// move last
																	// children(sibling
																	// pointer)
		int out = -1;
		for (int i = (-currNode.count) - 1; i >= 0; i--) {
			if (tempkeys[i] > key) {
				tempkeys[i + 1] = tempkeys[i];
				tempchild[i + 1] = tempchild[i];
			} else {
				tempkeys[i + 1] = key;
				tempchild[i + 1] = dataaddr;
				break;
			}
		}
		int[] newkeys1 = new int[blockSize / 12 - 1];
		int[] newkeys2 = new int[blockSize / 12 - 1];
		long[] newchild1 = new long[blockSize / 12];
		long[] newchild2 = new long[blockSize / 12];
		System.arraycopy(tempkeys, 0, newkeys1, 0, blockSize / 24);
		System.arraycopy(tempkeys, blockSize / 24, newkeys2, 0, blockSize / 12 - blockSize / 24);
		System.arraycopy(tempchild, 0, newchild1, 0, blockSize / 24);
		System.arraycopy(tempchild, blockSize / 24, newchild2, 0, blockSize / 12 - blockSize / 24);
		newchild2[newchild2.length - 1] = tempchild[tempchild.length - 1];
		out = newkeys2[0];
		long newNodeaddr = getFree();
		newchild1[newchild1.length - 1] = newNodeaddr;// add sibling pointer
		BTreeNode newNode1 = new BTreeNode(-blockSize / 24, newkeys1, newchild1);
		BTreeNode newNode2 = new BTreeNode(-(blockSize / 12 - blockSize / 24), newkeys2, newchild2);
		newNode1.writeNode(Nodeaddr);
		newNode2.writeNode(newNodeaddr);
		long[] outaddr = new long[1];
		outaddr[0] = newNodeaddr;
		return new BTreeNode(out, newkeys1, outaddr);
		// count is the push number, children[0] is address of newNode
	}

	// add key to in bound leaf
	private void addkey(int key, long Nodeaddr, long dataaddr, BTreeNode currNode) throws IOException {
		for (int i = (-currNode.count) - 1; i >= 0; i--) {
			if (currNode.keys[i] > key) {
				currNode.keys[i + 1] = currNode.keys[i];
				currNode.keys[i] = 0;
				currNode.children[i + 1] = currNode.children[i];
			} else {
				currNode.keys[i + 1] = key;
				currNode.children[i + 1] = dataaddr;
				currNode.count--;
				break;
			}
		}
		if (currNode.keys[0] == 0) {
			currNode.keys[0] = key;
			currNode.children[0] = dataaddr;
			currNode.count--;
		}
		currNode.writeNode(Nodeaddr);
	}

	public long remove(int key) {
		return 0;
		/*
		 * If the key is in the Btree, remove the key and return the address of
		 * the row return 0 if the key is not found in the B+tree
		 */
	}

	boolean find = false;

	// return next search address
	private long findaddr(int key, BTreeNode currNode) {
		int i = 0;
		int count = currNode.count;
		if (count < 0)// leaf
		{
			count = -count;
			while (i < count) {
				if (key == currNode.keys[i]) {
					find = true;
					return currNode.children[i];
				}
				i++;
			}
			return -1;// no exist;
		}
		while (i < count)// no leaf give search address
		{
			if (key < currNode.keys[i]) {
				return currNode.children[i];
			}
			if ((key >= currNode.keys[i] && i == count - 1)
					|| (key >= currNode.keys[i] && key < currNode.keys[i + 1])) {
				return currNode.children[i + 1];
			}
			i++;
		}
		return currNode.children[i];
	}

	Stack<Long> Path = new Stack<>();

	public long search(int k) throws IOException {
		/*
		 * This is an equality search If the key is found return the address of
		 * the row with the key otherwise return 0
		 */
		Path.clear();
		BTreeNode currNode = new BTreeNode(root);
		long path = findaddr(k, currNode);
		Path.push(root);
		while (path > 0) {
			if (find == true) {
				find = false;
				return path;
			}
			currNode = new BTreeNode(path);
			Path.push(path);
			path = findaddr(k, currNode);
		}
		return 0;
	}

	public long getFree() throws IOException {
		if (free == 0) {
			return f.length();
		}
		long temp = free;
		f.seek(free);
		free = f.readLong();
		return temp;
	}

	public LinkedList<Long> rangeSearch(int low, int high) throws IOException
	// PRE: low <= high
	/*
	 * return a list of row addresses for all keys in the range low to high
	 * inclusive return an empty list when no keys are in the range
	 */
	{
		LinkedList<Long> out = new LinkedList<>();
		search(low);
		long start = Path.peek();
		search(high);
		BTreeNode currNode = new BTreeNode(start);
		int i = 0;
		while (currNode.keys[i] < low) {
			i++;
			if (i == -currNode.count) {
				if (currNode.children[blockSize / 12 - 1] == 0) {
					return null;
				}
				currNode = new BTreeNode(currNode.children[blockSize / 12 - 1]);
				i = 0;
			}
		}
		while (currNode.keys[i] <= high) {
			out.add(currNode.children[i]);
			System.out.println(currNode.keys[i]);
			i++;
			if (i == -currNode.count) {
				if (currNode.children[blockSize / 12 - 1] == 0) {
					return out;
				}
				currNode = new BTreeNode(currNode.children[blockSize / 12 - 1]);
				i = 0;
			}
		}
		return out;
	}

	public void print() throws IOException {
		// print the B+Tree to standard output
		// print one node per line
		// This method can be helpful for debugging
		long p = 20;
		String out = "";
		System.out.println("root: " + root);
		while (p < f.length()) {
			out += p + "  ";
			BTreeNode curr = new BTreeNode(p);
			out += curr.count + "  ";
			for (int i = 0; i < blockSize / 12 - 1; i++) {
				out += (" " + curr.keys[i]);
			}
			out += ("  ");
			for (int i = 0; i < blockSize / 12; i++) {
				out += (" " + curr.children[i]);
			}
			System.out.println(out);
			out = "";
			p += blockSize;
		}
	}

	public void close() throws IOException {
		// close the B+tree. The tree should not be accessed after close is
		// called
		f.seek(0);
		f.writeLong(root);
		f.writeLong(free);
		f.close();
	}

	public static void main(String args[]) throws IOException {
		BTree a = new BTree("BTreetest", 48);
		System.out.println(a.insert(29, 20));
		System.out.println(a.insert(44, 84));
		System.out.println(a.insert(77, 148));
		System.out.println(a.insert(30, 242));
		System.out.println(a.insert(84, 276));
		System.out.println(a.insert(148, 10));
		System.out.println(a.insert(222, 60));
		System.out.println(a.insert(340, 20));
		System.out.println(a.insert(276, 84));
		System.out.println(a.insert(150, 260));
		System.out.println(a.insert(180, 280));
		System.out.println(a.insert(22, 543));
		System.out.println(a.insert(27, 666));
		System.out.println(a.insert(440, 231));
		System.out.println(a.insert(490, 333));
		System.out.println(a.insert(550, 432));
		System.out.println(a.insert(600, 454));
		System.out.println(a.insert(650, 466));
		// System.out.println(a.search(550));
		a.rangeSearch(3, 666);
		a.print();
	}
}