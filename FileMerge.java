package com.aliyun.odps.mapred;

import java.io.*;
import java.util.*;

public class FileMerge {

	public class Record implements Comparable {
		private String s;
		private int i;
		
		public Record(String _s, int _i) {
			this.s = _s;
			this.i = _i;
		}
		
		public int compareTo(Object o) {
			Record r = (Record)o;
			if (this.s.compareTo(r.s) != 0) {
				return this.s.compareTo(r.s);
			}
			if (this.i < r.i) return -1;
			if (this.i > r.i) return 1;
			return 0;
		}
		
		public String toString() {
			return s + " " + i;
		}
	}
	
	public void fileMerge(String[] filename) throws IOException {
		int n = filename.length;
		BufferedReader b[] = new BufferedReader[n];
        BufferedWriter bw = new BufferedWriter(new FileWriter("reduceInput"));
		for (int i = 0; i < n; i ++) {
			b[i] = new BufferedReader(new FileReader(filename[i]));
		}
		Queue<Record> priorityQueue = new PriorityQueue<Record>();
		for (int i = 0; i < n; i ++) {
			String s = b[i].readLine();
			priorityQueue.add(new Record(s, i));
		}
		while (!priorityQueue.isEmpty()) {
			Record r = priorityQueue.poll();
			//System.out.println(r.s);
            bw.write(r.s+"\n");
            //bw.flush();
			String s = b[r.i].readLine();
			if (s != null) {
				priorityQueue.add(new Record(s, r.i));
			}
		}
        bw.flush();
	}
	
	public static void main(String[] argc) throws IOException {
		int n = argc.length;
		for (int i = 0; i < n; i ++) {
			System.out.println(argc[i]);
		}
		FileMerge f = new FileMerge();
		f.fileMerge(argc);
	}
	
}
