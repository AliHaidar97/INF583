package inf583.wordcount_spark;

import java.io.FileNotFoundException;

public class PartB_1_Threads {

	public static int getMostImportantElementUsingMatrixMutliplicationUsingThreads(int iteration, int numberOfElements)
			throws FileNotFoundException, CloneNotSupportedException {

		SharedVector r = new SharedVector(numberOfElements);
		SharedVector newR = new SharedVector(numberOfElements);

		final int nofThreads = 4;
		String[] files = new String[nofThreads];
		Thread[] threads = new Thread[4];
		for (int it = 0; it < iteration; it++) {
			for (int i = 0; i < nofThreads; i++) {
				files[i] = "tags-" + (i + 1) + ".txt";
				threads[i] = new Thread(new MatrixMultiplicationThread(files[i], r, newR));
			}
			for (int i = 0; i < nofThreads; i++) {
				threads[i].start();
			}
			for (int i = 0; i < nofThreads; i++) {
				try {
					threads[i].join();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			newR.normalize();
			for (int i = 0; i < numberOfElements; i++)
				r.set(i, newR.get(i));
		}
		int is = 0;
		for (int i = 0; i < numberOfElements; i++) {
			if (r.get(is) < r.get(i)) {
				is = i;
			}
		}
		return is;
	}

	public static void main(String args[]) throws FileNotFoundException, CloneNotSupportedException {
		int it = 6;
		System.out.println("The most important nodes_" + it + "="
				+ getMostImportantElementUsingMatrixMutliplicationUsingThreads(it, 64375));
	}

}
