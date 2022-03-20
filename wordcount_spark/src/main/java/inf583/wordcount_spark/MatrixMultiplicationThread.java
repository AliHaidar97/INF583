package inf583.wordcount_spark;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

public class MatrixMultiplicationThread implements Runnable {
    private Scanner sc;
    private SharedVector newR;
    private SharedVector r;
  

    public MatrixMultiplicationThread(String filename,SharedVector r, SharedVector newR) throws FileNotFoundException {
    	File file = new File(filename);
    	sc = new Scanner(file);
        this.r = r;
        this.newR = newR;
      
    }

    @Override
    public void run() {
        
        while (sc.hasNextLine()) {
        	String[] contents = (sc.nextLine()).split(" "); 
        	double result = 0;
        	int row =  Integer.parseInt(contents[0]);
        	for(int col = 1;col<contents.length;col++) {
        		result += r.get(Integer.parseInt(contents[col]));
        	}
        	newR.set(row, result);
        }
    }
}
