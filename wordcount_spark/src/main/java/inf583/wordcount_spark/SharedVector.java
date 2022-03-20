package inf583.wordcount_spark;

import java.util.ArrayList;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SharedVector {
    private ArrayList<Double> r;
    private Lock lock;

    public  SharedVector() {
        r = new ArrayList<Double>();
        lock = new ReentrantLock();
        
    }
    public  SharedVector(int n) {
        r = new ArrayList<Double>(n);
        lock = new ReentrantLock();
        for(int i=0;i<n;i++) {
        	r.set(i, 1.0/n);
        }
    }

    public void set(int index,double value) {
        lock.lock();
        r.set(index, value);
        lock.unlock();
    }

    public double get(int index) {
    	lock.lock();
    	double result = r.get(index);
    	lock.unlock();
    	return result;
    }
    
    public void printResult() {
        for(Double val :r) {
        	System.out.println(val);
        }
    }
    
    public void normalize() {
    	double norm = 0.0;
    	for(Double val:r) {
    		norm+=val*val;
    	}
    	for(int i=0;i<r.size();i++) {
    		r.set(i, r.get(i)/norm);
    	}
    }

    
    public Object clone() throws CloneNotSupportedException
    {
        // Assign the shallow copy to
        // new reference variable t
    	SharedVector t = (SharedVector)super.clone();
    	t.r= (ArrayList<Double>) r.clone();
    	
        return t;
    }
}
