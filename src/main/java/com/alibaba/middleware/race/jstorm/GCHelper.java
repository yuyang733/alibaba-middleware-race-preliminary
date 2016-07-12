package com.alibaba.middleware.race.jstorm;

import java.util.Queue;
import java.util.concurrent.Callable;
//复制释放

public class GCHelper<T> implements Callable<Boolean> {
	
	private Queue<T> queue = null;
	private int releaseThreshold = 0;							
	
	public GCHelper(Queue<T> queue, int releaseThreshold){
		this.queue = queue;
		this.releaseThreshold = releaseThreshold;
	}
	
	@Override
	public Boolean call() throws Exception {
		
		if(this.queue == null){
			return false;
		}
		
		if(this.queue.size() < this.releaseThreshold){
			return true;
		}
		
		for(int i =0; i != this.queue.size()/2; i++){
			this.queue.remove();							//迭代删除队头元素
		}
		
		return true;
	}

}
