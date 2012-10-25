package edu.cornell.clo.r.message_queue;

public interface Producer {
	
	public int open(String url, String topic);
	public int close();
	
	public int putText(String message);
}
