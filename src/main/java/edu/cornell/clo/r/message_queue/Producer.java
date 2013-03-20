package edu.cornell.clo.r.message_queue;

public interface Producer {
	
	/**
	 * Open a queue at the following url.
	 * @param url
	 * @param queue
	 * @return
	 */
	public int open(String url, String queue);
	
	/**
	 * Close this queue connection and cleanup.
	 * @return
	 */
	public int close();

	/**
	 * Put the following text message into the queue
	 * @param message
	 * @return
	 */
	public int putText(String message);
	
	/**
	 * Put the following tedt message into the queue.
	 * Also sets the correlationId and replyTo header values.
	 * @param message
	 * @param correlationId
	 * @param replyToQueue
	 * @return
	 */
	public int putText(String message, String correlationId, String replyToQueue);
}
