package edu.cornell.clo.r.message_queue;

public interface Consumer {

	public int open(String url, String queue);
	public int close();
	

	/**
	 * Non blocking call that looks for the next text message.
	 * @return
	 */
	public STextMessage getNextText();

}
