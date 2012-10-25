package edu.cornell.clo.r.message_queue.activemq;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import org.apache.log4j.Logger;

import edu.cornell.clo.r.message_queue.Consumer;

public class ActiveMQConsumer extends ActiveMQHandler implements Consumer {
	static Logger logger = Logger.getLogger(ActiveMQConsumer.class);
	

	

	/**
	 * Pull the next text message off the queue.
	 * 
	 * If the next message isn't a text message, roll it back.
	 */
	public String getNextText() {
		String results = null;
		
		try {
			// is there a message ready/waiting?
			Message message = consumer.receiveNoWait();
			if (message != null) {
				
				// is it a text message?
				if (message instanceof TextMessage) {
					TextMessage tmessage = (TextMessage) message;
					results = tmessage.getText();
				
				// wrong kind of message.. rollback/skip
				} else {
					logger.warn("WARNING: Adding to poison/DLQ because it isn't a text message: " + message.getJMSType() + ", id: " + message.getJMSMessageID());
					session.rollback();
					
					// check the next message recursively
					results = getNextText();
				}
			} else {
				logger.debug("No message available.");
			}
		} catch (JMSException e) {
			logger.error("ERROR: Unable to retrieve a message from this queue: " + queue, e);
			e.printStackTrace();
		}
		
		return results;
	}

	
	/**
	 * Open a connection to this queue.
	 */
	public int open(String url, String queue) {
		return super.openConsumer(url, queue);
	}
	

	/**
	 * Close this consumer.  Clean up memory.
	 */
	public int close() {
		return super.close();
	}

}
