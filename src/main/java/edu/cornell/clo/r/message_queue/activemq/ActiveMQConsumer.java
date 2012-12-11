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
//			Message message = consumer.receive();
			if (message != null) {
				
				// is it a text message?
				if (message instanceof TextMessage) {
					TextMessage tmessage = (TextMessage) message;
					lastStatusMessage = "message received";
					results = tmessage.getText();
					lastStatusCode = 1;
				
				// wrong kind of message.. rollback/skip
				} else {
					lastStatusMessage = "WARNING: Adding to poison/DLQ because it isn't a text message: " + message.getJMSType() + ", id: " + message.getJMSMessageID();
					lastStatusCode = -8;
					logger.debug(lastStatusMessage);
					if (session != null) {
						session.rollback();
					} else {
						lastStatusMessage = "WARNING: Unable to adding to poison/DLQ (rollback) because it isn't a text message: " + message.getJMSType() + ", id: " + message.getJMSMessageID();
						lastStatusCode = -7;
					}
					
					// check the next message recursively
					results = getNextText();
				}
			} else {
				lastStatusMessage = "No message available";
				logger.debug(lastStatusMessage);
				lastStatusCode = -6;
			}
		} catch (JMSException e) {
			lastStatusMessage = "ERROR: Unable to retrieve a message from this queue: " + queue + ", " + e.getMessage();
			lastStatusCode = -2;
			logger.error(lastStatusMessage, e);
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
