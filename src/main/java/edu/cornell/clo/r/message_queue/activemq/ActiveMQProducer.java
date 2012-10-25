package edu.cornell.clo.r.message_queue.activemq;

import javax.jms.JMSException;
import javax.jms.TextMessage;

import org.apache.log4j.Logger;

import edu.cornell.clo.r.message_queue.Producer;

public class ActiveMQProducer extends ActiveMQHandler implements Producer {
	static Logger logger = Logger.getLogger(ActiveMQProducer.class);

	/**
	 * Create and put a message on the queue.
	 */
	public int putText(String message) {
		int result = -1;
		
		try {
			TextMessage tmessage = session.createTextMessage(message);
			producer.send(tmessage);
			result = 1;
		} catch (JMSException e) {
			logger.error("ERROR: Unable to create/send text message to queue: " + queue, e);
			result = -2;
		}
		return result;
	}

	
	
	public int open(String url, String queue) {
		return super.openProducer(url, queue);
	}

	public int close() {
		return super.close();
	}

}
