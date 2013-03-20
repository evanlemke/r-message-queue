package edu.cornell.clo.r.message_queue.activemq;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.TextMessage;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.log4j.Logger;

import edu.cornell.clo.r.message_queue.Consumer;
import edu.cornell.clo.r.message_queue.MessageQueueFactory;
import edu.cornell.clo.r.message_queue.Producer;

public class ActiveMQProducer extends ActiveMQHandler implements Producer {
	static Logger logger = Logger.getLogger(ActiveMQProducer.class);
	
	protected MessageProducer producer = null;

	/**
	 * Create and put a message on the queue
	 */
	public int putText(String message) {
		return putText(message, null, null);
	}
	
	/**
	 * Create and put a message on the queue.
	 */
	public int putText(String message, String correlationId, String replyTo) {
		int result = -1;
		
		try {
			if (session != null) {
				TextMessage tmessage = session.createTextMessage(message);
	
			
				
				// set the correlation id if it was passed
				if (correlationId != null && !correlationId.isEmpty()) {
					tmessage.setJMSCorrelationID(correlationId);
				}
			
				// set the replyto queue if it was passed
				if (replyTo != null && !replyTo.isEmpty()) {
					tmessage.setJMSReplyTo(new ActiveMQQueue(replyTo));
					logger.debug("replyTo set: " + replyTo + ", get: " + tmessage.getJMSReplyTo());
				}
			
				
				
				
				if (producer != null) {
					producer.send(tmessage);
					result = 1;
					lastStatusMessage = "message sent";
				} else {
					result = -5;
					lastStatusMessage = "ERROR: producer is null";
				}
			} else {
				result = -4;
				lastStatusMessage = "ERROR: session is null";
			}
		} catch (JMSException e) {
			lastStatusMessage = "ERROR: Unable to create/send text message to queue: " + queue + ", " + e.getMessage();
			logger.error(lastStatusMessage, e);
			result = -2;
		}
		lastStatusCode = result;
		return result;
	}

	
	
	/**
	 * Open a producer connection to this queue
	 */
	public int open(String url, String queue) {
		int status = -1;
		this.queue = queue;
		this.hostUrl = url;
			
		try {
			session = getSession(url);
			Destination destination = session.createQueue(queue);
			producer = session.createProducer(destination);
			
			status = 1;
		} catch (JMSException e) {
			logger.error("ERROR: Unable to create connection to activemq queue: " + queue, e);
			status = -2;
		}
			
		lastStatusCode = status;
		return status;
	}

	
	/**
	 * Close this producer
	 */
	public int close() {
		int status = -1;
		
		try {
			if (producer != null) {
				producer.close();
			}
			status = super.close();
		} catch (JMSException e) {
			logger.error("ERROR: Unable to close connection for host: " + this.hostUrl + ", to producer queue: " + this.queue, e);
			status = -2;
		}
		
		return status;
	}

}
