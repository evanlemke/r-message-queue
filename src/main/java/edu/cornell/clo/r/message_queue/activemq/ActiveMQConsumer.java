package edu.cornell.clo.r.message_queue.activemq;

import java.util.Enumeration;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.log4j.Logger;

import edu.cornell.clo.r.message_queue.Consumer;
import edu.cornell.clo.r.message_queue.STextMessage;

public class ActiveMQConsumer extends ActiveMQHandler implements Consumer {
	static Logger logger = Logger.getLogger(ActiveMQConsumer.class);
	
	protected MessageConsumer consumer;
	

	/**
	 * Pull the next text message off the queue.
	 * Non blocking
	 * 
	 * If the next message isn't a text message, roll it back.
	 */
	public STextMessage getNextText() {
		logger.debug("  getNextText() - begin");
		STextMessage result = null;
		
		try {
			// is there a message ready/waiting?
			Message message = consumer.receiveNoWait();
			if (message != null) {
				
				// is it a text message?
				if (message instanceof TextMessage) {
					logger.debug("    received a text message");
					TextMessage tmessage = (TextMessage) message;
					lastStatusMessage = "message received";
					
					result = new STextMessage();
					result.value = tmessage.getText();
			
					// get a list of all properties..
					Enumeration names = tmessage.getPropertyNames();
					if (names.hasMoreElements()) {
						logger.debug("    properties to set");
					} else {
						logger.debug("    no properties to set");
					}
					while (names.hasMoreElements()) {
						String propName = (String)names.nextElement();
						String propValue = String.valueOf(tmessage.getObjectProperty(propName));
						logger.debug("    setting property '" + propName + "', value: '" + propValue + "'");
						result.properties.put(propName, propValue);
					}
					result.properties.put(STextMessage.JMS_MESSAGE_ID, tmessage.getJMSMessageID());
					result.properties.put(STextMessage.JMS_TIMESTAMP, String.valueOf(tmessage.getJMSTimestamp()));
					
					
					ActiveMQDestination dest = (ActiveMQDestination) tmessage.getJMSReplyTo();
					if (dest != null) {
						result.replyTo = dest.getPhysicalName();
						logger.debug("    replyTo: " + result.replyTo);
					}
					result.correlationId = tmessage.getJMSCorrelationID();
					
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
					result = getNextText();
				}
			} else {
				lastStatusMessage = "No message available";
				logger.debug("    " + lastStatusMessage);
				lastStatusCode = -6;
			}
		} catch (JMSException e) {
			lastStatusMessage = "ERROR: Unable to retrieve a message from this queue: " + queue + ", " + e.getMessage();
			lastStatusCode = -2;
			logger.error("    " + lastStatusMessage, e);
		}
		
		logger.debug("  getNextText() - end");
		return result;
	}

	
	/**
	 * Open a connection to this queue.
	 */
	public int open(String url, String queue) {
		int status = -1;
		this.queue = queue;
		this.hostUrl = url;
		
		try {
			session = getSession(url);
			Destination destination = session.createQueue(queue);
			consumer = session.createConsumer(destination);
		
			status = 1;
		} catch (JMSException e) {
			logger.error("ERROR: Unable to create connection to queue: " + queue, e);
			status = -2;
		}
		lastStatusCode = status;
		return status;
	}
	

	
	/**
	 * Close this consumer.  Clean up memory.
	 */
	public int close() {
		int status = -1;
		
		try {
			if (consumer != null) {
				consumer.close();
			}
			status = super.close();
		} catch (JMSException e) {
			logger.error("ERROR: Unable to close connection for host: " + this.hostUrl + ", to consumer queue: " + this.queue, e);
			status = -2;
		}
		
		lastStatusCode = status;
		return status;
	}

}
