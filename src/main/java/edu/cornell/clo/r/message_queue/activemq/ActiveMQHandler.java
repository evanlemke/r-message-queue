package edu.cornell.clo.r.message_queue.activemq;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;

/**
 * Helper methods.
 * 
 * Not very well organized, but I wanted all the opening/closing of connections to be in one place.
 * 
 * @author msm336
 *
 */
public class ActiveMQHandler {
	static Logger logger = Logger.getLogger(ActiveMQHandler.class);
	
	protected MessageConsumer consumer = null;
	protected MessageProducer producer = null;
	protected Connection connection = null;
	protected Session session = null;
	protected String queue = null;

	
	/**
	 * Open a connection to this queue.
	 */
	public int openConsumer(String url, String queue) {
		int status = -1;
		this.queue = queue;
		
		ConnectionFactory cf = new ActiveMQConnectionFactory(url);
		try {
			connection = cf.createConnection();
			connection.start();
			
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination destination = session.createQueue(queue);
			consumer = session.createConsumer(destination);
		
			status = 1;
		} catch (JMSException e) {
			logger.error("ERROR: Unable to create connection to activemq queue: " + queue, e);
			status = -2;
		}
		return status;
	}
	
	/**
	 * Open a connection to this queue.
	 */
	public int openProducer(String url, String queue) {
		int status = -1;
		this.queue = queue;
		
		ConnectionFactory cf = new ActiveMQConnectionFactory(url);
		try {
			connection = cf.createConnection();
			connection.start();
			
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			Destination destination = session.createQueue(queue);
			producer = session.createProducer(destination);
		
			status = 1;
		} catch (JMSException e) {
			logger.error("ERROR: Unable to create connection to queue: " + queue, e);
			status = -2;
		}
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
			if (producer != null) {
				producer.close();
			}
			
			if (session != null) {
				session.close();
			}
			
			if (connection != null) {
				connection.close();
			}
			status = 1;
		} catch (JMSException e) {
			logger.error("ERROR: Unable to close connection for queue: " + this.queue, e);
			status = -2;
		}
		
		return status;
	}
}
