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
 * TODO fails to create 2 connections to the same internal instance for testing (vm://)
 * 
 * @author msm336
 *
 */
public class ActiveMQHandler {
	static Logger logger = Logger.getLogger(ActiveMQHandler.class);
	
	protected Connection connection = null;
	protected Session session = null;
	protected Destination destination = null;
	protected String queue = null;
	protected String hostUrl = null;
	
	public String lastStatusMessage = "unknown";
	public int lastStatusCode = 0;
	

	
	public String getStatusString(int statusCode) {
		String result = "unknown";
		if (statusCode > 0) {
			result = "success";
		} else if (statusCode == -1) {
			result = "STATUS:  Unknown error (-1)";
		} else if (statusCode == -2) {
			result = "STATUS:  JMS exception (-2)";
		} else if (statusCode == -4) {
			result = "STATUS:  Session is null (-4)";
		} else if (statusCode == -5) {
			result = "STATUS:  Producer/Consumer is null (-5)";
		} else if (statusCode == -6) {
			result = "STATUS:  No message available (-6)";
		} else if (statusCode == -7) {
			result = "STATUS:  Unable to add message to the poison queue (-7)";
		} else if (statusCode == -8) {
			result = "STATUS:  Next message was not a text message (-8)";
		}
		return result;
	}
	
	

	/**
	 * Find/create a session object
	 * @param url
	 * @return
	 * @throws JMSException
	 */
	protected Session getSession(String url) throws JMSException {
		if (session == null) {
			ConnectionFactory cf = new ActiveMQConnectionFactory(url);
			connection = cf.createConnection();
			connection.start();
				
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		}
		return session;
	}


	
	
	
	/**
	 * Close this consumer.  Clean up memory.
	 */
	public int close() {
		int status = -1;
		
		try {
			
			if (session != null) {
				session.close();
			}
			
			if (connection != null) {
				connection.close();
			}
			status = 1;
		} catch (JMSException e) {
			logger.error("ERROR: Unable to close connection and session for host: " + this.hostUrl, e);
			status = -2;
		}
		
		lastStatusCode = status;
		return status;
	}
}
