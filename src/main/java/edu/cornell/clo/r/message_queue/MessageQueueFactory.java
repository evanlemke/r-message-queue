package edu.cornell.clo.r.message_queue;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import edu.cornell.clo.r.message_queue.activemq.ActiveMQConsumer;
import edu.cornell.clo.r.message_queue.activemq.ActiveMQProducer;
import edu.cornell.clo.r.message_queue.rabbitmq.RabbitMQConsumer;
import edu.cornell.clo.r.message_queue.rabbitmq.RabbitMQProducer;


/**
 * Create an instance of any flavor of queue.
 * 
 * @author msm336
 */
public class MessageQueueFactory {
	static Logger logger = Logger.getLogger(MessageQueueFactory.class);
	
	public static final String QUEUE_TYPE_RABBITMQ = "rabbitmq";
	public static final String QUEUE_TYPE_ACTIVEMQ = "activemq";
	
	
	
	public MessageQueueFactory() { }
	public static int lastStatusCode = 0;
	
	

	/**
	 * This should be a static method, but I'm not sure how to get R to call a static method.
	 * @param url
	 * @param topic
	 * @param queueType
	 * @return
	 */
	public static Consumer getConsumerFor(String url, String queueName, String queueType) {
		logger.debug("getConsumerFor('" + url + "', '" + queueName + "', '" + queueType + "')");
		Consumer consumer = null;
		
		if (StringUtils.isEmpty(queueType)) {
			logger.error("getConsumerFor() - queueType must be specified.");
			
		} else if (QUEUE_TYPE_RABBITMQ.equalsIgnoreCase(queueType)) {
			consumer = new RabbitMQConsumer();
			MessageQueueFactory.lastStatusCode = consumer.open(url, queueName);
			
		} else if (QUEUE_TYPE_ACTIVEMQ.equalsIgnoreCase(queueType)) {
			consumer = new ActiveMQConsumer();
			MessageQueueFactory.lastStatusCode = consumer.open(url, queueName);
			
		} else {
			logger.error("getConsumerFor() - Unsupported queue type: " + queueType);
		}
		if (MessageQueueFactory.lastStatusCode < 0) {
			consumer = null;
		}
		return consumer;
	}
	
	public static Producer getProducerFor(String url, String queueName, String queueType) {
		logger.debug("getProducerFor('" + url + "', '" + queueName + "', '" + queueType + "')");
		Producer producer = null;
		
		if (StringUtils.isEmpty(queueType)) {
			logger.error("getProducerFor() - queueType must be specified.");
			
		} else if (QUEUE_TYPE_RABBITMQ.equalsIgnoreCase(queueType)) {
			producer = new RabbitMQProducer();
			MessageQueueFactory.lastStatusCode = producer.open(url, queueName);
			
		} else if (QUEUE_TYPE_ACTIVEMQ.equalsIgnoreCase(queueType)) {
			producer = new ActiveMQProducer();
			MessageQueueFactory.lastStatusCode = producer.open(url, queueName);
			
		} else {
			logger.error("getProducerFor() - Unsupported queue type: " + queueType);
		}
		if (MessageQueueFactory.lastStatusCode < 0) {
			producer = null;
		}
		return producer;
	}

}
