package edu.cornell.clo.r.message_queue;

import org.apache.commons.lang3.StringUtils;

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
	
	public static final String QUEUE_TYPE_RABBITMQ = "rabbitmq";
	public static final String QUEUE_TYPE_ACTIVEMQ = "activemq";
	
	
	
	public MessageQueueFactory() { }
	
	

	/**
	 * This should be a static method, but I'm not sure how to get R to call a static method.
	 * @param url
	 * @param topic
	 * @param queueType
	 * @return
	 */
	public Consumer getConsumerFor(String url, String queueName, String queueType) {
		Consumer consumer = null;
		if (StringUtils.equals(QUEUE_TYPE_RABBITMQ, queueType)) {
			consumer = new RabbitMQConsumer();
			consumer.open(url, queueName);
		} else if (StringUtils.equals(QUEUE_TYPE_ACTIVEMQ, queueType)) {
			consumer = new ActiveMQConsumer();
			consumer.open(url, queueName);
		}
		return consumer;
	}
	
	public static Producer getProducerFor(String url, String queueName, String queueType) {
		Producer producer = null;
		if (StringUtils.equals(QUEUE_TYPE_RABBITMQ, queueType)) {
			producer = new RabbitMQProducer();
			producer.open(url, queueName);
		} else if (StringUtils.equals(QUEUE_TYPE_ACTIVEMQ, queueType)) {
			producer = new ActiveMQProducer();
			producer.open(url, queueName);
		}
		return producer;
	}

}
