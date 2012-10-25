package edu.cornell.clo.r.message_queue;

import org.apache.commons.lang3.StringUtils;


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
	public Consumer getConsumerFor(String url, String topic, String queueType) {
		Consumer consumer = null;
		if (StringUtils.equals(QUEUE_TYPE_RABBITMQ, queueType)) {
			
		} else if (StringUtils.equals(QUEUE_TYPE_ACTIVEMQ, queueType)) {
			
		}
		return consumer;
	}
	
	public static Producer getProducerFor(String url, String topic, String queueType) {
		Producer producer = null;
		if (StringUtils.equals(QUEUE_TYPE_RABBITMQ, queueType)) {
			
		} else if (StringUtils.equals(QUEUE_TYPE_ACTIVEMQ, queueType)) {
			
		}
		return producer;
	}

}
