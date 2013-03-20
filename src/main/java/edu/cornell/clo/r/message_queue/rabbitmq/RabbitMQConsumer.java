package edu.cornell.clo.r.message_queue.rabbitmq;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

import edu.cornell.clo.r.message_queue.Consumer;
import edu.cornell.clo.r.message_queue.STextMessage;

public class RabbitMQConsumer extends RabbitMQHandler implements Consumer {
	static Logger logger = Logger.getLogger(RabbitMQConsumer.class);

	QueueingConsumer queueingConsumer = null;

	/**
	 * Retrieve the next text message.
	 */
	public STextMessage getNextText() {
		STextMessage result = null;
		
		try {
			
			Delivery d = queueingConsumer.nextDelivery(50);
			
			result = new STextMessage();
			result.value = new String(d.getBody());
			result.correlationId = d.getProperties().getCorrelationId();
			result.replyTo = d.getProperties().getReplyTo();
		} catch (InterruptedException e) {
			result = null;
			logger.error("ERROR: Unable to retrieve a message from this queue: " + queue, e);
		}
		
		return result;
	}
	

	public int open(String url, String queue) {
		int status = super.open(url, queue);
		queueingConsumer = new QueueingConsumer(channel);
		
		try {
			channel.basicConsume(queue, true, queueingConsumer);
		} catch (IOException e) {
			logger.error("ERROR: Unable to create connection to queue: " + queue, e);
			status = -2;
		}
		return status;
	}

	public int close() {
		queueingConsumer = null;
		return super.close();
	}

}
