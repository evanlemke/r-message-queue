package edu.cornell.clo.r.message_queue.rabbitmq;

import java.io.IOException;

import com.rabbitmq.client.QueueingConsumer;

import edu.cornell.clo.r.message_queue.Consumer;

public class RabbitMQConsumer extends RabbitMQHandler implements Consumer {

	QueueingConsumer queueingConsumer = null;

	/**
	 * Retrieve the next text message.
	 */
	public String getNextText() {
		String result = null;
		
		if (queueingConsumer == null) {
			queueingConsumer = new QueueingConsumer(channel);
		}
		
		try {
			result = channel.basicConsume(queue, true, queueingConsumer);
		} catch (IOException e) {
			logger.error("ERROR: Unable to retrieve a message from this queue: " + queue, e);
		}
		
		return result;
	}

	public int open(String url, String queue) {
		return super.open(url, queue);
	}

	public int close() {
		return super.close();
	}

}
