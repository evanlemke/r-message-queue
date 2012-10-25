package edu.cornell.clo.r.message_queue.rabbitmq;

import java.io.IOException;

import org.apache.log4j.Logger;

import edu.cornell.clo.r.message_queue.Producer;

public class RabbitMQProducer extends RabbitMQHandler implements Producer {
	static Logger logger = Logger.getLogger(RabbitMQProducer.class);

	/**
	 * Put a text message on the queue
	 */
	public int putText(String message) {
		int status = -1;
		try {
			channel.basicPublish(DEFAULT_EXCHANGE, DEFAULT_ROUTING, null, message.getBytes());
			status = 1;
		} catch (IOException e) {
			logger.error("ERROR: Unable to create/send text message to queue: " + queue, e);
			status = -2;
		}
		return status;
	}

	public int open(String url, String queue) {
		return super.open(url, queue);
	}

	public int close() {
		return super.close();
	}

}
