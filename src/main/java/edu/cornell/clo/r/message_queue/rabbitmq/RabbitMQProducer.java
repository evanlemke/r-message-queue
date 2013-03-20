package edu.cornell.clo.r.message_queue.rabbitmq;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.rabbitmq.client.AMQP.BasicProperties;

import edu.cornell.clo.r.message_queue.Producer;

public class RabbitMQProducer extends RabbitMQHandler implements Producer {
	static Logger logger = Logger.getLogger(RabbitMQProducer.class);

	/**
	 * Put a text message on the queue
	 */
	public int putText(String message) {
		return putText(message, null, null);
	}
	
	/**
	 * Put a text message on the queue
	 */
	public int putText(String message, String correlationId, String replyToQueue) {
		int status = -1;
		try {
			
			BasicProperties bp = new BasicProperties();
		
			if (correlationId != null && !correlationId.isEmpty()) {
				// deprecated, but doesn't tell you what to use instead?  wtf?
				bp.setCorrelationId(correlationId);
			}
			
			if (replyToQueue != null && !replyToQueue.isEmpty()) {
				// deprecated, but doesn't tell you what to use instead?  wtf?
				if (this.hostUrl.endsWith("/")) {
					bp.setReplyTo(this.hostUrl + replyToQueue);
				} else {
					bp.setReplyTo(this.hostUrl + "/" + replyToQueue);
				}
			}
			
			channel.basicPublish(DEFAULT_EXCHANGE, DEFAULT_ROUTING, bp, message.getBytes());
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
