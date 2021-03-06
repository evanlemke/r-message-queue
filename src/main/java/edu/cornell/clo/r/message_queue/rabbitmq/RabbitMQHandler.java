package edu.cornell.clo.r.message_queue.rabbitmq;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.AMQP.Queue;

public class RabbitMQHandler {
	static Logger logger = Logger.getLogger(RabbitMQHandler.class);

	/* as noted in the rabbitmq documentation */
	public static final String DEFAULT_EXCHANGE = "";
	public static final String DEFAULT_ROUTING = "";
	
	protected Connection connection = null;
	protected Channel channel = null;
	protected String queue = null;
	protected String hostUrl = null;

	
	/**
	 * Open a connection to the given queue.
	 * @param url
	 * @param queue
	 * @return
	 */
	public int open(String url, String queue) {
		return open(url, queue, null);
	}

	
	/**
	 * Open a connection to the given queue.
	 * @param url
	 * @param queue
	 * @return
	 */
	public int open(String url, String queue, String topic) {
		int status = -1;
		
		this.hostUrl = url;
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(url);
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
	
			// unsupported at the moment, but it's here
			if (!StringUtils.isEmpty(topic)) {
				channel.exchangeDeclare(topic, "topic");
			}
			
			// ensure the queue exists..
			Queue.DeclareOk result = channel.queueDeclare(queue, true, false, false, null);
			if (result != null) {
				String queueName = result.getQueue();
				
				// ensure we only retrieve 1 message at a time
				channel.basicQos(1);
				
				//channel.queueBind(queueName, DEFAULT_EXCHANGE, DEFAULT_ROUTING);
				status = 1;
			} else {
				status = -3;
			}
		} catch (IOException e) {
			logger.error("ERROR: Unable to create connection to rabbitmq queue: " + queue, e);
			e.printStackTrace();
			status = -2;
		}
		
		return status;
	}

	
	/**
	 * Close and release memory.
	 * @return
	 */
	public int close() {
		int status = -1;
	
		try {
			if (channel != null) {
				channel.close();
			}
			
			if (connection != null) {
				connection.close(500);
			}
			status = 1;
		} catch (IOException e) {
			logger.error("ERROR: Unable to close connection for queue: " + this.queue, e);
			status = -2;
		}
		return status;
	}
}
