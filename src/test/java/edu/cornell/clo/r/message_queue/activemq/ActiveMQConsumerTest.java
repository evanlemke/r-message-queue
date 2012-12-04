package edu.cornell.clo.r.message_queue.activemq;

import javax.jms.ConnectionFactory;

import org.apache.log4j.Logger;

import edu.cornell.clo.r.message_queue.Consumer;
import edu.cornell.clo.r.message_queue.MessageQueueFactory;
import edu.cornell.clo.r.message_queue.Producer;
import junit.framework.Assert;
import junit.framework.TestCase;

public class ActiveMQConsumerTest extends TestCase {
	static Logger logger = Logger.getLogger(ActiveMQConsumerTest.class);
	
	private static final String queueName = "junit-test-queue";
	
	/** special local URL to enable testing **/
	private static final String queueUrl = "vm:localhost?broker.persistent=false";
	//private static final String queueUrl = "tcp://localhost:61616";
	
	private ConnectionFactory connectionFactory = null;

	public void setup() {
	}
	
	public void tearDown() {
	}
	
	public void testClose() {
		Consumer consumer = MessageQueueFactory.getConsumerFor(queueUrl, queueName, "activeMQ");
		int result = consumer.close();
		Assert.assertEquals("Error closing consumer, status returned: " + result, 1, result);
	}

	public void testGetNextTextEmpty() {
		Consumer consumer = MessageQueueFactory.getConsumerFor(queueUrl, queueName, "activeMQ");
		String message = consumer.getNextText();
		int result = consumer.close();
		Assert.assertNull("Message should be null, as no message exists", message);
	}
	
	public void testGetNextTextNotEmpty() throws InterruptedException {
		// clear out the queue, ensure it's empty
		Consumer consumer = MessageQueueFactory.getConsumerFor(queueUrl, queueName, "activeMQ");
		while (consumer.getNextText() != null) { }
		consumer.close();
		
	
		// send a message
		Producer producer = MessageQueueFactory.getProducerFor(queueUrl, queueName, "activeMQ");
		String message = "<currentTime>" + System.currentTimeMillis() + "</currentTime>";
		int sendStatus = producer.putText(message);
		int status = producer.close();
		Assert.assertEquals("Message was not sent", 1, sendStatus);
	
	
		// reconnect to the queue
		consumer = MessageQueueFactory.getConsumerFor(queueUrl, queueName, "activeMQ");
		// wait for the message to arrive
		Thread.sleep(2000);
		
		// loop until we get a message
		String received = consumer.getNextText();
		int count = 0;
		while (received == null && count < 10) {
			Thread.sleep(500);
			count++;
			received = consumer.getNextText();
			logger.debug("waiting, received: " + received);
		}
		status = consumer.close();
		Assert.assertEquals("Message should be equal", message, received);
	}

	public void testOpen() {
		Consumer consumer = MessageQueueFactory.getConsumerFor(queueUrl, queueName, "activeMQ");
		int result = consumer.close();
		Assert.assertNotNull("Consumer is null for url : " + queueUrl, consumer);
	}

}
