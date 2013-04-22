package edu.cornell.clo.r.message_queue;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple text message.  Very basic as you can see.
 * @author msm336
 *
 */
public class STextMessage {
	
	public static String JMS_MESSAGE_ID = "jms-message-id";
	public static String JMS_TIMESTAMP = "jms-timestamp";
	
	public String value;
	public String correlationId;
	public String replyTo;
	
	public Map<String, String> properties = new HashMap<String, String>();;
}
