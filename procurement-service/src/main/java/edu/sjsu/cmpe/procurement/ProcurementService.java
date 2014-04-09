package edu.sjsu.cmpe.procurement;

import java.util.HashMap;
import edu.sjsu.cmpe.procurement.domain.BookOrders;
import edu.sjsu.cmpe.procurement.domain.ShippedBook;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Destination;
import javax.jms.TextMessage;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.client.JerseyClientBuilder;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;

import de.spinscale.dropwizard.jobs.JobsBundle;
import edu.sjsu.cmpe.procurement.api.resources.RootResource;
import edu.sjsu.cmpe.procurement.config.ProcurementServiceConfiguration;

public class ProcurementService extends
		Service<ProcurementServiceConfiguration> {

	private final Logger log = LoggerFactory.getLogger(getClass());
	private static HashMap<Integer, String> orderList = new HashMap<Integer, String>();
	private static Connection connection;

	/**
	 * FIXME: THIS IS A HACK!
	 */
	public static Client jerseyClient;
	private static String apolloHost;
	private static String apolloPort;
	private static String apolloUser;
	private static String apolloPassword;
	static String queueName;
	private static Session session = null;
	private static MessageConsumer consumer;
	MessageProducer producer;
	private static HashMap<Integer, String> orders = new HashMap<Integer, String>();
	private static String orderedIsbns = null;
	private static BookOrders bookOrders = new BookOrders();
 static String topic;
	public static void main(String[] args) throws Exception {
		new ProcurementService().run(args);
	}

	@Override
	public void initialize(Bootstrap<ProcurementServiceConfiguration> bootstrap) {
		bootstrap.setName("procurement-service");
		/**
		 * NOTE: All jobs must be placed under edu.sjsu.cmpe.procurement.jobs
		 * package
		 */
		bootstrap.addBundle(new JobsBundle("edu.sjsu.cmpe.procurement.jobs"));
	}

	@Override
	public void run(ProcurementServiceConfiguration configuration,
			Environment environment) throws Exception {
		jerseyClient = new JerseyClientBuilder()
				.using(configuration.getJerseyClientConfiguration())
				.using(environment).build();

		/**
		 * Root API - Without RootResource, Dropwizard will throw this
		 * exception:
		 * 
		 * ERROR [2013-10-31 23:01:24,489]
		 * com.sun.jersey.server.impl.application.RootResourceUriRules: The
		 * ResourceConfig instance does not contain any root resource classes.
		 */
		environment.addResource(RootResource.class);

		queueName = configuration.getStompQueueName();
		String topicName = configuration.getStompTopicPrefix();
		log.debug("Queue name is {}. Topic is {}", queueName, topicName);
		// TODO: Apollo STOMP Broker URL and login

		apolloUser = configuration.getApolloUser();
		apolloPassword = configuration.getApolloPassword();
		apolloHost = configuration.getApolloHost();
		apolloPort = configuration.getApolloPort();
		topic = configuration.getStompTopicPrefix();
	}

	public static BookOrders getOrderFromQueue() throws JMSException {
		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
		factory.setBrokerURI("tcp://" + apolloHost + ":" + apolloPort);
		try {
			connection = factory.createConnection(apolloUser, apolloPassword);
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		connection.start();

		System.out.println("connected to " + queueName);
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		Destination dest = new StompJmsDestination(queueName);

		consumer = session.createConsumer(dest);
		System.out.println("Waiting for messages from " + queueName + "...");
		while (true) {

			Message msg = consumer.receive(5000);

			if (msg instanceof TextMessage) {
				String body = ((TextMessage) msg).getText();

				System.out.println("Received message = " + body);
				orderedIsbns = body.substring(10);
				bookOrders.getOrder_book_isbns().add(
						Integer.parseInt(orderedIsbns));

			}
			if (msg == null)
				break;

		}
		//consumer.close();
		//connection.close();
		return bookOrders;
	}

	private static void pushToPublisher(
			HashMap<Integer, String> ordersToPublisher) {
		System.out.println("Request sending to Publisher....");
		Client client = Client.create();
		String url = "http://54.215.133.131:9000/orders";
		WebResource webRes = client.resource(url);
		ClientResponse response = webRes.accept("application/json")
				.type("application/json")
				.entity(ordersToPublisher, "application/json")
				.post(ClientResponse.class);
		System.out.println(response.getEntity(String.class));
		System.out.println("Sent successfully!");
	}

	public void publishTopicMessage() throws JMSException {
		//topicName = .getStompTopicPrefix();

		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		String url = "http://54.215.133.131:9000/orders/78033";
		Client client = Client.create();
    	WebResource webResource = client.resource(url);
    	ShippedBook response = webResource.accept("application/json")
    			.type("application/json").get(ShippedBook.class);
    	StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
       factory.setBrokerURI("tcp://" + apolloHost + ":" +apolloPort);
        
		try {
			connection = factory.createConnection(apolloUser, apolloPassword);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
        for (int isbn_count=0;isbn_count<response.getShipped_books().size();isbn_count++)
        {      

        	Destination dest = new StompJmsDestination(topic + response.getShipped_books().get(isbn_count).getCategory());
        	MessageProducer producer = session.createProducer(dest);
        	producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        	TextMessage msg = session.createTextMessage(response.getShipped_books().get(isbn_count).getIsbn() +  
        			":" + response.getShipped_books().get(isbn_count).getTitle() + 
        			":" + response.getShipped_books().get(isbn_count).getCategory() + 
        			":" + response.getShipped_books().get(isbn_count).getCoverimage());
        	System.out.println(msg);
        	producer.send(msg);
        }
        connection.close();
	}

}
