package edu.sjsu.cmpe.library;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.fusesource.stomp.jms.*;
import org.slf4j.LoggerFactory;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.assets.AssetsBundle;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.views.ViewBundle;

import edu.sjsu.cmpe.library.api.resources.BookResource;
import edu.sjsu.cmpe.library.api.resources.RootResource;
import edu.sjsu.cmpe.library.config.LibraryServiceConfiguration;
import edu.sjsu.cmpe.library.repository.BookRepository;
import edu.sjsu.cmpe.library.repository.BookRepositoryInterface;
import edu.sjsu.cmpe.library.ui.resources.HomeResource;

public class LibraryService extends Service<LibraryServiceConfiguration> {

	private final Logger log = LoggerFactory.getLogger(getClass());
	public static String apolloUser = null;
	public static String apolloPassword = null;
	public static String apolloHost = null;
	public static String apolloPort = null;
	public static String libraryName = null;
	private static Connection connection;

	public static void main(String[] args) throws Exception {
		new LibraryService().run(args);
	}

	@Override
	public void initialize(Bootstrap<LibraryServiceConfiguration> bootstrap) {
		bootstrap.setName("library-service");
		bootstrap.addBundle(new ViewBundle());
		bootstrap.addBundle(new AssetsBundle());
	}

	@Override
	public void run(LibraryServiceConfiguration configuration,
			Environment environment) throws Exception {
		BookRepositoryInterface bookRepository = new BookRepository();
		backgroundTask(configuration, bookRepository);
		// This is how you pull the configurations from library_x_config.yml
		String queueName = configuration.getStompQueueName();
		String topicName = configuration.getStompTopicName();
		log.debug("{} - Queue name is {}. Topic name is {}",
				configuration.getLibraryName(), queueName, topicName);
		// TODO: Apollo STOMP Broker URL and login

		apolloUser = configuration.getApolloUser();
		apolloPassword = configuration.getApolloPassword();
		apolloHost = configuration.getApolloHost();
		apolloPort = configuration.getApolloPort();
		libraryName = configuration.getLibraryName();
		/** Root API */
		environment.addResource(RootResource.class);
		/** Books APIs */
		environment.addResource(new BookResource(bookRepository));

		/** UI Resources */
		environment.addResource(new HomeResource(bookRepository));
	}

	private void backgroundTask(final LibraryServiceConfiguration configuration,
			final BookRepositoryInterface bookRepository) {
		int numThreads = 1;
		ExecutorService executor = Executors.newFixedThreadPool(numThreads);
		Runnable bTask = new Runnable() {

			@Override
			public void run() {
				try {
					Listener.pubsubListener(configuration, bookRepository);
				} catch (JMSException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		};
		
		executor.execute(bTask);
		System.out.println("Submitted the background task..");
		
		executor.shutdown();
    	System.out.println("Finished the background task");

	}

	public static void orderBook(Long isbn) throws JMSException {
		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
		factory.setBrokerURI("tcp://" + apolloHost + ":" + apolloPort);
		connection = factory.createConnection(apolloUser, apolloPassword);
		connection.start();

		Destination dest = new StompJmsDestination("/queue/78033.book.orders");
		Session session = connection.createSession(false,
				Session.AUTO_ACKNOWLEDGE);

		MessageProducer producer = session.createProducer(dest);
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

		String data = libraryName + ":" + isbn.toString();
		TextMessage message = session.createTextMessage(data);
		producer.send(message);
		System.out.println("Placed an order for the book in the queue"
				+ libraryName + ":" + isbn.toString());
		connection.close();

	}
}
