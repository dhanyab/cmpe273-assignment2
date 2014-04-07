package edu.sjsu.cmpe.procurement.jobs;

import javax.jms.Connection;
import javax.jms.JMSException;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.Every;
import edu.sjsu.cmpe.procurement.ProcurementService;
import edu.sjsu.cmpe.procurement.config.ProcurementServiceConfiguration;
import edu.sjsu.cmpe.procurement.domain.BookOrders;
import edu.sjsu.cmpe.procurement.domain.ShippedBook;

/**
 * This job will run at every 5 second.
 */
@Every("300s")
public class ProcurementSchedulerJob extends Job {
	private final Logger log = LoggerFactory.getLogger(getClass());

	@Override
	public void doJob() {
		ProcurementService procServ = new ProcurementService();
		
		BookOrders bookOrders;
		try {
			bookOrders = ProcurementService.getOrderFromQueue();
			if (bookOrders.getOrder_book_isbns().size() > 0) {
				System.out.println("Request sending to Publisher........");
				Client client = Client.create();
				String url = "http://54.215.133.131:9000/orders";
				WebResource webRes = client.resource(url);
				ClientResponse response = webRes.accept("application/json")
						.type("application/json")
						.entity(bookOrders, "application/json")
						.post(ClientResponse.class);
				System.out.println(response.getEntity(String.class));
				System.out.println("Sent successfully!");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			procServ.publishTopicMessage();
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// String strResponse = ProcurementService.jerseyClient.resource(
		// "http://ip.jsontest.com/").get(String.class);
		// log.debug("Response from jsontest.com: {}", strResponse);
	}
}
