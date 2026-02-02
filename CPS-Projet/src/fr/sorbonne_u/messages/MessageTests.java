package fr.sorbonne_u.messages;

import static org.junit.Assert.*;
import java.time.Instant;

import org.junit.Test;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;

public class MessageTests {
	@Test
	public void testMessageAndFilter() throws Exception {

		Message msg = new Message("Météo Data");
		msg.putProperty("windSpeed", 55.0);

		MessageFilter.ValueFilter vf = new MessageFilter.ValueFilter(55.0);
		MessageFilter.PropertyFilter pf = new MessageFilter.PropertyFilter("windSpeed", vf);

		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] { pf },
				null,
				null);

		assertTrue("Filter match fail", filter.match(msg));
	}

	@Test
	public void testTimeFilter() {
		Instant now = Instant.now();
		Message msg = new Message("Time Test");

		MessageFilter.TimeFilter tf = new MessageFilter.TimeFilter(now.minusSeconds(1), now.plusSeconds(1));

		assertTrue("Time filter fail", tf.match(msg.getTimeStamp()));
	}
}