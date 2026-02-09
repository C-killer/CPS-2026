package fr.sorbonne_u.messages;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import java.io.Serializable;
import java.time.Instant;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.*;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownPropertyException;

public class MessageTests {
	private Message message;
	private String payload;

	@Before
	public void setUp() {
		payload = "Danger urgent";
		message = new Message(payload);
	}

	@Test
	public void testPayload() {
		assertEquals(payload, message.getPayload());
		message.setPayload("111");
		assertEquals("111", message.getPayload());
	}

	@Test
	public void testProperties() throws UnknownPropertyException {
		message.putProperty("speed", 100.0);
		assertTrue(message.propertyExists("speed"));
		assertEquals(100.0, message.getPropertyValue("speed"));
		message.removeProperty("speed");
		assertFalse(message.propertyExists("speed"));
	}

	@Test
	public void testCopy() throws UnknownPropertyException {
		message.putProperty("id", 1);
		Message copy = (Message) message.copy();

		assertEquals(message.getPayload(), copy.getPayload());
		assertEquals(message.getTimeStamp(), copy.getTimeStamp());
		assertEquals(message.getPropertyValue("id"), copy.getPropertyValue("id"));

		message.putProperty("temp", true);
		assertFalse(copy.propertyExists("temp"));
	}

	@Test
	public void testTimeFilter() {
		Instant now = message.getTimeStamp();

		// AfterFilter
		TimeFilterI afterFilter = new MessageFilter.TimeFilter.AfterFilter(now.minusSeconds(10));
		assertTrue(afterFilter.match(now));

		// BeforeFilter
		TimeFilterI beforeFilter = new MessageFilter.TimeFilter.BeforeFilter(now.plusSeconds(10));
		assertTrue(beforeFilter.match(now));
	}

	@Test
	public void testPropertyFilter() {
		message.putProperty("temperature", 25);

		ValueFilterI valueFilter = new MessageFilter.ValueFilter(25);
		PropertyFilterI propFilter = new MessageFilter.PropertyFilter("temperature", valueFilter);

		MessageFilter mf = new MessageFilter(
				new PropertyFilterI[] { propFilter },
				null,
				null);

		assertTrue(mf.match(message));

		message.setPayload("different");
		MessageFilter mfFail = new MessageFilter(
				new PropertyFilterI[] {
						new MessageFilter.PropertyFilter("temperature", new MessageFilter.ValueFilter(30)) },
				null,
				null);
		assertFalse(mfFail.match(message));
	}

	@Test
	public void testMultiValuesFilter() {
		message.putProperty("weight", 80.0);
		message.putProperty("height", 1.80);

		String[] names = { "weight", "height" };
		Serializable[] expectedValues = { 80.0, 1.80 };

		MultiValuesFilterI mvf = new MessageFilter.MultiValuesFilter(names);
		assertTrue(mvf.match(expectedValues));

		PropertiesFilterI pf = new MessageFilter.PropertiesFilter(mvf);
		MessageFilter mf = new MessageFilter(null, new PropertiesFilterI[] { pf }, null);

		assertTrue(mf.match(message));
	}
}