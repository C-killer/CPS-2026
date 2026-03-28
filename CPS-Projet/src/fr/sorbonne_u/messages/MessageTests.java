package fr.sorbonne_u.messages;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import java.io.Serializable;
import java.time.Instant;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.*;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownPropertyException;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
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
		String[] names = { "weight", "height" };

		MultiValuesFilterI mvf = new MessageFilter.MultiValuesFilter(names, new Serializable[] { 0, 0 }) {
			@Override
			public boolean match(Serializable... values) {
				if (values == null || values.length != 2)
					return false;
				double weight = ((Number) values[0]).doubleValue();
				double height = ((Number) values[1]).doubleValue();
				return weight / (height * height) >= 25.0;
			}
		};

		// BMI = 80 / (1.80 * 1.80) ≈ 24.7 < 25 → false
		message.putProperty("weight", 80.0);
		message.putProperty("height", 1.80);
		PropertiesFilterI pf = new MessageFilter.PropertiesFilter(mvf);
		MessageFilter mf = new MessageFilter(null, new PropertiesFilterI[] { pf }, null);
		assertFalse(mf.match(message));

		// BMI = 90 / (1.70 * 1.70) ≈ 31.1 >= 25 → true
		message.putProperty("weight", 90.0);
		message.putProperty("height", 1.70);
		assertTrue(mf.match(message));
	}

	@Test
	public void testValueFilters() {
		// AnyValueFilter
		ValueFilterI any = new MessageFilter.ValueFilter.AnyValueFilter();
		assertTrue(any.match(999));
		assertTrue(any.match("anything"));

		// GreaterThanFilter
		ValueFilterI gt = new MessageFilter.ValueFilter.GreaterThanFilter(10.0);
		assertTrue(gt.match(15.0));
		assertFalse(gt.match(5.0));
		assertFalse(gt.match("not a number"));

		// LessThanFilter
		ValueFilterI lt = new MessageFilter.ValueFilter.LessThanFilter(10.0);
		assertTrue(lt.match(5.0));
		assertFalse(lt.match(15.0));

		// BetweenFilter
		ValueFilterI bt = new MessageFilter.ValueFilter.BetweenFilter(10.0, 20.0);
		assertTrue(bt.match(15.0));
		assertFalse(bt.match(25.0));
		assertTrue(bt.match(10.0)); // 边界
		assertTrue(bt.match(20.0)); // 边界
	}

	@Test
	public void testTimeBetweenFilter() {
		Instant now = Instant.now();
		TimeFilterI btf = new MessageFilter.TimeFilter.BetweenFilter(
				now.minusSeconds(10), now.plusSeconds(10));
		assertTrue(btf.match(now));
		assertFalse(btf.match(now.minusSeconds(20)));
	}

	@Test
	public void testTimeJokerFilter() {
		TimeFilterI joker = new MessageFilter.TimeFilter.JokerFilter();
		assertTrue(joker.match(Instant.now()));
		assertTrue(joker.match(Instant.EPOCH));
	}

	@Test
	public void testMatchNullMessage() {
		MessageFilter mf = new MessageFilter(null, null, null);
		assertFalse(mf.match(null));
	}

	@Test
	public void testMissingProperty() {
		PropertyFilterI pf = new MessageFilter.PropertyFilter("nonexistent",
				new MessageFilter.ValueFilter(42));
		MessageFilter mf = new MessageFilter(new PropertyFilterI[] { pf }, null, null);
		assertFalse(mf.match(message));
	}

	@Test
	public void testMultiValuesFilterExact() {
		String[] names = { "a", "b" };
		Serializable[] expected = { 1, 2 };
		MultiValuesFilterI mvf = new MessageFilter.MultiValuesFilter(names, expected);
		assertTrue(mvf.match(1, 2));
		assertFalse(mvf.match(1, 3));
		assertFalse(mvf.match(1)); // 长度不匹配
	}
}