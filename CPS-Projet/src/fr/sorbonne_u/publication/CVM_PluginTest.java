package fr.sorbonne_u.publication;

import java.time.Instant;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.messages.MessageFilter;

public class CVM_PluginTest extends AbstractCVM {

	public static final int NB_CHANNELS = 2;

	public CVM_PluginTest() throws Exception {
		super();
	}

	protected static Message windMessage(
			String payload,
			int speed,
			String zone) throws Exception {

		Message m = new Message(payload, Instant.now());
		m.putProperty("type", "wind");
		m.putProperty("speed", speed);
		m.putProperty("zone", zone);
		return m;
	}

	protected static Message alertMessage(
			String payload,
			String level) throws Exception {

		Message m = new Message(payload, Instant.now());
		m.putProperty("type", "alert");
		m.putProperty("level", level);
		return m;
	}

	// Filters
	// speed > 30
	protected static MessageFilterI strongWindFilter() {
		return new MessageFilter(
				new MessageFilter.PropertyFilterI[] {
						new MessageFilter.PropertyFilter(
								"speed",
								new MessageFilter.ValueFilter.GreaterThanFilter(30.0))
				},
				null,
				null);
	}

	// type == alert
	protected static MessageFilterI alertFilter() {
		return new MessageFilter(
				new MessageFilter.PropertyFilterI[] {
						new MessageFilter.PropertyFilter(
								"type",
								new MessageFilter.ValueFilter("alert"))
				},
				null,
				null);
	}

	// dummy message (避免 BCM constructor 参数为 null)
	protected static Message dummyMessage() throws Exception {
		Message m = new Message("dummy", Instant.now());
		m.putProperty("type", "dummy");
		return m;
	}

	/** filter that never matches (for publisher clients) */
	/**
	 * 一个永远不会匹配任何消息的过滤器，用于发布者客户端。
	 * 由于 Client.execute() 只要设置了 channel 就会自动订阅，该过滤器用于避免发布者收到自己发布的消息。
	 */
	protected static MessageFilterI neverMatchFilter() {
		return new MessageFilter(
				new MessageFilter.PropertyFilterI[] {
						new MessageFilter.PropertyFilter(
								"__never__",
								new MessageFilter.ValueFilter("x"))
				},
				null,
				null);
	}

	@Override
	public void deploy() throws Exception {

		AbstractComponent.createComponent(
				Broker.class.getCanonicalName(),
				new Object[] { NB_CHANNELS });

		final String brokerURI = Broker.registrationPortURI();

		// strong wind subscriber
		AbstractComponent.createComponent(
				Client.class.getCanonicalName(),
				new Object[] {
						"windmill-1",
						brokerURI,
						RegistrationClass.FREE,
						"channel0",
						strongWindFilter(),
						dummyMessage()
				});
		// alert subscriber
		AbstractComponent.createComponent(
				Client.class.getCanonicalName(),
				new Object[] {
						"desk-1",
						brokerURI,
						RegistrationClass.PREMIUM,
						"channel1",
						alertFilter(),
						dummyMessage()
				});

		Message windStrong = windMessage("station1 strong wind", 42, "north");
		Message windWeak = windMessage("station2 weak wind", 12, "south");
		Message alertMsg = alertMessage("storm warning", "orange");

		// station 1
		AbstractComponent.createComponent(
				Client.class.getCanonicalName(),
				new Object[] {
						"station-1",
						brokerURI,
						RegistrationClass.FREE,
						"channel0",
						neverMatchFilter(),
						windStrong
				});

		// station 2
		AbstractComponent.createComponent(
				Client.class.getCanonicalName(),
				new Object[] {
						"station-2",
						brokerURI,
						RegistrationClass.STANDARD,
						"channel0",
						neverMatchFilter(),
						windWeak
				});

		// office publisher
		AbstractComponent.createComponent(
				Client.class.getCanonicalName(),
				new Object[] {
						"office-1",
						brokerURI,
						RegistrationClass.PREMIUM,
						"channel1",
						neverMatchFilter(),
						alertMsg
				});

		super.deploy();
	}

	public static void main(String[] args) {
		try {
			CVM_PluginTest cvm = new CVM_PluginTest();

			cvm.startStandardLifeCycle(10000L);

			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}