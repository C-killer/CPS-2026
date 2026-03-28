package fr.sorbonne_u.publication;

import java.time.Duration;
import java.time.Instant;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.messages.MessageFilter;
import fr.sorbonne_u.meteo.MeteoAlert;
import fr.sorbonne_u.meteo.Position;
import fr.sorbonne_u.meteo.RectangularRegion;
import fr.sorbonne_u.meteo.WindData;
import fr.sorbonne_u.publication.components.MeteoOffice;
import fr.sorbonne_u.publication.components.MeteoStation;
import fr.sorbonne_u.publication.components.Windmill;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class CVM_PluginTest extends AbstractCVM {

	public static final int NB_CHANNELS = 2;

	public CVM_PluginTest() throws Exception {
		super();
	}

	protected static Message windMessage(
			String description,
			int speed,
			String zone) throws Exception {

		WindData wd = new WindData(new Position(0, 0), zoneToX(speed, zone), zoneToY(speed, zone));
		Message m = new Message(wd, Instant.now());
		m.putProperty("type", "wind");
		m.putProperty("speed", wd.force());
		m.putProperty("zone", zone);
		return m;
	}

	protected static Message alertMessage(
			String description,
			String level) throws Exception {

		MeteoAlert alert = new MeteoAlert(
				MeteoAlertI.AlertType.STORM,
				parseLevel(level),
				new RectangularRegion[] { new RectangularRegion(new Position(0, 45), new Position(10, 52)) },
				Instant.now(),
				Duration.ofHours(6));
		Message m = new Message(alert, Instant.now());
		m.putProperty("type", "alert");
		m.putProperty("level", level);
		return m;
	}

	private static double zoneToX(int speed, String zone) {
		if ("east".equals(zone))
			return speed;
		if ("west".equals(zone))
			return -speed;
		return 0;
	}

	private static double zoneToY(int speed, String zone) {
		if ("north".equals(zone))
			return speed;
		if ("south".equals(zone))
			return -speed;
		return 0;
	}

	private static MeteoAlertI.Level parseLevel(String level) {
		switch (level.toLowerCase()) {
			case "red":
				return MeteoAlertI.Level.RED;
			case "yellow":
				return MeteoAlertI.Level.YELLOW;
			case "green":
				return MeteoAlertI.Level.GREEN;
			case "scarlet":
				return MeteoAlertI.Level.SCARLET;
			default:
				return MeteoAlertI.Level.ORANGE;
		}
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

		// Windmill: FREE subscriber on channel0, strong-wind filter
		AbstractComponent.createComponent(
				Windmill.class.getCanonicalName(),
				new Object[] {
						"windmill-1",
						brokerURI,
						RegistrationClass.FREE,
						"channel0",
						strongWindFilter()
				});

		// Windmill: PREMIUM subscriber on channel1, alert filter
		AbstractComponent.createComponent(
				Windmill.class.getCanonicalName(),
				new Object[] {
						"desk-1",
						brokerURI,
						RegistrationClass.PREMIUM,
						"channel1",
						alertFilter()
				});

		Message windStrong = windMessage("station1 strong wind", 42, "north");
		Message windWeak = windMessage("station2 weak wind", 12, "south");
		Message alertMsg = alertMessage("storm warning", "orange");

		// MeteoStation: FREE publisher on channel0
		AbstractComponent.createComponent(
				MeteoStation.class.getCanonicalName(),
				new Object[] {
						"station-1",
						brokerURI,
						RegistrationClass.FREE,
						"channel0",
						windStrong
				});

		// MeteoStation: STANDARD publisher on channel0
		AbstractComponent.createComponent(
				MeteoStation.class.getCanonicalName(),
				new Object[] {
						"station-2",
						brokerURI,
						RegistrationClass.STANDARD,
						"channel0",
						windWeak
				});

		// MeteoOffice: PREMIUM publisher on channel1
		AbstractComponent.createComponent(
				MeteoOffice.class.getCanonicalName(),
				new Object[] {
						"office-1",
						brokerURI,
						RegistrationClass.PREMIUM,
						"channel1",
						alertMsg
				});

		super.deploy();
	}

	public static void main(String[] args) {
		try {
			CVM_PluginTest cvm = new CVM_PluginTest();

			cvm.startStandardLifeCycle(5000L);

			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}