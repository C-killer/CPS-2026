package fr.sorbonne_u.publication;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.ComponentI.FComponentTask;
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

import fr.sorbonne_u.utils.aclocks.ClocksServer;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.components.utils.tests.TestStep;
import fr.sorbonne_u.components.utils.tests.TestStepI;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class CVM_ScenarioTest extends AbstractCVM {

	// Pre-created channels (created by Broker at startup)
	public static final int NB_CHANNELS = 2;

	// Clock configuration
	public static final String CLOCK_URI = "scenario-clock";
	public static final String START_INSTANT = "2026-03-10T10:00:00Z";
	public static final String END_INSTANT = "2026-03-10T10:02:00Z";
	public static final double ACCELERATION_FACTOR = 60.0;

	// -------------------------------------------------------------------------
	// Client URIs — FREE subscribers (channel0, strongWindFilter)
	// -------------------------------------------------------------------------
	public static final String WINDMILL_1 = "windmill-1";
	public static final String WINDMILL_2 = "windmill-2";
	public static final String WINDMILL_3 = "windmill-3";
	public static final String WINDMILL_4 = "windmill-4";
	public static final String WINDMILL_5 = "windmill-5";
	public static final String WINDMILL_6 = "windmill-6";
	public static final String WINDMILL_7 = "windmill-7";
	public static final String WINDMILL_8 = "windmill-8";
	public static final String WINDMILL_9 = "windmill-9";
	public static final String WINDMILL_10 = "windmill-10";

	// PREMIUM subscribers (channel1, alertFilter)
	public static final String DESK_1 = "desk-1";
	public static final String DESK_2 = "desk-2";
	public static final String DESK_3 = "desk-3";
	public static final String DESK_4 = "desk-4";
	public static final String DESK_5 = "desk-5";
	public static final String DESK_6 = "desk-6";
	public static final String DESK_7 = "desk-7";
	public static final String DESK_8 = "desk-8";

	// STANDARD subscribers (channel2, no filter) — channel2 created dynamically
	public static final String MONITOR_1 = "monitor-1";
	public static final String MONITOR_2 = "monitor-2";
	public static final String MONITOR_3 = "monitor-3";
	public static final String MONITOR_4 = "monitor-4";
	public static final String MONITOR_5 = "monitor-5";

	// STANDARD subscribers (channel3, no filter) — channel3 created dynamically
	public static final String SENSOR_1 = "sensor-1";
	public static final String SENSOR_2 = "sensor-2";
	public static final String SENSOR_3 = "sensor-3";
	public static final String SENSOR_4 = "sensor-4";
	public static final String SENSOR_5 = "sensor-5";

	// FREE publishers (channel0)
	public static final String STATION_1 = "station-1";
	public static final String STATION_2 = "station-2";
	public static final String STATION_3 = "station-3";
	public static final String STATION_4 = "station-4";
	public static final String STATION_5 = "station-5";
	public static final String STATION_6 = "station-6";
	public static final String STATION_7 = "station-7";
	public static final String STATION_8 = "station-8";

	// PREMIUM publishers (channel1)
	public static final String OFFICE_1 = "office-1";
	public static final String OFFICE_2 = "office-2";
	public static final String OFFICE_3 = "office-3";
	public static final String OFFICE_4 = "office-4";

	// PREMIUM channel creators: create channel2/channel3, then publish to them
	public static final String PREMIUM_CREATOR_1 = "premium-creator-1";
	public static final String PREMIUM_CREATOR_2 = "premium-creator-2";

	// STANDARD publishers (channel2 / channel3) — created dynamically
	public static final String DATA_1 = "data-1";
	public static final String DATA_2 = "data-2";
	public static final String REPORT_1 = "report-1";

	// -------------------------------------------------------------------------
	// Constructor
	// -------------------------------------------------------------------------
	public CVM_ScenarioTest() throws Exception {
		super();
	}

	// -------------------------------------------------------------------------
	// Message factories
	// -------------------------------------------------------------------------
	protected static Message windMessage(String description, int speed, String zone) throws Exception {
		WindData wd = new WindData(new Position(0, 0), zoneToX(speed, zone), zoneToY(speed, zone));
		Message m = new Message(wd, Instant.now());
		m.putProperty("type", "wind");
		m.putProperty("speed", wd.force());
		m.putProperty("zone", zone);
		return m;
	}

	protected static Message alertMessage(String description, String level) throws Exception {
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

	protected static Message dataMessage(String description, String source) throws Exception {
		Message m = new Message(description, Instant.now());
		m.putProperty("type", "data");
		m.putProperty("source", source);
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

	protected static Message dummyMessage() throws Exception {
		Message m = new Message("dummy", Instant.now());
		m.putProperty("type", "dummy");
		return m;
	}

	// -------------------------------------------------------------------------
	// Filter factories
	// -------------------------------------------------------------------------
	protected static MessageFilterI strongWindFilter() {
		return new MessageFilter(
				new MessageFilter.PropertyFilterI[] {
						new MessageFilter.PropertyFilter(
								"speed",
								new MessageFilter.ValueFilter.GreaterThanFilter(30.0))
				},
				null, null);
	}

	protected static MessageFilterI alertFilter() {
		return new MessageFilter(
				new MessageFilter.PropertyFilterI[] {
						new MessageFilter.PropertyFilter(
								"type",
								new MessageFilter.ValueFilter("alert"))
				},
				null, null);
	}

	protected static MessageFilterI neverMatchFilter() {
		return new MessageFilter(
				new MessageFilter.PropertyFilterI[] {
						new MessageFilter.PropertyFilter(
								"__never__",
								new MessageFilter.ValueFilter("x"))
				},
				null, null);
	}

	// -------------------------------------------------------------------------
	// Scenario builder
	// -------------------------------------------------------------------------
	protected static TestScenario buildScenario() throws Exception {

		Instant start = Instant.parse(START_INSTANT);
		Instant end = Instant.parse(END_INSTANT);

		// -- Time instants --
		// t+5s : PREMIUM creators create their channels
		Instant tCreateChannels = start.plusSeconds(5);

		// t+20s : FREE windmill subscribers
		Instant tSubscribeWindmill = start.plusSeconds(20);
		// t+25s : PREMIUM desk subscribers
		Instant tSubscribeDesk = start.plusSeconds(25);
		// t+30s : STANDARD monitor subscribers (channel2)
		Instant tSubscribeMonitor = start.plusSeconds(30);
		// t+33s : STANDARD sensor subscribers (channel3)
		Instant tSubscribeSensor = start.plusSeconds(33);

		// t+40s : FREE station publishers (channel0)
		Instant tPublishStation = start.plusSeconds(40);
		// t+45s : PREMIUM office publishers (channel1)
		Instant tPublishOffice = start.plusSeconds(45);
		// t+50s : STANDARD data/report publishers (channel2/3)
		Instant tPublishData = start.plusSeconds(50);
		// t+55s : PREMIUM creators publish to their channels
		Instant tPublishCreator = start.plusSeconds(55);

		return new TestScenario(
				CLOCK_URI,
				start,
				end,
				new TestStepI[] {

						// ---- Channel creation (t+5s) ----
						new TestStep(CLOCK_URI, PREMIUM_CREATOR_1, tCreateChannels,
								(FComponentTask) owner -> ((Client) owner).runScenarioCreateChannels()),

						new TestStep(CLOCK_URI, PREMIUM_CREATOR_2, tCreateChannels,
								(FComponentTask) owner -> ((Client) owner).runScenarioCreateChannels()),

						// ---- FREE windmill subscribers (t+20s) ----
						new TestStep(CLOCK_URI, WINDMILL_1, tSubscribeWindmill,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, WINDMILL_2, tSubscribeWindmill,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, WINDMILL_3, tSubscribeWindmill,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, WINDMILL_4, tSubscribeWindmill,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, WINDMILL_5, tSubscribeWindmill,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, WINDMILL_6, tSubscribeWindmill,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, WINDMILL_7, tSubscribeWindmill,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, WINDMILL_8, tSubscribeWindmill,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, WINDMILL_9, tSubscribeWindmill,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, WINDMILL_10, tSubscribeWindmill,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),

						// ---- PREMIUM desk subscribers (t+25s) ----
						new TestStep(CLOCK_URI, DESK_1, tSubscribeDesk,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, DESK_2, tSubscribeDesk,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, DESK_3, tSubscribeDesk,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, DESK_4, tSubscribeDesk,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, DESK_5, tSubscribeDesk,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, DESK_6, tSubscribeDesk,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, DESK_7, tSubscribeDesk,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, DESK_8, tSubscribeDesk,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),

						// ---- STANDARD monitor subscribers (t+30s) ----
						new TestStep(CLOCK_URI, MONITOR_1, tSubscribeMonitor,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, MONITOR_2, tSubscribeMonitor,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, MONITOR_3, tSubscribeMonitor,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, MONITOR_4, tSubscribeMonitor,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, MONITOR_5, tSubscribeMonitor,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),

						// ---- STANDARD sensor subscribers (t+33s) ----
						new TestStep(CLOCK_URI, SENSOR_1, tSubscribeSensor,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, SENSOR_2, tSubscribeSensor,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, SENSOR_3, tSubscribeSensor,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, SENSOR_4, tSubscribeSensor,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, SENSOR_5, tSubscribeSensor,
								(FComponentTask) owner -> ((Client) owner).runScenarioSubscribe()),

						// ---- FREE station publishers (t+40s) ----
						new TestStep(CLOCK_URI, STATION_1, tPublishStation,
								(FComponentTask) owner -> ((Client) owner).runScenarioPublish()),
						new TestStep(CLOCK_URI, STATION_2, tPublishStation,
								(FComponentTask) owner -> ((Client) owner).runScenarioPublish()),
						new TestStep(CLOCK_URI, STATION_3, tPublishStation,
								(FComponentTask) owner -> ((Client) owner).runScenarioPublish()),
						new TestStep(CLOCK_URI, STATION_4, tPublishStation,
								(FComponentTask) owner -> ((Client) owner).runScenarioPublish()),
						new TestStep(CLOCK_URI, STATION_5, tPublishStation,
								(FComponentTask) owner -> ((Client) owner).runScenarioPublish()),
						new TestStep(CLOCK_URI, STATION_6, tPublishStation,
								(FComponentTask) owner -> ((Client) owner).runScenarioPublish()),
						new TestStep(CLOCK_URI, STATION_7, tPublishStation,
								(FComponentTask) owner -> ((Client) owner).runScenarioPublish()),
						new TestStep(CLOCK_URI, STATION_8, tPublishStation,
								(FComponentTask) owner -> ((Client) owner).runScenarioPublish()),

						// ---- PREMIUM office publishers (t+45s) ----
						new TestStep(CLOCK_URI, OFFICE_1, tPublishOffice,
								(FComponentTask) owner -> ((Client) owner).runScenarioPublish()),
						new TestStep(CLOCK_URI, OFFICE_2, tPublishOffice,
								(FComponentTask) owner -> ((Client) owner).runScenarioPublish()),
						new TestStep(CLOCK_URI, OFFICE_3, tPublishOffice,
								(FComponentTask) owner -> ((Client) owner).runScenarioPublish()),
						new TestStep(CLOCK_URI, OFFICE_4, tPublishOffice,
								(FComponentTask) owner -> ((Client) owner).runScenarioPublish()),

						// ---- STANDARD data/report publishers (t+50s) ----
						new TestStep(CLOCK_URI, DATA_1, tPublishData,
								(FComponentTask) owner -> ((Client) owner).runScenarioPublish()),
						new TestStep(CLOCK_URI, DATA_2, tPublishData,
								(FComponentTask) owner -> ((Client) owner).runScenarioPublish()),
						new TestStep(CLOCK_URI, REPORT_1, tPublishData,
								(FComponentTask) owner -> ((Client) owner).runScenarioPublish()),

						// ---- PREMIUM creators publish to their channels (t+55s) ----
						new TestStep(CLOCK_URI, PREMIUM_CREATOR_1, tPublishCreator,
								(FComponentTask) owner -> ((Client) owner).runScenarioPublish()),

						new TestStep(CLOCK_URI, PREMIUM_CREATOR_2, tPublishCreator,
								(FComponentTask) owner -> ((Client) owner).runScenarioPublish()),
				});
	}

	// -------------------------------------------------------------------------
	// Deploy
	// -------------------------------------------------------------------------
	@Override
	public void deploy() throws Exception {
		long startTimeNanos = TimeUnit.MILLISECONDS.toNanos(
				System.currentTimeMillis() + 2000L);

		// -- Clock server --
		AbstractComponent.createComponent(
				ClocksServer.class.getCanonicalName(),
				new Object[] {
						CLOCK_URI,
						startTimeNanos,
						Instant.parse(START_INSTANT),
						ACCELERATION_FACTOR
				});

		// -- Shared scenario --
		final TestScenario scenario = buildScenario();

		// -- Broker (pre-creates channel0 and channel1) --
		AbstractComponent.createComponent(
				Broker.class.getCanonicalName(),
				new Object[] { NB_CHANNELS });

		final String brokerURI = Broker.registrationPortURI();

		// =====================================================================
		// Prepare messages
		// =====================================================================

		// channel0 — strong wind (speed=42, passes strongWindFilter)
		Message windStrong1 = windMessage("station1 strong wind", 45, "north");
		Message windStrong2 = windMessage("station2 strong wind", 38, "east");
		Message windStrong3 = windMessage("station3 strong wind", 52, "west");
		Message windStrong4 = windMessage("station4 strong wind", 35, "north");
		// channel0 — weak wind (speed<=30, filtered out by strongWindFilter)
		Message windWeak5 = windMessage("station5 weak wind", 15, "south");
		Message windWeak6 = windMessage("station6 weak wind", 8, "south");
		Message windWeak7 = windMessage("station7 weak wind", 22, "east");
		Message windWeak8 = windMessage("station8 weak wind", 5, "west");

		// channel1 — alerts
		Message alert1 = alertMessage("office1 storm warning", "orange");
		Message alert2 = alertMessage("office2 flood warning", "red");
		Message alert3 = alertMessage("office3 heatwave", "yellow");
		Message alert4 = alertMessage("office4 fog alert", "orange");

		// channel2 — data from STANDARD publishers
		Message data1msg = dataMessage("data-1 sensor reading", "sensor-array-A");
		Message data2msg = dataMessage("data-2 sensor reading", "sensor-array-B");

		// channel3 — report from STANDARD publisher
		Message report1msg = dataMessage("report-1 daily summary", "report-system");

		// channel2 — published by the PREMIUM creator at t+55s
		Message creatorMsg2 = dataMessage("premium-creator-1 channel2 announcement", "creator-1");
		// channel3 — published by the PREMIUM creator at t+55s
		Message creatorMsg3 = dataMessage("premium-creator-2 channel3 announcement", "creator-2");

		// =====================================================================
		// Create clients — 45 total
		// =====================================================================

		// -- 10 FREE subscribers (channel0, strongWindFilter) --
		for (int i = 1; i <= 10; i++) {
			AbstractComponent.createComponent(
					Windmill.class.getCanonicalName(),
					new Object[] {
							"windmill-" + i,
							brokerURI,
							RegistrationClass.FREE,
							"channel0",
							strongWindFilter(),
							scenario
					});
		}

		// -- 8 PREMIUM subscribers (channel1, alertFilter) --
		for (int i = 1; i <= 8; i++) {
			AbstractComponent.createComponent(
					Windmill.class.getCanonicalName(),
					new Object[] {
							"desk-" + i,
							brokerURI,
							RegistrationClass.PREMIUM,
							"channel1",
							alertFilter(),
							scenario
					});
		}

		// -- 5 STANDARD subscribers (channel2, pass-all filter) --
		for (int i = 1; i <= 5; i++) {
			AbstractComponent.createComponent(
					Windmill.class.getCanonicalName(),
					new Object[] {
							"monitor-" + i,
							brokerURI,
							RegistrationClass.STANDARD,
							"channel2",
							new MessageFilter(null, null, null),
							scenario
					});
		}

		// -- 5 STANDARD subscribers (channel3, pass-all filter) --
		for (int i = 1; i <= 5; i++) {
			AbstractComponent.createComponent(
					Windmill.class.getCanonicalName(),
					new Object[] {
							"sensor-" + i,
							brokerURI,
							RegistrationClass.STANDARD,
							"channel3",
							new MessageFilter(null, null, null),
							scenario
					});
		}

		// -- 8 FREE publishers (channel0) --
		Message[] windMsgs = {
				windStrong1, windStrong2, windStrong3, windStrong4,
				windWeak5, windWeak6, windWeak7, windWeak8
		};
		for (int i = 1; i <= 8; i++) {
			AbstractComponent.createComponent(
					MeteoStation.class.getCanonicalName(),
					new Object[] {
							"station-" + i,
							brokerURI,
							RegistrationClass.FREE,
							"channel0",
							windMsgs[i - 1],
							scenario
					});
		}

		// -- 4 PREMIUM publishers (channel1) --
		Message[] alertMsgs = { alert1, alert2, alert3, alert4 };
		for (int i = 1; i <= 4; i++) {
			AbstractComponent.createComponent(
					MeteoOffice.class.getCanonicalName(),
					new Object[] {
							"office-" + i,
							brokerURI,
							RegistrationClass.PREMIUM,
							"channel1",
							alertMsgs[i - 1],
							scenario
					});
		}

		// -- 2 STANDARD publishers (channel2) --
		AbstractComponent.createComponent(
				MeteoStation.class.getCanonicalName(),
				new Object[] {
						DATA_1,
						brokerURI,
						RegistrationClass.STANDARD,
						"channel2",
						data1msg,
						scenario
				});
		AbstractComponent.createComponent(
				MeteoStation.class.getCanonicalName(),
				new Object[] {
						DATA_2,
						brokerURI,
						RegistrationClass.STANDARD,
						"channel2",
						data2msg,
						scenario
				});

		// -- 1 STANDARD publisher (channel3) --
		AbstractComponent.createComponent(
				MeteoStation.class.getCanonicalName(),
				new Object[] {
						REPORT_1,
						brokerURI,
						RegistrationClass.STANDARD,
						"channel3",
						report1msg,
						scenario
				});

		// -- 2 PREMIUM channel creators (channel2, channel3) --
		// Each creator: step1=createChannel (t+5s), step2=publish (t+55s)
		AbstractComponent.createComponent(
				Client.class.getCanonicalName(),
				new Object[] {
						PREMIUM_CREATOR_1,
						brokerURI,
						RegistrationClass.PREMIUM,
						"channel2",
						neverMatchFilter(),
						creatorMsg2,
						scenario
				});
		AbstractComponent.createComponent(
				Client.class.getCanonicalName(),
				new Object[] {
						PREMIUM_CREATOR_2,
						brokerURI,
						RegistrationClass.PREMIUM,
						"channel3",
						neverMatchFilter(),
						creatorMsg3,
						scenario
				});

		super.deploy();
	}

	// -------------------------------------------------------------------------
	// Main
	// -------------------------------------------------------------------------
	public static void main(String[] args) {
		try {
			CVM_ScenarioTest cvm = new CVM_ScenarioTest();
			cvm.startStandardLifeCycle(20000L);
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
