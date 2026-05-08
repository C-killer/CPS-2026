package fr.sorbonne_u.publication;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.ComponentI.FComponentTask;
import fr.sorbonne_u.components.cvm.AbstractDistributedCVM;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.components.utils.tests.TestStep;
import fr.sorbonne_u.components.utils.tests.TestStepI;
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

/**
 * Scenario de test temporise reparti sur 3 JVMs (§4.5 point 3 du cahier des charges).
 *
 * <p>Combine un {@link TestScenario} avec horloge acceleree et un deploiement
 * sur 3 JVMs, chacune avec un courtier local et des composants clients
 * de l'application meteo/eolienne.</p>
 *
 * <p>Topologie gossip :</p>
 * <pre>
 *     Broker-1 ---- Broker-2
 *         \          /
 *          Broker-3
 * </pre>
 *
 * <p>Scenario temporise (acceleration 60x) :</p>
 * <pre>
 *   t+5s   : JVM2 premium-creator cree "storm-alerts"
 *   t+15s  : JVM2 windmills souscrivent a channel0
 *            JVM3 desks souscrivent a channel1 et storm-alerts
 *   t+25s  : JVM1 stations publient sur channel0 (vents forts)
 *   t+30s  : JVM1 office publie alerte ORANGE sur channel1
 *   t+35s  : JVM2 creator publie alerte RED sur storm-alerts
 *   t+45s  : JVM3 station publie vent faible sur channel0 (filtre)
 * </pre>
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class DistributedScenarioCVM extends AbstractDistributedCVM {

	public static final String JVM1_URI = "jvm1";
	public static final String JVM2_URI = "jvm2";
	public static final String JVM3_URI = "jvm3";

	public static final int NB_CHANNELS = 2;

	// Clock
	public static final String CLOCK_URI = "distributed-clock";
	public static final Instant START_INSTANT = Instant.parse("2026-05-10T10:00:00Z");
	public static final Instant END_INSTANT = Instant.parse("2026-05-10T10:01:00Z");
	public static final double ACCELERATION_FACTOR = 60.0;

	// Broker-1 (JVM1)
	public static final String B1_URI = "broker-1";
	public static final String B1_REG = "b1-registration";
	public static final String B1_PUB = "b1-publishing";
	public static final String B1_PRIV = "b1-privileged";
	public static final String B1_GOSSIP = "b1-gossip-receiver";

	// Broker-2 (JVM2)
	public static final String B2_URI = "broker-2";
	public static final String B2_REG = "b2-registration";
	public static final String B2_PUB = "b2-publishing";
	public static final String B2_PRIV = "b2-privileged";
	public static final String B2_GOSSIP = "b2-gossip-receiver";

	// Broker-3 (JVM3)
	public static final String B3_URI = "broker-3";
	public static final String B3_REG = "b3-registration";
	public static final String B3_PUB = "b3-publishing";
	public static final String B3_PRIV = "b3-privileged";
	public static final String B3_GOSSIP = "b3-gossip-receiver";

	// Client URIs
	public static final String STATION_1 = "station-1";
	public static final String STATION_2 = "station-2";
	public static final String OFFICE_1 = "office-1";
	public static final String WINDMILL_1 = "windmill-1";
	public static final String WINDMILL_2 = "windmill-2";
	public static final String WINDMILL_3 = "windmill-3";
	public static final String CREATOR_1 = "creator-1";
	public static final String DESK_1 = "desk-1";
	public static final String DESK_2 = "desk-2";
	public static final String STATION_3 = "station-3";

	public static final String DEFAULT_CONFIG = "config-3jvm.xml";

	public DistributedScenarioCVM(String[] args) throws Exception {
		super(args);
	}

	// =====================================================================
	// Message / filter factories
	// =====================================================================

	static Message windMessage(double x, double y, int speed, String zone) throws Exception {
		double vx = "east".equals(zone) ? speed : "west".equals(zone) ? -speed : 0;
		double vy = "north".equals(zone) ? speed : "south".equals(zone) ? -speed : 0;
		WindData wd = new WindData(new Position(x, y), vx, vy);
		Message m = new Message(wd, Instant.now());
		m.putProperty("type", "wind");
		m.putProperty("speed", wd.force());
		m.putProperty("zone", zone);
		return m;
	}

	static Message alertMessage(String level) throws Exception {
		MeteoAlertI.Level lv;
		switch (level) {
			case "red": lv = MeteoAlertI.Level.RED; break;
			case "yellow": lv = MeteoAlertI.Level.YELLOW; break;
			default: lv = MeteoAlertI.Level.ORANGE;
		}
		MeteoAlert alert = new MeteoAlert(
				MeteoAlertI.AlertType.STORM, lv,
				new RectangularRegion[] {
						new RectangularRegion(new Position(-100, -100), new Position(100, 100))
				},
				Instant.now(), Duration.ofHours(6));
		Message m = new Message(alert, Instant.now());
		m.putProperty("type", "alert");
		m.putProperty("level", level);
		return m;
	}

	static MessageFilterI strongWindFilter() {
		return new MessageFilter(
				new MessageFilter.PropertyFilterI[] {
						new MessageFilter.PropertyFilter("speed",
								new MessageFilter.ValueFilter.GreaterThanFilter(30.0))
				}, null, null);
	}

	static MessageFilterI alertFilter() {
		return new MessageFilter(
				new MessageFilter.PropertyFilterI[] {
						new MessageFilter.PropertyFilter("type",
								new MessageFilter.ValueFilter("alert"))
				}, null, null);
	}

	static MessageFilterI neverMatchFilter() {
		return new MessageFilter(
				new MessageFilter.PropertyFilterI[] {
						new MessageFilter.PropertyFilter("__never__",
								new MessageFilter.ValueFilter("x"))
				}, null, null);
	}

	// =====================================================================
	// Shared scenario (built identically on every JVM)
	// =====================================================================

	static TestScenario buildScenario() throws Exception {
		Instant t0 = START_INSTANT;

		Instant tCreateChannel = t0.plusSeconds(5);
		Instant tSubscribe = t0.plusSeconds(15);
		Instant tPublishWind = t0.plusSeconds(25);
		Instant tPublishAlert = t0.plusSeconds(30);
		Instant tPublishStorm = t0.plusSeconds(35);
		Instant tPublishLate = t0.plusSeconds(45);

		return new TestScenario(
				CLOCK_URI, t0, END_INSTANT,
				new TestStepI[] {
						// t+5s : creator-1 cree storm-alerts (JVM2)
						new TestStep(CLOCK_URI, CREATOR_1, tCreateChannel,
								(FComponentTask) o -> ((Client) o).runScenarioCreateChannels()),

						// t+15s : windmills souscrivent channel0 (JVM2)
						new TestStep(CLOCK_URI, WINDMILL_1, tSubscribe,
								(FComponentTask) o -> ((Client) o).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, WINDMILL_2, tSubscribe,
								(FComponentTask) o -> ((Client) o).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, WINDMILL_3, tSubscribe,
								(FComponentTask) o -> ((Client) o).runScenarioSubscribe()),

						// t+15s : desks souscrivent channel1 + storm-alerts (JVM3)
						new TestStep(CLOCK_URI, DESK_1, tSubscribe,
								(FComponentTask) o -> ((Client) o).runScenarioSubscribe()),
						new TestStep(CLOCK_URI, DESK_2, tSubscribe,
								(FComponentTask) o -> ((Client) o).runScenarioSubscribe()),

						// t+25s : stations publient vent fort sur channel0 (JVM1)
						new TestStep(CLOCK_URI, STATION_1, tPublishWind,
								(FComponentTask) o -> ((Client) o).runScenarioPublish()),
						new TestStep(CLOCK_URI, STATION_2, tPublishWind,
								(FComponentTask) o -> ((Client) o).runScenarioPublish()),

						// t+30s : office publie alerte ORANGE sur channel1 (JVM1)
						new TestStep(CLOCK_URI, OFFICE_1, tPublishAlert,
								(FComponentTask) o -> ((Client) o).runScenarioPublish()),

						// t+35s : creator publie alerte RED sur storm-alerts (JVM2)
						new TestStep(CLOCK_URI, CREATOR_1, tPublishStorm,
								(FComponentTask) o -> ((Client) o).runScenarioPublish()),

						// t+45s : station-3 publie vent faible (JVM3, filtre)
						new TestStep(CLOCK_URI, STATION_3, tPublishLate,
								(FComponentTask) o -> ((Client) o).runScenarioPublish()),
				});
	}

	// =====================================================================
	// Deploy
	// =====================================================================

	@Override
	public void instantiateAndPublish() throws Exception {
		TestScenario scenario = buildScenario();

		if (thisJVMURI.equals(JVM1_URI)) {
			// =============================================================
			// JVM1 : ClocksServer + Broker-1
			//       + 2 stations (FREE pub ch0) + 1 office (PREMIUM pub ch1)
			// =============================================================

			// Horloge acceleree (partagee par toutes les JVMs via RMI)
			long startNanos = TimeUnit.MILLISECONDS.toNanos(
					System.currentTimeMillis() + 5000L);
			AbstractComponent.createComponent(
					ClocksServer.class.getCanonicalName(),
					new Object[] { CLOCK_URI, startNanos, START_INSTANT, ACCELERATION_FACTOR });

			// Broker-1 (voisins : B2, B3)
			AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {
							B1_URI, NB_CHANNELS,
							B1_REG, B1_PUB, B1_PRIV,
							B1_GOSSIP,
							new String[] { B2_GOSSIP, B3_GOSSIP }
					});

			// station-1 : vent fort 45kt nord
			AbstractComponent.createComponent(
					MeteoStation.class.getCanonicalName(),
					new Object[] { STATION_1, B1_REG, RegistrationClass.FREE,
							"channel0", windMessage(2, 3, 45, "north"), scenario });

			// station-2 : vent fort 38kt est
			AbstractComponent.createComponent(
					MeteoStation.class.getCanonicalName(),
					new Object[] { STATION_2, B1_REG, RegistrationClass.FREE,
							"channel0", windMessage(5, 8, 38, "east"), scenario });

			// office-1 : alerte ORANGE
			AbstractComponent.createComponent(
					MeteoOffice.class.getCanonicalName(),
					new Object[] { OFFICE_1, B1_REG, RegistrationClass.PREMIUM,
							"channel1", alertMessage("orange"), scenario });

		} else if (thisJVMURI.equals(JVM2_URI)) {
			// =============================================================
			// JVM2 : Broker-2
			//       + 3 windmills (FREE sub ch0, strongWindFilter)
			//       + 1 creator PREMIUM (cree storm-alerts, publie RED)
			// =============================================================
			AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {
							B2_URI, NB_CHANNELS,
							B2_REG, B2_PUB, B2_PRIV,
							B2_GOSSIP,
							new String[] { B1_GOSSIP, B3_GOSSIP }
					});

			// 3 windmills subscribers channel0
			AbstractComponent.createComponent(
					Windmill.class.getCanonicalName(),
					new Object[] { WINDMILL_1, B2_REG, RegistrationClass.FREE,
							"channel0", strongWindFilter(), scenario });
			AbstractComponent.createComponent(
					Windmill.class.getCanonicalName(),
					new Object[] { WINDMILL_2, B2_REG, RegistrationClass.FREE,
							"channel0", strongWindFilter(), scenario });
			AbstractComponent.createComponent(
					Windmill.class.getCanonicalName(),
					new Object[] { WINDMILL_3, B2_REG, RegistrationClass.FREE,
							"channel0", strongWindFilter(), scenario });

			// creator : cree storm-alerts (t+5s), publie alert RED (t+35s)
			AbstractComponent.createComponent(
					Client.class.getCanonicalName(),
					new Object[] { CREATOR_1, B2_REG, RegistrationClass.PREMIUM,
							"storm-alerts", neverMatchFilter(),
							alertMessage("red"), scenario });

		} else if (thisJVMURI.equals(JVM3_URI)) {
			// =============================================================
			// JVM3 : Broker-3
			//       + 1 desk (PREMIUM sub ch1, alertFilter)
			//       + 1 desk (PREMIUM sub storm-alerts, alertFilter)
			//       + 1 station (FREE pub ch0, vent faible = filtre)
			// =============================================================
			AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {
							B3_URI, NB_CHANNELS,
							B3_REG, B3_PUB, B3_PRIV,
							B3_GOSSIP,
							new String[] { B1_GOSSIP, B2_GOSSIP }
					});

			// desk-1 : sub channel1 (alerte ORANGE de JVM1)
			AbstractComponent.createComponent(
					Windmill.class.getCanonicalName(),
					new Object[] { DESK_1, B3_REG, RegistrationClass.PREMIUM,
							"channel1", alertFilter(), scenario });

			// desk-2 : sub storm-alerts (alerte RED de JVM2, canal dynamique)
			AbstractComponent.createComponent(
					Windmill.class.getCanonicalName(),
					new Object[] { DESK_2, B3_REG, RegistrationClass.PREMIUM,
							"storm-alerts", alertFilter(), scenario });

			// station-3 : vent faible 10kt (sera filtre par strongWindFilter)
			AbstractComponent.createComponent(
					MeteoStation.class.getCanonicalName(),
					new Object[] { STATION_3, B3_REG, RegistrationClass.FREE,
							"channel0", windMessage(50, 50, 10, "south"), scenario });

		} else {
			System.err.println("Unknown JVM URI: " + thisJVMURI);
		}

		super.instantiateAndPublish();
	}

	@Override
	public void interconnect() throws Exception {
		super.interconnect();
	}

	@Override
	public void finalise() throws Exception {
		super.finalise();
	}

	@Override
	public void shutdown() throws Exception {
		super.shutdown();
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: DistributedScenarioCVM <jvmURI> [config-3jvm.xml]");
			System.err.println("  jvmURI: jvm1 | jvm2 | jvm3");
			System.exit(1);
		}
		if (args.length == 1) {
			args = new String[] { args[0], DEFAULT_CONFIG };
		}
		try {
			DistributedScenarioCVM dcvm = new DistributedScenarioCVM(args);
			dcvm.startStandardLifeCycle(30000L);
			Thread.sleep(5000L);
			System.exit(0);
		} catch (Throwable e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
