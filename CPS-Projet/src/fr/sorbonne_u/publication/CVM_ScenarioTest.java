package fr.sorbonne_u.publication;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.ComponentI.FComponentTask;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.messages.MessageFilter;

import fr.sorbonne_u.utils.aclocks.ClocksServer;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.components.utils.tests.TestStep;
import fr.sorbonne_u.components.utils.tests.TestStepI;

public class CVM_ScenarioTest extends AbstractCVM {

	public static final int NB_CHANNELS = 2;

	public static final String CLOCK_URI = "scenario-clock";
	public static final String START_INSTANT = "2026-03-10T10:00:00Z";
	public static final String END_INSTANT = "2026-03-10T10:02:00Z";
	public static final double ACCELERATION_FACTOR = 60.0;

	public static final String WINDMILL_1_URI = "windmill-1";
	public static final String DESK_1_URI = "desk-1";
	public static final String STATION_1_URI = "station-1";
	public static final String STATION_2_URI = "station-2";
	public static final String OFFICE_1_URI = "office-1";

	public CVM_ScenarioTest() throws Exception {
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

	// dummy message
	protected static Message dummyMessage() throws Exception {
		Message m = new Message("dummy", Instant.now());
		m.putProperty("type", "dummy");
		return m;
	}

	/** filter that never matches (for publisher clients) */
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

	// -------------------------------------------------------------------------
	// Scenario builder
	// -------------------------------------------------------------------------

	protected static TestScenario buildScenario() throws Exception {

		Instant start = Instant.parse(START_INSTANT);
		Instant end = Instant.parse(END_INSTANT);

		Instant subscribeWindmill = start.plusSeconds(20);
		Instant subscribeDesk = start.plusSeconds(30);
		Instant publishStation1 = start.plusSeconds(40);
		Instant publishStation2 = start.plusSeconds(50);
		Instant publishOffice = start.plusSeconds(60);

		return new TestScenario(
				CLOCK_URI,
				start,
				end,
				new TestStepI[] {

						new TestStep(
								CLOCK_URI,
								WINDMILL_1_URI,
								subscribeWindmill,
								(FComponentTask) owner -> {
									((Client) owner).runScenarioSubscribe();
								}),

						new TestStep(
								CLOCK_URI,
								DESK_1_URI,
								subscribeDesk,
								(FComponentTask) owner -> {
									((Client) owner).runScenarioSubscribe();
								}),

						new TestStep(
								CLOCK_URI,
								STATION_1_URI,
								publishStation1,
								(FComponentTask) owner -> {
									((Client) owner).runScenarioPublish();
								}),

						new TestStep(
								CLOCK_URI,
								STATION_2_URI,
								publishStation2,
								(FComponentTask) owner -> {
									((Client) owner).runScenarioPublish();
								}),

						new TestStep(
								CLOCK_URI,
								OFFICE_1_URI,
								publishOffice,
								(FComponentTask) owner -> {
									((Client) owner).runScenarioPublish();
								})
				});
	}

	@Override
	public void deploy() throws Exception {
		long startTimeNanos = TimeUnit.MILLISECONDS.toNanos(
				System.currentTimeMillis() + 2000L); // start in 2 seconds

		// ---------------------------------------------------------------------
		// 1. Create clock server
		// ---------------------------------------------------------------------

		AbstractComponent.createComponent(
				ClocksServer.class.getCanonicalName(),
				new Object[] {
						CLOCK_URI,
						startTimeNanos,
						Instant.parse(START_INSTANT),
						ACCELERATION_FACTOR
				});

		// ---------------------------------------------------------------------
		// 2. Build the shared test scenario
		// ---------------------------------------------------------------------
		final TestScenario scenario = buildScenario();

		// ---------------------------------------------------------------------
		// 3. Create broker
		// ---------------------------------------------------------------------
		AbstractComponent.createComponent(
				Broker.class.getCanonicalName(),
				new Object[] { NB_CHANNELS });

		final String brokerURI = Broker.registrationPortURI();

		// ---------------------------------------------------------------------
		// 4. Prepare messages
		// ---------------------------------------------------------------------
		Message windStrong = windMessage("station1 strong wind", 42, "north");
		Message windWeak = windMessage("station2 weak wind", 12, "south");
		Message alertMsg = alertMessage("storm warning", "orange");

		// ---------------------------------------------------------------------
		// 5. Create clients
		// ---------------------------------------------------------------------

		// strong wind subscriber
		AbstractComponent.createComponent(
				Client.class.getCanonicalName(),
				new Object[] {
						WINDMILL_1_URI,
						brokerURI,
						RegistrationClass.FREE,
						"channel0",
						strongWindFilter(),
						dummyMessage(),
						scenario
				});

		// alert subscriber
		AbstractComponent.createComponent(
				Client.class.getCanonicalName(),
				new Object[] {
						DESK_1_URI,
						brokerURI,
						RegistrationClass.PREMIUM,
						"channel1",
						alertFilter(),
						dummyMessage(),
						scenario
				});

		// station 1
		AbstractComponent.createComponent(
				Client.class.getCanonicalName(),
				new Object[] {
						STATION_1_URI,
						brokerURI,
						RegistrationClass.FREE,
						"channel0",
						neverMatchFilter(),
						windStrong,
						scenario
				});

		// station 2
		AbstractComponent.createComponent(
				Client.class.getCanonicalName(),
				new Object[] {
						STATION_2_URI,
						brokerURI,
						RegistrationClass.STANDARD,
						"channel0",
						neverMatchFilter(),
						windWeak,
						scenario
				});

		// office publisher
		AbstractComponent.createComponent(
				Client.class.getCanonicalName(),
				new Object[] {
						OFFICE_1_URI,
						brokerURI,
						RegistrationClass.PREMIUM,
						"channel1",
						neverMatchFilter(),
						alertMsg,
						scenario
				});

		super.deploy();
	}

	public static void main(String[] args) {
		try {
			CVM_ScenarioTest cvm = new CVM_ScenarioTest();

			cvm.startStandardLifeCycle(15000L);

			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}