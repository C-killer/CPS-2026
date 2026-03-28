package fr.sorbonne_u.publication;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.ComponentI.FComponentTask;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.components.utils.tests.TestStep;
import fr.sorbonne_u.components.utils.tests.TestStepI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.messages.MessageFilter;
import fr.sorbonne_u.meteo.Position;
import fr.sorbonne_u.meteo.WindData;
import fr.sorbonne_u.publication.components.MeteoStation;
import fr.sorbonne_u.publication.components.Windmill;
import fr.sorbonne_u.utils.aclocks.ClocksServer;

/**
 * Integration test for advanced reception modes (§3.5.3) and concurrent
 * pressure on the broker (§3.6).
 *
 * <h3>Components (14 clients + broker)</h3>
 * <ul>
 *   <li><b>3 wait-subs</b> (FREE/STANDARD/PREMIUM):
 *       call {@code waitForNextMessage} simultaneously</li>
 *   <li><b>2 future-subs</b> (STANDARD/PREMIUM):
 *       call {@code getNextMessage} simultaneously</li>
 *   <li><b>3 normal-subs</b> (FREE×2, STANDARD):
 *       standard receive (no waiter)</li>
 *   <li><b>3 sync-publishers</b> (FREE): standard {@code publish}</li>
 *   <li><b>2 async-publishers</b> (STANDARD/PREMIUM):
 *       {@code asyncPublishAndNotify}</li>
 *   <li><b>1 late-publisher</b> (FREE): publishes after all waiters consumed</li>
 * </ul>
 *
 * <h3>Timeline (60x acceleration)</h3>
 * <pre>
 *  t+5s   all 8 subscribers subscribe to channel0
 *  t+12s  wait-sub-1/2/3 call waitForNextMessage  → block
 *         future-sub-1/2 call getNextMessage       → get Futures
 *  t+20s  pub-1..3 publish (sync), pub-4..5 asyncPublishAndNotify
 *         → 5 concurrent publishes at the SAME instant
 *         → first message per sub dispatched to waiter/future
 *         → remaining messages go through normal receive
 *  t+30s  pub-late publishes → all waiters consumed, normal path only
 * </pre>
 *
 * <h3>Demonstrates</h3>
 * <ol>
 *   <li>5 concurrent publishes (3 sync + 2 async) hitting the broker</li>
 *   <li>3 concurrent {@code waitForNextMessage} resolving correctly</li>
 *   <li>2 concurrent {@code getNextMessage} Futures resolving correctly</li>
 *   <li>Waiter-first dispatch vs normal receive coexistence</li>
 *   <li>{@code asyncPublishAndNotify} with notification port</li>
 *   <li>Service-class-differentiated delivery (PREMIUM 4 threads,
 *       STANDARD 2, FREE 1)</li>
 * </ol>
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class CVM_AsyncTest extends AbstractCVM {

	public static final int    NB_CHANNELS        = 1;
	public static final String CLOCK_URI           = "async-clock";
	public static final String START_INSTANT       = "2026-04-02T10:00:00Z";
	public static final String END_INSTANT         = "2026-04-02T10:02:00Z";
	public static final double ACCELERATION_FACTOR = 60.0;

	// Subscribers
	public static final String WAIT_SUB_1   = "wait-sub-1";
	public static final String WAIT_SUB_2   = "wait-sub-2";
	public static final String WAIT_SUB_3   = "wait-sub-3";
	public static final String FUTURE_SUB_1 = "future-sub-1";
	public static final String FUTURE_SUB_2 = "future-sub-2";
	public static final String NORMAL_SUB_1 = "normal-sub-1";
	public static final String NORMAL_SUB_2 = "normal-sub-2";
	public static final String NORMAL_SUB_3 = "normal-sub-3";

	// Publishers
	public static final String PUB_1 = "pub-1";
	public static final String PUB_2 = "pub-2";
	public static final String PUB_3 = "pub-3";
	public static final String PUB_4 = "pub-4";
	public static final String PUB_5 = "pub-5";
	public static final String PUB_6 = "pub-late";

	public CVM_AsyncTest() throws Exception { super(); }

	// -------------------------------------------------------------------------
	// Scenario
	// -------------------------------------------------------------------------

	protected static TestScenario buildScenario() throws Exception {
		Instant start = Instant.parse(START_INSTANT);
		Instant end   = Instant.parse(END_INSTANT);

		Instant tSub     = start.plusSeconds(5);   // all subscribe
		Instant tRequest = start.plusSeconds(12);   // waiters + futures register
		Instant tBurst   = start.plusSeconds(20);   // 5 concurrent publishes
		Instant tLate    = start.plusSeconds(30);   // 1 late publish (all waiters consumed)

		return new TestScenario(CLOCK_URI, start, end, new TestStepI[] {

				// ---- Subscribe (t+5s) ----
				new TestStep(CLOCK_URI, WAIT_SUB_1,   tSub, (FComponentTask) o -> ((Client) o).runScenarioSubscribe()),
				new TestStep(CLOCK_URI, WAIT_SUB_2,   tSub, (FComponentTask) o -> ((Client) o).runScenarioSubscribe()),
				new TestStep(CLOCK_URI, WAIT_SUB_3,   tSub, (FComponentTask) o -> ((Client) o).runScenarioSubscribe()),
				new TestStep(CLOCK_URI, FUTURE_SUB_1, tSub, (FComponentTask) o -> ((Client) o).runScenarioSubscribe()),
				new TestStep(CLOCK_URI, FUTURE_SUB_2, tSub, (FComponentTask) o -> ((Client) o).runScenarioSubscribe()),
				new TestStep(CLOCK_URI, NORMAL_SUB_1, tSub, (FComponentTask) o -> ((Client) o).runScenarioSubscribe()),
				new TestStep(CLOCK_URI, NORMAL_SUB_2, tSub, (FComponentTask) o -> ((Client) o).runScenarioSubscribe()),
				new TestStep(CLOCK_URI, NORMAL_SUB_3, tSub, (FComponentTask) o -> ((Client) o).runScenarioSubscribe()),

				// ---- Advanced reception requests (t+12s, all simultaneous) ----
				new TestStep(CLOCK_URI, WAIT_SUB_1,   tRequest, (FComponentTask) o -> ((Client) o).runScenarioWaitForMessage()),
				new TestStep(CLOCK_URI, WAIT_SUB_2,   tRequest, (FComponentTask) o -> ((Client) o).runScenarioWaitForMessage()),
				new TestStep(CLOCK_URI, WAIT_SUB_3,   tRequest, (FComponentTask) o -> ((Client) o).runScenarioWaitForMessage()),
				new TestStep(CLOCK_URI, FUTURE_SUB_1, tRequest, (FComponentTask) o -> ((Client) o).runScenarioGetNextMessage()),
				new TestStep(CLOCK_URI, FUTURE_SUB_2, tRequest, (FComponentTask) o -> ((Client) o).runScenarioGetNextMessage()),

				// ---- 5 concurrent publishes (t+20s, SAME instant) ----
				// pub-1..3: synchronous publish (FREE)
				new TestStep(CLOCK_URI, PUB_1, tBurst, (FComponentTask) o -> ((Client) o).runScenarioPublish()),
				new TestStep(CLOCK_URI, PUB_2, tBurst, (FComponentTask) o -> ((Client) o).runScenarioPublish()),
				new TestStep(CLOCK_URI, PUB_3, tBurst, (FComponentTask) o -> ((Client) o).runScenarioPublish()),
				// pub-4..5: asyncPublishAndNotify (STANDARD / PREMIUM)
				new TestStep(CLOCK_URI, PUB_4, tBurst, (FComponentTask) o -> ((Client) o).runScenarioAsyncPublish()),
				new TestStep(CLOCK_URI, PUB_5, tBurst, (FComponentTask) o -> ((Client) o).runScenarioAsyncPublish()),

				// ---- Late publish (t+30s, all waiters consumed) ----
				new TestStep(CLOCK_URI, PUB_6, tLate, (FComponentTask) o -> ((Client) o).runScenarioPublish()),
		});
	}

	// -------------------------------------------------------------------------
	// Deploy
	// -------------------------------------------------------------------------

	@Override
	public void deploy() throws Exception {
		long startTimeNanos = TimeUnit.MILLISECONDS.toNanos(
				System.currentTimeMillis() + 2000L);

		AbstractComponent.createComponent(ClocksServer.class.getCanonicalName(),
				new Object[] {
						CLOCK_URI, startTimeNanos,
						Instant.parse(START_INSTANT), ACCELERATION_FACTOR });

		final TestScenario scenario = buildScenario();

		AbstractComponent.createComponent(Broker.class.getCanonicalName(),
				new Object[] { NB_CHANNELS });

		final String brokerURI = Broker.registrationPortURI();
		MessageFilterI matchAll = new MessageFilter(null, null, null);

		// ---- Messages (one per publisher, different wind vectors) ----
		Message[] msgs = new Message[6];
		int[][] vecs = { {0,45}, {35,0}, {-25,25}, {0,-40}, {20,20}, {-10,30} };
		for (int i = 0; i < 6; i++) {
			WindData wd = new WindData(new Position(i + 1, i + 1), vecs[i][0], vecs[i][1]);
			msgs[i] = new Message(wd, Instant.now());
			msgs[i].putProperty("type", "wind");
			msgs[i].putProperty("speed", wd.force());
			msgs[i].putProperty("source", "pub-" + (i < 5 ? (i + 1) : "late"));
		}

		// ---- 8 Subscribers (mixed service classes) ----
		//  wait-sub-1 FREE, wait-sub-2 STANDARD, wait-sub-3 PREMIUM
		//  future-sub-1 STANDARD, future-sub-2 PREMIUM
		//  normal-sub-1 FREE, normal-sub-2 FREE, normal-sub-3 STANDARD
		String[]            subNames = { WAIT_SUB_1, WAIT_SUB_2, WAIT_SUB_3,  FUTURE_SUB_1, FUTURE_SUB_2, NORMAL_SUB_1, NORMAL_SUB_2, NORMAL_SUB_3 };
		RegistrationClass[] subRC    = { RegistrationClass.FREE, RegistrationClass.STANDARD, RegistrationClass.PREMIUM,
		                                 RegistrationClass.STANDARD, RegistrationClass.PREMIUM,
		                                 RegistrationClass.FREE, RegistrationClass.FREE, RegistrationClass.STANDARD };
		for (int i = 0; i < subNames.length; i++) {
			AbstractComponent.createComponent(Windmill.class.getCanonicalName(),
					new Object[] { subNames[i], brokerURI, subRC[i],
							"channel0", matchAll, scenario });
		}

		// ---- 6 Publishers (mixed service classes) ----
		//  pub-1..3 FREE (sync publish), pub-4 STANDARD (async), pub-5 PREMIUM (async), pub-late FREE
		String[]            pubNames = { PUB_1, PUB_2, PUB_3, PUB_4, PUB_5, PUB_6 };
		RegistrationClass[] pubRC    = { RegistrationClass.FREE, RegistrationClass.FREE, RegistrationClass.FREE,
		                                 RegistrationClass.STANDARD, RegistrationClass.PREMIUM, RegistrationClass.FREE };
		for (int i = 0; i < 6; i++) {
			AbstractComponent.createComponent(MeteoStation.class.getCanonicalName(),
					new Object[] { pubNames[i], brokerURI, pubRC[i],
							"channel0", msgs[i], scenario });
		}

		super.deploy();
	}

	// -------------------------------------------------------------------------
	// Main
	// -------------------------------------------------------------------------

	public static void main(String[] args) {
		try {
			CVM_AsyncTest cvm = new CVM_AsyncTest();
			cvm.startStandardLifeCycle(15000L);
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
