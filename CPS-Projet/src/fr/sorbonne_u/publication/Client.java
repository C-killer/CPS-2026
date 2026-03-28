package fr.sorbonne_u.publication;

import java.rmi.RemoteException;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.MessageFilter;
import fr.sorbonne_u.publication.implementations.ReceivingImplI;
import fr.sorbonne_u.publication.plugins.ClientPublicationPlugin;
import fr.sorbonne_u.publication.plugins.ClientRegistrationPlugin;
import fr.sorbonne_u.publication.plugins.ClientSubscriptionPlugin;
import fr.sorbonne_u.utils.aclocks.AcceleratedClock;
import fr.sorbonne_u.utils.aclocks.ClocksServer;
import fr.sorbonne_u.utils.aclocks.ClocksServerCI;
import fr.sorbonne_u.utils.aclocks.ClocksServerConnector;
import fr.sorbonne_u.utils.aclocks.ClocksServerOutboundPort;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class Client extends AbstractComponent implements ReceivingImplI {

	protected final String receivingInboundURI;
	protected final String brokerRegistrationInboundURI;
	protected final RegistrationClass serviceClass;
	protected final String channel;
	protected final MessageFilterI filter;
	protected final MessageI messageToPublish;
	protected final TestScenario scenario;

	protected ClientRegistrationPlugin registrationPlugin;
	protected ClientPublicationPlugin publicationPlugin;
	protected ClientSubscriptionPlugin subscriptionPlugin;

	protected Client(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageFilterI filter,
			MessageI messageToPublish) throws Exception {
		super(receivingInboundURI, 2, 1);

		this.receivingInboundURI = receivingInboundURI;
		this.brokerRegistrationInboundURI = brokerRegistrationInboundURI;
		this.serviceClass = serviceClass;
		this.channel = channel;
		this.filter = filter;
		this.messageToPublish = messageToPublish;
		this.scenario = null;

		this.initialisePlugins();
	}

	protected Client(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageFilterI filter,
			MessageI messageToPublish,
			TestScenario scenario) throws Exception {
		super(receivingInboundURI, 2, 1);

		this.receivingInboundURI = receivingInboundURI;
		this.brokerRegistrationInboundURI = brokerRegistrationInboundURI;
		this.serviceClass = serviceClass;
		this.channel = channel;
		this.filter = filter;
		this.messageToPublish = messageToPublish;
		this.scenario = scenario;

		this.initialisePlugins();
	}

	protected Client(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel) throws Exception {
		this(receivingInboundURI,
				brokerRegistrationInboundURI,
				serviceClass,
				channel,
				null,
				null,
				null);
	}

	protected Client(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			TestScenario scenario) throws Exception {
		this(receivingInboundURI,
				brokerRegistrationInboundURI,
				serviceClass,
				channel,
				null,
				null,
				scenario);
	}

	protected void initialisePlugins() throws Exception {

		this.registrationPlugin = new ClientRegistrationPlugin();
		this.registrationPlugin.setPluginURI("reg-plugin-" + this.receivingInboundURI);
		this.registrationPlugin.configure(
				this.receivingInboundURI,
				this.brokerRegistrationInboundURI,
				this.serviceClass);

		this.publicationPlugin = new ClientPublicationPlugin();
		this.publicationPlugin.setPluginURI("pub-plugin-" + this.receivingInboundURI);
		this.publicationPlugin.configure(
				this.receivingInboundURI,
				this.brokerRegistrationInboundURI,
				this.serviceClass);

		this.subscriptionPlugin = new ClientSubscriptionPlugin();
		this.subscriptionPlugin.setPluginURI("sub-plugin-" + this.receivingInboundURI);
		this.subscriptionPlugin.configure(
				this.receivingInboundURI,
				this.brokerRegistrationInboundURI,
				this.serviceClass);

		this.installPlugin(this.registrationPlugin);
		this.installPlugin(this.publicationPlugin);
		this.installPlugin(this.subscriptionPlugin);
	}

	public ClientRegistrationPlugin getRegistrationPlugin() {
		return this.registrationPlugin;
	}

	public ClientPublicationPlugin getPublicationPlugin() {
		return this.publicationPlugin;
	}

	public ClientSubscriptionPlugin getSubscriptionPlugin() {
		return this.subscriptionPlugin;
	}

	@Override
	public synchronized void start() throws ComponentStartException {
		try {
			super.start();
		} catch (Exception e) {
			throw new ComponentStartException("Client.start failed", e);
		}
	}

	@Override
	public void execute() throws Exception {

		System.out.println("[" + this.receivingInboundURI + "] execute begin");

		this.registrationPlugin.register(this.serviceClass);

		String brokerPublishingInboundURI = this.registrationPlugin.getBrokerPublishingInboundURI();

		this.logMessage("Registered. Publishing inbound URI = " +
				brokerPublishingInboundURI);

		this.publicationPlugin.connectToBroker(brokerPublishingInboundURI);

		if (this.scenario != null) {
			this.logMessage("Executing scenario...");

			this.addRequiredInterface(ClocksServerCI.class);
			ClocksServerOutboundPort clockPort = new ClocksServerOutboundPort(this);
			clockPort.publishPort();
			this.doPortConnection(
					clockPort.getPortURI(),
					ClocksServer.STANDARD_INBOUNDPORT_URI,
					ClocksServerConnector.class.getCanonicalName());

			AcceleratedClock rawClock = clockPort.getClock(this.scenario.getClockURI());
			rawClock.waitUntilStart();
			AcceleratedClock clock = new AcceleratedClock(
					rawClock.getClockURI(),
					rawClock.getStartEpochNanos(),
					rawClock.getStartInstant(),
					this.scenario.getEndInstant(),
					rawClock.getAccelerationFactor());

			String myURI = this.receivingInboundURI;

			while (!this.scenario.scenarioTerminated(myURI)) {
				this.scenario.scheduleNextStep(myURI, this, clock);
				this.scenario.advanceToNextStep(myURI);
			}

			this.doPortDisconnection(clockPort.getPortURI());
			clockPort.unpublishPort();
			clockPort.destroyPort();
			this.removeRequiredInterface(ClocksServerCI.class);

			return;
		}

		if (this.channel != null) {
			this.subscriptionPlugin.subscribe(
					this.channel,
					(this.filter != null) ? this.filter : new MessageFilter(null, null, null));

			System.out.println("[" + this.receivingInboundURI +
					"] subscribed to " + this.channel);
		}

		if (this.messageToPublish != null
				&& !"dummy".equals(this.messageToPublish.getPayload())) {

			Thread.sleep(300);

			this.publicationPlugin.publish(
					this.channel,
					this.messageToPublish);

			System.out.println("[" + this.receivingInboundURI +
					"] published to " + this.channel);
		}
	}

	// Utiliser runTask pour transformer la méthode de réception des messages en une
	// exécution asynchrone et non bloquante
	@Override
	public void receive(String channel, MessageI message) throws RemoteException {
		try {
			this.runTask(c -> {
				try {
					((Client) c).getSubscriptionPlugin().receive(channel, message);
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		} catch (Exception e) {
			throw new RemoteException("Error async receive", e);
		}
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws RemoteException {
		try {
			this.runTask(c -> {
				try {
					((Client) c).getSubscriptionPlugin().receive(channel, messages);
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
		} catch (Exception e) {
			throw new RemoteException("Error async batch receive", e);
		}
	}

	public boolean hasCreatedChannel(String channel) throws Exception {
		return this.publicationPlugin.hasCreatedChannel(channel);
	}

	public boolean isAuthorisedUser(String channel, String uri) throws Exception {
		return this.publicationPlugin.isAuthorisedUser(channel, uri);
	}

	public void modifyAuthorisedUsers(String channel, String autorisedUsers) throws Exception {
		this.publicationPlugin.modifyAuthorisedUsers(channel, autorisedUsers);
	}

	public void removeAuthorisedUsers(String channel, String regularExpression) throws Exception {
		this.publicationPlugin.removeAuthorisedUsers(channel, regularExpression);
	}

	public void createChannel(String channel, String autorisedUsers) throws Exception {
		this.publicationPlugin.createChannel(channel, autorisedUsers);
	}

	public void destroyChannel(String channel) throws Exception {
		this.publicationPlugin.destroyChannel(channel);
	}

	public void destroyChannelNow(String channel) throws Exception {
		this.publicationPlugin.destroyChannelNow(channel);
	}

	public boolean channelQuotaReached() throws Exception {
		return this.publicationPlugin.channelQuotaReached();
	}

	public void runScenarioCreateChannels() {
		try {
			System.out.println("[" + this.receivingInboundURI + "] Scenario: create channel " + this.channel);
			this.createChannel(this.channel, ".*");
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void runScenarioSubscribe() {
		try {
			System.out.println(("[" + this.receivingInboundURI + "] Scenario: subscribe to " + this.channel));

			this.subscriptionPlugin.subscribe(
					this.channel,
					(this.filter != null) ? this.filter : new MessageFilter(null, null, null));

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void runScenarioPublish() {
		try {

			if (this.messageToPublish != null) {

				System.out.println(("[" + this.receivingInboundURI + "] Scenario: publish to " + this.channel));

				this.publicationPlugin.publish(
						this.channel,
						this.messageToPublish);
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void runScenarioDestroyChannel() {
		try {
			System.out.println("[" + this.receivingInboundURI + "] Scenario: destroy channel " + this.channel);
			this.destroyChannel(this.channel);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}