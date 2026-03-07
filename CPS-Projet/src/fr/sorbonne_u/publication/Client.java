package fr.sorbonne_u.publication;

import java.rmi.RemoteException;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.MessageFilter;
import fr.sorbonne_u.publication.plugins.ClientPublicationPlugin;
import fr.sorbonne_u.publication.plugins.ClientRegistrationPlugin;
import fr.sorbonne_u.publication.plugins.ClientSubscriptionPlugin;
import fr.sorbonne_u.utils.aclocks.AcceleratedClock;
import fr.sorbonne_u.utils.aclocks.ClocksServer;
import fr.sorbonne_u.utils.aclocks.ClocksServerConnector;
import fr.sorbonne_u.utils.aclocks.ClocksServerOutboundPort;

/*
 * Version pluginisée du client.
 *
 * Cette classe remplace l'ancienne approche monolithique du composant client
 * par une décomposition en rôles, chacun porté par un greffon spécialisé :
 * - ClientRegistrationPlugin : gestion de l'enregistrement auprès du broker ;
 * - ClientPublicationPlugin  : gestion de la publication des messages ;
 * - ClientSubscriptionPlugin : gestion des abonnements et de la réception.
 */

public class Client extends AbstractComponent implements ReceivingCI {

	protected final String receivingInboundURI;
	protected final String brokerRegistrationInboundURI;
	protected final RegistrationClass serviceClass;

	/**
	 * channel pour publication ou subscription
	 * On supprime role pour realiser d'etre publisher et subscriber en meme temps
	 */
	protected final String channel;

	/** optional message filter */
	protected final MessageFilterI filter;

	/** optional message to publish */
	protected final MessageI messageToPublish;

	protected final TestScenario scenario;

	// plugins
	protected ClientRegistrationPlugin registrationPlugin;
	protected ClientPublicationPlugin publicationPlugin;
	protected ClientSubscriptionPlugin subscriptionPlugin;

	// constructeur
	/*
	 * Nous avons conçu deux constructeurs de base :
	 * l'un permet d'initialiser l'ensemble des attributs du composant Client,
	 * tandis que l'autre en constitue une version simplifiée ne conservant que les
	 * paramètres de configuration essentiels et fixant filter et messageToPublish à
	 * null.
	 * Cette version simplifiée permet au client de s'adapter plus facilement aux
	 * différents rôles, tels que publisher ou subscriber.
	 * 
	 * Par ailleurs, chacun de ces deux constructeurs possède également une variante
	 * avec paramètre TestScenario, afin de permettre l'exécution des composants
	 * dans un scénario de test avec contrôle temporel.
	 * 
	 * 我们设计了两个基础构造函数：
	 * 一个用于初始化 Client 的全部属性；
	 * 另一个是简化版本，只保留基本配置参数，并将 filter 和 messageToPublish 设为 null。这种简化版本使 Client
	 * 能够更加灵活地适应 publisher 或 subscriber 等不同角色的功能需求。
	 * 
	 * 此外，我们还分别为这两个基础构造函数设计了带 TestScenario 参数的版本，以支持在测试场景中按照预定时间顺序执行组件的操作。
	 */
	public Client(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageFilterI filter,
			MessageI messageToPublish) throws Exception {
		super(1, 1);

		this.receivingInboundURI = receivingInboundURI;
		this.brokerRegistrationInboundURI = brokerRegistrationInboundURI;
		this.serviceClass = serviceClass;

		this.channel = channel;
		this.filter = filter;
		this.messageToPublish = messageToPublish;
		this.scenario = null;

		this.initialisePlugins();
	}

	public Client(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageFilterI filter,
			MessageI messageToPublish,
			TestScenario scenario) throws Exception {
		super(1, 1);

		this.receivingInboundURI = receivingInboundURI;
		this.brokerRegistrationInboundURI = brokerRegistrationInboundURI;
		this.serviceClass = serviceClass;

		this.channel = channel;
		this.filter = filter;
		this.messageToPublish = messageToPublish;
		this.scenario = scenario;

		this.initialisePlugins();
	}

	public Client(
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

	public Client(
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

	/**
	 * Initialise les plugins.
	 */

	/*
	 * La méthode initialisePlugins() installe et initialise les différents greffons
	 * du client en fonction de sa configuration.
	 * Ces greffons prennent en charge les principales interactions avec le broker,
	 * notamment l’enregistrement, la publication et la souscription.
	 * 
	 * initialisePlugins() 方法根据 Client 在构造时的配置初始化并安装相应的插件，并通过这些插件实现客户端与 broker
	 * 之间的注册、发布和订阅等功能。
	 * 
	 */
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
	public synchronized void start() {
		try {
			super.start();

			this.registrationPlugin.initialise();
			this.publicationPlugin.initialise();
			this.subscriptionPlugin.initialise();

		} catch (Exception e) {
			throw new RuntimeException("Client.start failed", e);
		}
	}

	@Override
	public void execute() throws Exception {

		System.out.println("[" + this.receivingInboundURI + "] execute begin");

		// register to broker
		this.registrationPlugin.register(this.serviceClass);

		String brokerPublishingInboundURI = this.registrationPlugin.getBrokerPublishingInboundURI();

		this.logMessage("Registered. Publishing inbound URI = " +
				brokerPublishingInboundURI);

		// connect publication plugin
		this.publicationPlugin.connectToBroker(brokerPublishingInboundURI);

		if (this.scenario != null) {
			this.logMessage("Executing scenario...");

			// 1. 获取时钟
			ClocksServerOutboundPort clockPort = new ClocksServerOutboundPort(this);
			clockPort.publishPort();
			this.doPortConnection(
					clockPort.getPortURI(),
					ClocksServer.STANDARD_INBOUNDPORT_URI,
					ClocksServerConnector.class.getCanonicalName());

			AcceleratedClock clock = clockPort.getClock(this.scenario.getClockURI());

			// 2. 等待时钟启动
			clock.waitUntilStart();

			// 3. 调度该组件的所有步骤
			String myURI = this.receivingInboundURI;

			while (!this.scenario.scenarioTerminated(myURI)) {
				this.scenario.scheduleNextStep(myURI, this, clock);
				this.scenario.advanceToNextStep(myURI);
			}

			this.doPortDisconnection(clockPort.getPortURI());
			clockPort.unpublishPort();
			clockPort.destroyPort();

			// scenario 模式下不执行默认 subscribe/publish
			return;
		}

		// subscribe if channel exists
		if (this.channel != null) {

			this.subscriptionPlugin.subscribe(
					this.channel,
					(this.filter != null) ? this.filter : new MessageFilter(null, null, null));

			System.out.println("[" + this.receivingInboundURI +
					"] subscribed to " + this.channel);
		}

		// publish message if provided
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

	/*
	 * Recevoir les messages
	 */
	@Override
	public void receive(String channel, MessageI message)
			throws RemoteException {
		this.subscriptionPlugin.receive(channel, message);
	}

	@Override
	public void receive(String channel, MessageI[] messages)
			throws RemoteException {
		this.subscriptionPlugin.receive(channel, messages);
	}

	public void createChannel(String channel) throws Exception {
		this.publicationPlugin.createChannel(channel);
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
			System.out.println("Scenario: create channel " + this.channel);
			this.createChannel(this.channel);
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
}