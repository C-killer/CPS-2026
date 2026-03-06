package fr.sorbonne_u.publication.components;

import java.rmi.RemoteException;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.MessageFilter;
import fr.sorbonne_u.publication.plugins.ClientPublicationPlugin;
import fr.sorbonne_u.publication.plugins.ClientRegistrationPlugin;
import fr.sorbonne_u.publication.plugins.ClientSubscriptionPlugin;

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

	// plugins
	protected ClientRegistrationPlugin registrationPlugin;
	protected ClientPublicationPlugin publicationPlugin;
	protected ClientSubscriptionPlugin subscriptionPlugin;

	// constructeur
	public Client(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageFilterI filter,
			MessageI messageToPublish) throws Exception {
		super(1, 0);

		this.receivingInboundURI = receivingInboundURI;
		this.brokerRegistrationInboundURI = brokerRegistrationInboundURI;
		this.serviceClass = serviceClass;

		this.channel = channel;
		this.filter = filter;
		this.messageToPublish = messageToPublish;

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
				null);
	}

	/**
	 * Initialise les plugins.
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

		// subscribe if channel exists
		if (this.channel != null) {

			this.subscriptionPlugin.subscribe(
					this.channel,
					(this.filter != null) ? this.filter : new MessageFilter(null, null, null));

			System.out.println("[" + this.receivingInboundURI +
					"] subscribed to " + this.channel);
		}

		// publish message if provided
		if (this.messageToPublish != null) {

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
}