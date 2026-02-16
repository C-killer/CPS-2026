package fr.sorbonne_u.publication;

import java.rmi.RemoteException;
import java.time.Instant;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.publication.connectors.PublishingConnector;
import fr.sorbonne_u.publication.connectors.RegistrationConnector;
import fr.sorbonne_u.publication.implementations.ReceivingImplI;
import fr.sorbonne_u.publication.ports.PublishingOutbound;
import fr.sorbonne_u.publication.ports.ReceivingInbound;
import fr.sorbonne_u.publication.ports.RegistrationOutbound;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;

import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.messages.MessageFilter;

public class Client extends AbstractComponent implements ReceivingImplI {

	public enum Role {
		SUBSCRIBER, PUBLISHER
	}

	// ----- configuration -----
	protected final String receivingInboundURI; // must be used as receptionPortURI
	protected final String brokerRegistrationInboundURI;
	protected final RegistrationClass serviceClass;
	protected final Role role;
	protected final String channel;

	// subscriber-only
	protected final MessageFilterI filter;

	// publisher-only
	protected final MessageI messageToPublish;

	// ----- ports -----
	protected final ReceivingInbound receivingInboundPort; // offers ReceivingCI
	protected final RegistrationOutbound registrationOutbound; // requires RegistrationCI
	protected final PublishingOutbound publishingOutbound; // requires PublishingCI

	// set after register
	protected String brokerPublishingInboundURI;

	// 通用构造函数
	public Client(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			Role role,
			String channel,
			MessageFilterI filter,
			MessageI messageToPublish) throws Exception {
		super(1, 0);

		this.receivingInboundURI = receivingInboundURI;
		this.brokerRegistrationInboundURI = brokerRegistrationInboundURI;
		this.serviceClass = serviceClass;
		this.role = role;
		this.channel = channel;
		this.filter = filter;
		this.messageToPublish = messageToPublish;

		// offers ReceivingCI
		this.receivingInboundPort = new ReceivingInbound(this.receivingInboundURI, this);
		this.receivingInboundPort.publishPort();

		// requires RegistrationCI + PublishingCI
		this.registrationOutbound = new RegistrationOutbound(this);
		this.registrationOutbound.publishPort();

		this.publishingOutbound = new PublishingOutbound(this);
		this.publishingOutbound.publishPort();
	}

	// Subscriber constructor (no null args)
	// 默认subscriber构造函数
	public Client(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageFilterI filter) throws Exception {
		super(1, 0);

		this.receivingInboundURI = receivingInboundURI;
		this.brokerRegistrationInboundURI = brokerRegistrationInboundURI;
		this.serviceClass = serviceClass;
		this.role = Role.SUBSCRIBER;
		this.channel = channel;
		this.filter = filter;
		this.messageToPublish = null;

		this.receivingInboundPort = new ReceivingInbound(this.receivingInboundURI, this);
		this.receivingInboundPort.publishPort();

		this.registrationOutbound = new RegistrationOutbound(this);
		this.registrationOutbound.publishPort();

		this.publishingOutbound = new PublishingOutbound(this);
		this.publishingOutbound.publishPort();
	}

	// Publisher constructor (no null args)
	// 默认publisher构造函数
	public Client(
			String receivingInboundURI,
			String brokerRegistrationInboundURI,
			RegistrationClass serviceClass,
			String channel,
			MessageI messageToPublish) throws Exception {
		super(1, 0);

		this.receivingInboundURI = receivingInboundURI;
		this.brokerRegistrationInboundURI = brokerRegistrationInboundURI;
		this.serviceClass = serviceClass;
		this.role = Role.PUBLISHER;
		this.channel = channel;
		this.filter = null;
		this.messageToPublish = messageToPublish;

		this.receivingInboundPort = new ReceivingInbound(this.receivingInboundURI, this);
		this.receivingInboundPort.publishPort();

		this.registrationOutbound = new RegistrationOutbound(this);
		this.registrationOutbound.publishPort();

		this.publishingOutbound = new PublishingOutbound(this);
		this.publishingOutbound.publishPort();
	}

	@Override // 将registrationOutBound连接到Broker注册入口端口,交互基础
	public synchronized void start() {
		try {
			super.start();

			// Connect RegistrationOutbound -> Broker RegistrationInbound
			this.registrationOutbound.doConnection(
					this.brokerRegistrationInboundURI,
					RegistrationConnector.class.getCanonicalName());

		} catch (Exception e) {
			throw new RuntimeException("Client.start failed", e);
		}
	}

	@Override // 注册,调用register,会获取
				// Broker返回的brokerPublishingInboundURI,这是发布消息所需的动态地址,拿到地址后,立即连接publishingOutbound
	public void execute() throws Exception {
		// 1) register (receptionPortURI == receivingInboundURI)
		try {
			System.out.println("[" + this.receivingInboundURI + "] execute: begin, role=" + this.role);
			this.brokerPublishingInboundURI = this.registrationOutbound.register(this.receivingInboundURI,
					this.serviceClass);
			this.logMessage("Registered. Publishing inbound URI = " + this.brokerPublishingInboundURI);
		} catch (Exception e) {
			// Will catch AlreadyRegisteredException too (checked) if you added it
			// everywhere
			this.logMessage("Register failed: " + e.getClass().getSimpleName() + " : " + e.getMessage());
			throw e;
		}
		System.out.println(
				"[" + this.receivingInboundURI + "] registered, publishingURI=" + this.brokerPublishingInboundURI);

		// 2) connect PublishingOutbound -> broker publishing inbound
		this.publishingOutbound.doConnection(
				this.brokerPublishingInboundURI,
				PublishingConnector.class.getCanonicalName());

		// 3) role behavior
		switch (this.role) {
			case SUBSCRIBER:
				doSubscribe();
				System.out.println("[" + this.receivingInboundURI + "] subscribed to " + this.channel);
				break;
			case PUBLISHER:
				// give subscriber time to subscribe (simple deterministic demo)
				Thread.sleep(300);
				doPublish();
				System.out.println("[" + this.receivingInboundURI + "] published to " + this.channel);
				break;
			default:
				throw new IllegalStateException("Unknown role " + this.role);
		}
	}

	// 通过registrationOutbound告诉Broker:“我想接收channel频道里符合 filter 条件的消息”
	protected void doSubscribe() throws Exception {
		MessageFilterI f = (this.filter != null) ? this.filter : new MessageFilter(null, null, null);
		this.registrationOutbound.subscribe(this.receivingInboundURI, this.channel, f);
		this.logMessage("Subscribed to " + this.channel);// print
	}

	// 准备好要发送的MessageI
	// 通过publishingOutbound将消息推送到Broker
	protected void doPublish() throws Exception {
		MessageI m = (this.messageToPublish != null) ? this.messageToPublish : defaultMessage();
		this.publishingOutbound.publish(this.receivingInboundURI, this.channel, m);
		this.logMessage("Published 1 message to " + this.channel);
	}

	// 默认生成如风速的消息用于测试
	protected MessageI defaultMessage() throws Exception {
		// Minimal message with timestamp + a couple of properties (adjust if your
		// Message ctor differs)
		Message msg = new Message("demo", Instant.now());
		msg.putProperty("type", "wind");
		msg.putProperty("speed", 42);
		return msg;
	}

	// ----- ReceivingImplI -----
	@Override
	public void receive(String channel, MessageI message) throws RemoteException {
		System.out.println(
				"[" + this.receivingInboundURI + "] receive: begin, channel=" + channel + ", message=" + message);
		this.logMessage("RECEIVED on " + channel + " : " + message);
	}

	@Override
	public void receive(String channel, MessageI[] messages) throws RemoteException {
		this.logMessage("RECEIVED batch on " + channel + " : " + (messages == null ? 0 : messages.length));
	}

	@Override // 断连
	public synchronized void finalise() throws Exception {
		// disconnect outbounds if connected
		try {
			this.publishingOutbound.doDisconnection();
		} catch (Exception ignored) {
		}
		try {
			this.registrationOutbound.doDisconnection();
		} catch (Exception ignored) {
		}
		super.finalise();
	}

	@Override // 销毁端口
	public synchronized void shutdown() throws RuntimeException, ComponentShutdownException {
		try {
			this.receivingInboundPort.unpublishPort();
		} catch (Exception ignored) {
		}
		try {
			this.publishingOutbound.unpublishPort();
		} catch (Exception ignored) {
		}
		try {
			this.registrationOutbound.unpublishPort();
		} catch (Exception ignored) {
		}
		super.shutdown();
	}
}