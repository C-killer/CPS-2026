package fr.sorbonne_u.publication;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;

import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.messages.MessageFilter;
import fr.sorbonne_u.publication.components.Client_before_plugins;

/**
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class CVM_Audit1 extends AbstractCVM {
	public CVM_Audit1() throws Exception {
		super();
	}

	public static final int NB_CHANNELS = 3;

	@Override
	// deploy():

	// 创建1个Broker,初始化3个频道。

	// 创建1个订阅者Client,监听channel0。

	// 创建1个发布者Client,准备向channel0发送demo消息。
	public void deploy() throws Exception {
		// 1) Broker
		AbstractComponent.createComponent(
				Broker.class.getCanonicalName(),
				new Object[] { NB_CHANNELS });

		// 2) Subscriber client
		// Subscriber client (5 params)
		AbstractComponent.createComponent(
				Client_before_plugins.class.getCanonicalName(),
				new Object[] {
						"client-subscriber-receiving",
						Broker.registrationPortURI(),
						RegistrationClass.FREE,
						"channel0",
						new MessageFilter(null, null, null) // 注意：这里不是 null
				});

		// Publisher client (5 params)
		AbstractComponent.createComponent(
				Client_before_plugins.class.getCanonicalName(),
				new Object[] {
						"client-publisher-receiving",
						Broker.registrationPortURI(),
						RegistrationClass.FREE,
						"channel0",
						new fr.sorbonne_u.messages.Message("demo")
				});

		super.deploy();
	}

	public static void main(String[] args) {
		try {
			CVM_Audit1 cvm = new CVM_Audit1();
			// standard BCM lifecycle: deploy -> start -> execute -> finalise -> shutdown
			cvm.startStandardLifeCycle(5000L);
			System.exit(0);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}