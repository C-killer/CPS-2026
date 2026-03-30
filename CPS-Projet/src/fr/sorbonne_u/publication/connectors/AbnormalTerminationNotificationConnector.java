package fr.sorbonne_u.publication.connectors;

import fr.sorbonne_u.components.connectors.AbstractConnector;
import fr.sorbonne_u.publication.interfaces.AbnormalTerminationNotificationCI;

/**
 * Connector relaying abnormal-termination notifications from broker
 * to client.
 * 连接器负责将异常终止通知从代理转发给客户端。
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class AbnormalTerminationNotificationConnector
		extends AbstractConnector
		implements AbnormalTerminationNotificationCI {

	@Override
	public void notifyAbnormalTermination(Exception e) throws Exception {
		((AbnormalTerminationNotificationCI) this.offering)
				.notifyAbnormalTermination(e);
	}
}
