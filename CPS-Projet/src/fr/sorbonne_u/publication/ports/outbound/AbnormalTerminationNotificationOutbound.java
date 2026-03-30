package fr.sorbonne_u.publication.ports.outbound;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractOutboundPort;
import fr.sorbonne_u.publication.interfaces.AbnormalTerminationNotificationCI;

/**
 * Outbound port used by the broker to send abnormal-termination
 * notifications to a client.
 * 代理用于向client发送异常终止通知的出站端口。
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class AbnormalTerminationNotificationOutbound
		extends AbstractOutboundPort
		implements AbnormalTerminationNotificationCI {

	private static final long serialVersionUID = 1L;

	public AbnormalTerminationNotificationOutbound(ComponentI owner) throws Exception {
		super(AbnormalTerminationNotificationCI.class, owner);
	}

	@Override
	public void notifyAbnormalTermination(Exception e) throws Exception {
		((AbnormalTerminationNotificationCI) this.getConnector())
				.notifyAbnormalTermination(e);
	}
}
