package fr.sorbonne_u.publication.ports.inbound;

import fr.sorbonne_u.components.ComponentI;
import fr.sorbonne_u.components.ports.AbstractInboundPort;
import fr.sorbonne_u.publication.interfaces.AbnormalTerminationNotificationCI;

/**
 * Inbound port installed on a client component to receive
 * abnormal-termination notifications from the broker.
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public class AbnormalTerminationNotificationInbound
		extends AbstractInboundPort
		implements AbnormalTerminationNotificationCI {

	private static final long serialVersionUID = 1L;

	public AbnormalTerminationNotificationInbound(
			String uri, ComponentI owner) throws Exception {
		super(uri, AbnormalTerminationNotificationCI.class, owner);
	}

	@Override
	public void notifyAbnormalTermination(Exception e) throws Exception {
		this.getOwner().runTask(o -> {
			System.err.println("[" + o.toString()
					+ "] async publish abnormal termination: " + e.getMessage());
		});
	}
}
