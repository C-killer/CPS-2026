package fr.sorbonne_u.publication.interfaces;

import fr.sorbonne_u.components.interfaces.OfferedCI;
import fr.sorbonne_u.components.interfaces.RequiredCI;

/**
 * Component interface for asynchronous abnormal-termination notifications.
 *
 * <p>When the broker executes an {@code asyncPublishAndNotify} task and
 * the task terminates abnormally (throws an exception), the broker
 * connects to the client's inbound port offering this interface and
 * calls {@link #notifyAbnormalTermination} so the client can react.</p>
 *
 * @author PENG Kairui
 * @author CHU Feiyang
 */
public interface AbnormalTerminationNotificationCI
		extends OfferedCI, RequiredCI {

	/**
	 * Notify the component that an asynchronous task terminated abnormally.
	 *
	 * @param e the exception that caused the abnormal termination.
	 */
	void notifyAbnormalTermination(Exception e) throws Exception;
}
