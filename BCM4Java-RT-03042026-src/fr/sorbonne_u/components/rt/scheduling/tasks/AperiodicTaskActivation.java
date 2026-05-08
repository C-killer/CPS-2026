package fr.sorbonne_u.components.rt.scheduling.tasks;

// Copyright Jacques Malenfant, Sorbonne Universite.
// Jacques.Malenfant@lip6.fr
//
// This software is a computer program whose purpose is to provide a
// basic component programming model to program with components
// real time distributed applications in the Java programming language.
//
// This software is governed by the CeCILL-C license under French law and
// abiding by the rules of distribution of free software.  You can use,
// modify and/ or redistribute the software under the terms of the
// CeCILL-C license as circulated by CEA, CNRS and INRIA at the following
// URL "http://www.cecill.info".
//
// As a counterpart to the access to the source code and  rights to copy,
// modify and redistribute granted by the license, users are provided only
// with a limited warranty  and the software's author,  the holder of the
// economic rights,  and the successive licensors  have only  limited
// liability. 
//
// In this respect, the user's attention is drawn to the risks associated
// with loading,  using,  modifying and/or developing or reproducing the
// software by the user in light of its specific status of free software,
// that may mean  that it is complicated to manipulate,  and  that  also
// therefore means  that it is reserved for developers  and  experienced
// professionals having in-depth computer knowledge. Users are therefore
// encouraged to load and test the software's suitability as regards their
// requirements in conditions enabling the security of their systems and/or 
// data to be ensured and,  more generally, to use and operate it in the 
// same conditions as regards security. 
//
// The fact that you are presently reading this means that you have had
// knowledge of the CeCILL-C license and that you accept its terms.

import fr.sorbonne_u.components.rt.asynccall.AbstractRTAsyncCall;
import fr.sorbonne_u.components.rt.scheduling.SchedulingI;
import fr.sorbonne_u.exceptions.AssertionChecking;
import fr.sorbonne_u.exceptions.PreconditionException;
import java.io.Serializable;

// -----------------------------------------------------------------------------
/**
 * The class <code>AperiodicTaskActivation</code> implements an aperiodic task
 * activation for BCM4Java real time schedulers that is executed by calling a
 * method (properly annotated with {@code AperiodicTaskDescription}).
 *
 * <p><strong>Description</strong></p>
 * 
 * <p><strong>Implementation Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code task instanceof AbstractAperiodicTask}
 * invariant	{@code params != null}
 * invariant	{@code task.isVarArgs() || task.getNumberOfParameters() == params.length}
 * </pre>
 * 
 * <p><strong>Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * <p>Created on : 2023-04-07</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public class			AperiodicTaskActivation
extends		AbstractRealTimeTaskActivation
{
	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	/** when true, the scheduler makes debugging traces of its actions
	 *  through the owner component tracing facility.						*/
	public static boolean			DEBUG = false;

	/** reference to the asynchronous call object allowing to send back
	 *  a result when necessary.											*/
	protected AbstractRTAsyncCall	resultSendingProxy;

	// -------------------------------------------------------------------------
	// Invariants
	// -------------------------------------------------------------------------

	/**
	 * return true if the implementation invariants are observed, false otherwise.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code instance != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param instance	instance to be tested.
	 * @return			true if the implementation invariants are observed, false otherwise.
	 */
	protected static boolean	implementationInvariants(
		AperiodicTaskActivation instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		ret &= AssertionChecking.checkImplementationInvariant(
				instance.task instanceof AbstractAperiodicTask,
				AperiodicTaskActivation.class,
				instance,
				"task instanceof AbstractAperiodicTask");
		ret &= AssertionChecking.checkImplementationInvariant(
				instance.params != null,
				AperiodicTaskActivation.class,
				instance,
				"params != null");
		ret &= AssertionChecking.checkImplementationInvariant(
				instance.task.isVarArgs() ||
						instance.task.getNumberOfParameters() ==
													instance.params.length,
				AperiodicTaskActivation.class,
				instance,
				"task.isVarArgs() || task.getNumberOfParameters() =="
				+ "params.length");
		return ret;
	}

	/**
	 * return true if the invariants are observed, false otherwise.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code instance != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param instance	instance to be tested.
	 * @return			true if the invariants are observed, false otherwise.
	 */
	protected static boolean	invariants(
		AperiodicTaskActivation instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		return ret;
	}

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	/**
	 * create a new aperiodic task activation with no parameters.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code task != null}
	 * pre	{@code scheduler != null}
	 * pre	{@code activationTime > 0}
	 * post	{@code getAbsoluteDeadline() >= getActivationTime() + getWcet()}
	 * post	{@code getFutureResult() != null}
	 * </pre>
	 *
	 * @param task				task from which this activation is an executable instance.
	 * @param scheduler			scheduler responsible to trigger the execution of the task activations; associated with the owner component.
	 * @param activationTime	real time at which the task is activated in Unix epoch time and in nanoseconds.
	 */
	public				AperiodicTaskActivation(
		AbstractAperiodicTask task,
		SchedulingI scheduler,
		long activationTime
		)
	{
		this(task, scheduler, activationTime, new Object[]{}, null);
	}

	/**
	 * create a new aperiodic task activation.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code task != null}
	 * pre	{@code scheduler != null}
	 * pre	{@code activationTime > 0}
	 * pre	{@code params != null}
	 * pre	{@code task.isVarArgs() || task.getNumberOfParameters() == params.length}
	 * post	{@code getAbsoluteDeadline() >= getActivationTime() + getWcet()}
	 * post	{@code getFutureResult() != null}
	 * </pre>
	 *
	 * @param task					task from which this activation is an executable instance.
	 * @param scheduler				scheduler responsible to trigger the execution of the task activations; associated with the owner component.
	 * @param activationTime		real time at which the task is activated in Unix epoch time and in nanoseconds.
	 * @param params				parameters to be passed to the service when calling it.
	 * @param resultSendingProxy	reference to the asynchronous call object allowing to send back a result when necessary.
	 */
	public				AperiodicTaskActivation(
		AbstractAperiodicTask task,
		SchedulingI scheduler,
		long activationTime,
		Object[] params,
		AbstractRTAsyncCall resultSendingProxy
		)
	{
		super(task, scheduler, activationTime, params);

		this.resultSendingProxy = resultSendingProxy;
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTaskActivation#execute()
	 */
	@Override
	public void			execute() throws Exception
	{
		if (DEBUG) {
			this.scheduler.getOwner().logMessage(
					"AperiodicTaskActivation::execute "
					+ this.task.getName() + " "
					+ Thread.currentThread());
		}

		Serializable res = (Serializable) this.task.execute(this.params);
		if (this.resultSendingProxy != null) {
			this.resultSendingProxy.sendResult(res);
		}
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTaskActivation#toStringContent(java.lang.StringBuffer)
	 */
	@Override
	protected void		toStringContent(StringBuffer sb)
	{
		assert	sb != null : new PreconditionException("sb != null");

		super.toStringContent(sb);
		sb.append("; ");
		sb.append(this.resultSendingProxy);
	}
}
// -----------------------------------------------------------------------------
