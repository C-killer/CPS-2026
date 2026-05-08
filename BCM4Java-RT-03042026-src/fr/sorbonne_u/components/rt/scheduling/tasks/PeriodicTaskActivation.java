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

import fr.sorbonne_u.components.rt.scheduling.SchedulingI;
import fr.sorbonne_u.exceptions.AssertionChecking;
import fr.sorbonne_u.exceptions.ImplementationInvariantException;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PreconditionException;
import java.util.concurrent.TimeUnit;
import fr.sorbonne_u.components.exceptions.BCMRuntimeException;

// -----------------------------------------------------------------------------
/**
 * The class <code>PeriodicTaskActivation</code> implements a periodic task
 * activation for BCM4Java real time schedulers that is executed by calling a
 * method (properly annotated with {@code PeriodicTaskDescription}).
 *
 * <p><strong>Description</strong></p>
 * 
 * <p><strong>White-box Invariant</strong></p>
 * 
 * <pre>
 * invariant	{@code task instanceof PeriodicTask}
 * invariant	{@code task.isVarArgs() || task.getNumberOfParameters() == params.length}
 * </pre>
 * 
 * <p><strong>Black-box Invariant</strong></p>
 * 
 * <pre>
 * invariant	{@code getAbsoluteEarliestTime() >= getActivationTime()}
 * invariant	{@code getAbsoluteEarliestTime() <= getAbsoluteDeadline() - getWcetInSchedulerTimeUnit()}
 * </pre>
 * 
 * <p>Created on : 2023-03-03</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public class			PeriodicTaskActivation
extends		AbstractRealTimeTaskActivation
{
	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	/** absolute earliest time at which the task can be executed in the
	 *  scheduler time unit.												*/
	protected final long			absoluteEarliestTime;

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
		PeriodicTaskActivation instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		ret &= AssertionChecking.checkImplementationInvariant(
				instance.task instanceof PeriodicTask,
				PeriodicTaskActivation.class,
				instance,
				"task instanceof PeriodicTask");
		;
		ret &= AssertionChecking.checkImplementationInvariant(
				instance.params != null,
				PeriodicTaskActivation.class,
				instance,
				"params != null");
		ret &= AssertionChecking.checkImplementationInvariant(
				instance.task.isVarArgs() ||
						instance.task.getNumberOfParameters() ==
														instance.params.length,
				PeriodicTaskActivation.class,
				instance,
				"task.isVarArgs() || task.getNumberOfParameters() == "
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
		PeriodicTaskActivation instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		ret &= AssertionChecking.checkInvariant(
				instance.getAbsoluteEarliestTime() >= instance.getActivationTime(),
				PeriodicTaskActivation.class,
				instance,
				"getAbsoluteEarliestTime() >= getActivationTime()");
		ret &= AssertionChecking.checkInvariant(
				instance.getAbsoluteEarliestTime() <=
						instance.getAbsoluteDeadline() -
										instance.getWcetInSchedulerTimeUnit(),
				PeriodicTaskActivation.class,
				instance,
				"getAbsoluteEarliestTime() <= getAbsoluteDeadline() - "
				+ "getWcetInSchedulerTimeUnit()");
		return ret;
	}

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	/**
	 * create a new periodic task activation.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code task != null}
	 * pre	{@code scheduler != null}
	 * pre	{@code activationTime > 0}
	 * post	{@code getAbsoluteDeadline() > getActivationTime() + getWcet()}
	 * post	{@code getAbsoluteEarliestTime() >= getActivationTime()}
	 * post	{@code getAbsoluteEarliestTime() =< getAbsoluteDeadline() - getWcet()}
	 * </pre>
	 *
	 * @param task				task from which this activation is an executable instance.
	 * @param scheduler			scheduler responsible to trigger the execution of the task activations; associated with the owner component.
	 * @param activationTime	real time at which the task is activated in Unix epoch time and in nanoseconds.
	 */
	public				PeriodicTaskActivation(
		PeriodicTask task,
		SchedulingI scheduler,
		long activationTime
		)
	{
		this(task, scheduler, activationTime, new Object[]{});
	}

	/**
	 * create a new periodic task activation.
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
	 * post	{@code getAbsoluteEarliestTime() =< getAbsoluteDeadline() - getWcet()}
	 * </pre>
	 *
	 * @param task				task from which this activation is an executable instance.
	 * @param scheduler			scheduler responsible to trigger the execution of the task activations; associated with the owner component.
	 * @param activationTime	real time at which the task is activated in Unix epoch time and in nanoseconds.
	 * @param params			parameters to be passed to the service when calling it.
	 */
	public				PeriodicTaskActivation(
		PeriodicTask task,
		SchedulingI scheduler,
		long activationTime,
		Object[] params
		)
	{
		super(task, scheduler, activationTime, params);

		this.absoluteEarliestTime =
				activationTime +
					scheduler.convertToSchedulerTimeUnit(
							task.getRelativeEarliestStart(), task.getTimeUnit());

		assert 	PeriodicTaskActivation.implementationInvariants(this) :
				new ImplementationInvariantException(
						"PeriodicTaskActivation."
						+ "implementationInvariants(this)");
		assert	PeriodicTaskActivation.invariants(this) :
				new InvariantException("PeriodicTaskActivation.invariants(this)");
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTaskActivation#getAbsoluteEarliestTime()
	 */
	@Override
	public long			getAbsoluteEarliestTime()
	{
		return this.absoluteEarliestTime;
	}

	/**
	 * return the next activation of this task <i>i.e.</i>, at the current
	 * activation time plus the period.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @return	the next activation of this task <i>i.e.</i>, the current activation time plus the period.
	 */
	public PeriodicTaskActivation	nextActivation()
	{
		long nextActivationTime =
				this.activationTime +
					this.scheduler.convertToSchedulerTimeUnit(
									((PeriodicTask)this.task).getPeriod(),
									((PeriodicTask)this.task).getTimeUnit());

		long currentTime =
				this.scheduler.convertToSchedulerTimeUnit(
									System.currentTimeMillis(),
									TimeUnit.MILLISECONDS);

		assert	nextActivationTime > currentTime :
				new BCMRuntimeException("nextActivationTime > currentTime");
		assert 	PeriodicTaskActivation.implementationInvariants(this) :
				new ImplementationInvariantException(
						"PeriodicTaskActivation."
						+ "implementationInvariants(this)");
		assert	PeriodicTaskActivation.invariants(this) :
				new InvariantException("PeriodicTaskActivation.invariants(this)");

		return new PeriodicTaskActivation((PeriodicTask)this.task,
										  this.scheduler,
										  nextActivationTime,
										  params);
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTaskActivation#execute()
	 */
	@Override
	public void			execute() throws Exception
	{
		// schedule the next activation of the periodic task
		this.scheduler.addTaskActivation(this.nextActivation());
		// execute the current activation
		this.task.execute(this.params);
	}
}
// -----------------------------------------------------------------------------
