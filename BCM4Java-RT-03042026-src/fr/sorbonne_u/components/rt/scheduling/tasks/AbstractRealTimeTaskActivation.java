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

import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.components.rt.scheduling.SchedulingI;
import fr.sorbonne_u.components.rt.scheduling.SchedulingI.TaskKeyI;
import fr.sorbonne_u.exceptions.AssertionChecking;
import fr.sorbonne_u.exceptions.ImplementationInvariantException;
import fr.sorbonne_u.exceptions.InvariantException;

// -----------------------------------------------------------------------------
/**
 * The class <code>AbstractRealTimeTaskActivation</code> factors the common
 * content among real time task activations.
 *
 * <p><strong>Description</strong></p>
 * 
 * <p>
 * In BCM4Java real time schedulers, tasks are static definitions of code
 * elements that allow to create activations <i>i.e.</i>, instances of tasks
 * to be executed by the scheduler.
 * </p>
 * 
 * <p><strong>Implementation Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code scheduler != null}
 * invariant	{@code task != null}
 * invariant	{@code params != null}
 * </pre>
 * 
 * <p><strong>Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code getAbsoluteEarliestTime() >= getActivationTime()}
 * invariant	{@code getAbsoluteDeadline() >= getActivationTime() + getWcetInSchedulerTimeUnit()}
 * </pre>
 * 
 * <p>Created on : 2023-03-03</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public abstract class	AbstractRealTimeTaskActivation
{
	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	/** task to which refers this activation.								*/
	protected final AbstractRealTimeTask	task;
	/** scheduler responsible to trigger the execution of the task
	 *  activations; associated with the owner component.					*/
	protected final SchedulingI				scheduler;
	/** real time at which the task is activated in Unix epoch time and in
	 *  the scheduler time unit.										 	*/
	protected final long					activationTime;
	/** worst-case execution time of the task in the scheduler time unit.	*/
	protected final long					wcetInSchedulerTimeUnit;
	/** absolute deadline for the task in Unix epoch time and in
	 *  nanoseconds.														*/
	protected final long					absoluteDeadline;
	/** parameters of the task, when needed.								*/
	protected final Object[]				params;

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
		AbstractRealTimeTaskActivation instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		ret &= AssertionChecking.checkImplementationInvariant(
				instance.scheduler != null,
				AbstractRealTimeTaskActivation.class,
				instance,
				"scheduler != null");
		ret &= AssertionChecking.checkImplementationInvariant(
				instance.task != null,
				AbstractRealTimeTaskActivation.class,
				instance,
				"task != null");
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
		AbstractRealTimeTaskActivation instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		ret &= AssertionChecking.checkInvariant(
				instance.getAbsoluteDeadline() >=
					instance.getActivationTime() + instance.getWcetInSchedulerTimeUnit(),
				AbstractRealTimeTaskActivation.class,
				instance,
				"getAbsoluteDeadline() >= getActivationTime() + "
				+ "getWcetInSchedulerTimeUnit()");
		return ret;
	}

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	/**
	 * create a real time task activation.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code task != null}
	 * pre	{@code scheduler != null}
	 * pre	{@code activationTime > 0}
	 * pre	{@code params != null}
	 * pre	{@code task.isVarArgs() || task.getNumberOfParameters() == params.length}
	 * post	{@code getNumber() == task.getNumber()}
	 * post	{@code getName().equals(task.getName())}
	 * post	{@code getTaskKey().equals(task.getTaskKey())}
	 * post	{@code isScheduler(scheduler)}
	 * post	{@code getActivationTime() == activationTime}
	 * post	{@code getWcetInSchedulerTimeUnit() == scheduler.convertToSchedulerTimeUnit(task.getWcet(), task.getTimeUnit())}
	 * post	{@code getAbsoluteDeadline() >= getActivationTime() + getWcetInSchedulerTimeUnit()}
	 * </pre>
	 *
	 * @param task				task to which refers this activation.
	 * @param scheduler			scheduler responsible to trigger the execution of the task activations; associated with the owner component.
	 * @param activationTime	real time at which the task is activated in Unix epoch time and the scheduler time unit.
	 * @param params			parameters to be passed to the service when calling it.
	 */
	public				AbstractRealTimeTaskActivation(
		AbstractRealTimeTask task,
		SchedulingI scheduler,
		long activationTime,
		Object[] params
		)
	{
		super();

		assert	task != null : new PreconditionException("task != null");
		assert	scheduler != null :
				new PreconditionException("scheduler != null");
		assert	activationTime > 0 :
				new PreconditionException("activationTime > 0");
		assert	params != null :
				new PreconditionException("params != null");
		assert	task.isVarArgs() ||
							task.getNumberOfParameters() == params.length :
				new PreconditionException(
						"task.isVarArgs() || task.getNumberOfParameters() == "
						+ "params.length");

		this.task = task;
		this.scheduler = scheduler;
		this.activationTime = activationTime;
		this.params = params;
		this.wcetInSchedulerTimeUnit =
				scheduler.convertToSchedulerTimeUnit(task.getWcet(),
													 task.getTimeUnit());
		this.absoluteDeadline = 
				activationTime +
					scheduler.convertToSchedulerTimeUnit(task.getRelativeDeadline(),
														 task.getTimeUnit());

		assert 	AbstractRealTimeTaskActivation.implementationInvariants(this) :
				new ImplementationInvariantException(
						"AbstractRealTimeTaskActivation."
						+ "implementationInvariants(this)");
		assert	AbstractRealTimeTaskActivation.invariants(this) :
				new InvariantException(
						"AbstractRealTimeTaskActivation.invariants(this)");
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	/**
	 * return the number associated with the activated task; a zero must be
	 * interpreted as no attributed number.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return >= 0}
	 * </pre>
	 *
	 * @return	the number associated with this task.
	 */
	public int			getNumber()
	{
		return this.task.no;
	}

	/**
	 * return the name of the activated task.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null && !return.isEmpty()}
	 * </pre>
	 *
	 * @return	the name of the activated task.
	 */
	public String		getName()
	{
		return this.task.getName();
	}

	/**
	 * return the task key of the task that produced this task activation.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null}
	 * </pre>
	 *
	 * @return	the task key of the task that produced this task activation.
	 */
	public TaskKeyI		getTaskKey()
	{
		return this.task.getTaskKey();
	}

	/**
	 * return true if {@code scheduler} is the scheduler of this task activation.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param scheduler	a scheduler to be tested.
	 * @return			true if {@code scheduler} is the scheduler of this task activation.
	 */
	public boolean		isScheduler(SchedulingI scheduler)
	{
		return this.scheduler == scheduler;
	}

	/**
	 * return the activation time of the task in Unix epoch time, in the
	 * scheduler time unit.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return > 0}
	 * </pre>
	 *
	 * @return	the activation time of the task in Unix epoch time, in the scheduler time unit.
	 */
	public long			getActivationTime()
	{
		return this.activationTime;
	}

	/**
	 * return the worst-case execution time of the task in the scheduler time
	 * unit.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return > 0}
	 * </pre>
	 *
	 * @return	the worst-case execution time of the task in the scheduler time unit.
	 */
	public long			getWcetInSchedulerTimeUnit()
	{
		return this.wcetInSchedulerTimeUnit;
	}

	/**
	 * return absolute earliest time at which the task can be executed, in the
	 * scheduler time unit; by default, it is the activation time of the task
	 * but periodic tasks may have a later earliest time in their execution
	 * cycle.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code ret > 0}
	 * </pre>
	 *
	 * @return	absolute earliest time at which the task can be executed, in in the scheduler time unit.
	 */
	public long			getAbsoluteEarliestTime()
	{
		return this.getActivationTime();
	}

	/**
	 * return the absolute deadline for the task in Unix epoch time and in the
	 * scheduler time unit.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return > 0}
	 * </pre>
	 *
	 * @return	absolute deadline for the task in Unix epoch time and in in the scheduler time unit.
	 */
	public long			getAbsoluteDeadline()
	{
		return this.absoluteDeadline;
	}

	/**
	 * return the array of task numbers for tasks which must execute in mutual
	 * exclusion with this one.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null && Arrays.stream(mutex).allMatch(n -> n > 0 && n != getNumber())}
	 * </pre>
	 *
	 * @return	the array of task numbers for tasks which must execute in mutual exclusion with this one.
	 */
	public int[]		getMutexTasks()
	{
		return this.task.getMutexTasks();
	}

	/**
	 * execute the task on the owner component.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @throws Exception	exceptions thrown by {@code Method::invoke}.
	 */
	public abstract void	execute() throws Exception;

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String		toString()
	{
		StringBuffer sb = new StringBuffer(this.getClass().getSimpleName());
		sb.append('[');
		this.toStringContent(sb);
		sb.append(']');
		return sb.toString();
	}

	/**
	 * accumulate a description of this task in the given string buffer.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code sb != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param sb	a string buffer in which the content of this task is described.
	 */
	protected void		toStringContent(StringBuffer sb)
	{
		assert	sb != null :
				new PreconditionException("sb != null");

		sb.append("task = ");
		sb.append(this.task);
		sb.append("; scheduler = ");
		sb.append(this.scheduler);
		sb.append("; activationTime = ");
		sb.append(this.activationTime);
		sb.append("; wcetInSchedulerTimeUnit = ");
		sb.append(this.wcetInSchedulerTimeUnit);
		sb.append("; absoluteDeadline = ");
		sb.append(this.absoluteDeadline);
		if (params != null && params.length > 0) {
			sb.append("; params = [");
			for (int i = 0 ; i < params.length ; i++) {
				sb.append(params[i]);
				if (i < params.length - 1) {
					sb.append(", ");
				}
			}
			sb.append(']');
		}
	}
}
// -----------------------------------------------------------------------------
