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

import java.util.concurrent.TimeUnit;
import fr.sorbonne_u.components.rt.AbstractRTComponent;
import fr.sorbonne_u.components.rt.GenericScheduler.TaskKey;
import fr.sorbonne_u.components.rt.scheduling.SchedulingI;
import fr.sorbonne_u.components.rt.scheduling.SchedulingI.TaskKeyI;
import fr.sorbonne_u.exceptions.AssertionChecking;
import fr.sorbonne_u.exceptions.ImplementationInvariantException;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PreconditionException;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;

// -----------------------------------------------------------------------------
/**
 * The abstract class <code>AbstractRealTimeTask</code> defines the core
 * common information describing a real time task for a BCM4Java real time
 * component.
 *
 * <p><strong>Description</strong></p>
 * 
 * <p><strong>Implementation Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code owner != null}
 * invariant	{@code method != null}
 * invariant	{@code method.getDeclaringClass().isAssignableFrom(owner.getClass())}
 * </pre>
 * 
 * <p><strong>Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code getRelativeDeadline() >= getWcet()}
 * invariant	{@code getMutexTasks() == null || getNumber() > 0 || }
 * invariant	{@code getMutexTasks() == null || Arrays.stream(getMutexTasks()).allMatch(n -> n > 0 && n != getNumber())}
 * </pre>
 * 
 * <p>Created on : 2021-03-03</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public abstract class	AbstractRealTimeTask
{
	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	/** real time component holding the code this task require to execute.	*/
	protected final AbstractRTComponent	owner;
	/** the method in the owner component that this task will execute.		*/
	protected final Method				method;
	/** scheduler responsible to trigger the execution of the task
	 *  activations; associated with the owner component.					*/
	protected final SchedulingI			scheduler;
	/** the locally (within a component) unique number identifying the task;
	 *  a 0 number is considered as non numbered task.						*/
	protected final int					no;
	/** time unit in which the times are expressed in this descriptor.		*/
	protected final TimeUnit			timeUnit;
	/** worst-case execution time of the task.								*/
	protected final long				wcet;
	/** deadline for the task relative to the activation time, in
	 *  {@code timeUnit}.													*/
	protected final long				relativeDeadline;
	/** array of task numbers for tasks which must execute in mutual
	 *  exclusion with this one.											*/
	protected final int[]				mutexTasks;

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
		AbstractRealTimeTask instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		ret &= AssertionChecking.checkImplementationInvariant(
				instance.owner != null,
				AbstractRealTimeTask.class,
				instance,
				"owner != null");
		ret &= AssertionChecking.checkImplementationInvariant(
				instance.method != null,
				AbstractRealTimeTask.class,
				instance,
				"method != null");
		ret &= AssertionChecking.checkImplementationInvariant(
				instance.method.getDeclaringClass().
								isAssignableFrom(instance.owner.getClass()),
				AbstractRealTimeTask.class,
				instance,
				"method.getDeclaringClass().isAssignableFrom(owner.getClass())");
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
		AbstractRealTimeTask instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		ret &= AssertionChecking.checkInvariant(
				instance.getRelativeDeadline() >= instance.getWcet(),
				AbstractRealTimeTask.class,
				instance,
				"getRelativeDeadline() >= getWcet()");
		ret &= AssertionChecking.checkInvariant(
				instance.getMutexTasks() == null ||
						Arrays.stream(instance.getMutexTasks()).allMatch(
											n -> n >= 0 && n != instance.no),
				AbstractRealTimeTask.class,
				instance,
				"getMutexTasks() == null || Arrays.stream(getMutexTasks())."
				+ "allMatch(n -> n >= 0 && n != no)");
		return ret;
	}

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	/**
	 * create a new real time task.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code owner != null}
	 * pre	{@code scheduler != null}
	 * pre	{@code no >= 0}
	 * pre	{@code timeUnit != null}
	 * pre	{@code wcet > 0}
	 * pre	{@code relativeDeadline >= wcet}
	 * pre	{@code mutexTasks == null || no > 0}
	 * pre	{@code mutexTasks == null || Arrays.stream(mutexTasks).allMatch(n -> n > 0 && n != no)}
	 * pre	{@code method != null}
	 * pre	{@code method.getDeclaringClass().isAssignableFrom(owner.getClass())}
	 * post	{@code getNumber() == no}
	 * post	{@code getTimeUnit() == timeUnit}
	 * post	{@code getWcet() == wcet}
	 * post	{@code getRelativeDeadline() == relativeDeadline}
	 * post	{@code getMutexTasks() == null || getMutexTasks().equals(mutexTasks)}
	 * post	{@code getName().equals(method.getName())}
	 * post	{@code isVarArgs() == method.isVarArgs()}
	 * post	{@code getNumberOfParameters() == method.getParameterCount()}
	 * </pre>
	 *
	 * @param owner				real time component holding the code this task require to execute.
	 * @param scheduler			scheduler responsible to trigger the execution of the task activations; associated with the owner component.
	 * @param no				unique task number given by the user to this task.
	 * @param timeUnit			time unit in which the times are expressed in this descriptor.
	 * @param wcet				worst-case execution time of the task.
	 * @param relativeDeadline	deadline for the task; for periodic tasks, this is relative to the scheduling cycle start time.
	 * @param mutexTasks		numbers of the tasks that must execute in mutual exclusion with this one.
	 * @param method			the method in the owner component that this task will execute.
	 */
	public				AbstractRealTimeTask(
		AbstractRTComponent owner,
		SchedulingI scheduler,
		int no,
		TimeUnit timeUnit,
		long wcet,
		long relativeDeadline,
		int[] mutexTasks,
		Method method
		)
	{
		super();

		assert	owner != null : new PreconditionException("owner != null");
		assert	scheduler != null :
				new PreconditionException("scheduler != null");
		assert	no >= 0 : new PreconditionException("no >= 0");
		assert	timeUnit != null : new PreconditionException("timeUnit != null");
		assert	wcet > 0 : new PreconditionException("wcet > 0");
		assert	relativeDeadline >= wcet :
				new PreconditionException("relativeDeadline >= wcet");
		assert	mutexTasks == null ||
							Arrays.stream(mutexTasks).allMatch(n -> n != no) :
				new PreconditionException(
						"mutexTasks == null ||"
						+ " Arrays.stream(mutexTasks).allMatch(n -> n != no)");
		assert	method.getDeclaringClass().isAssignableFrom(owner.getClass()) :
		 		new PreconditionException(
		 				"method.getDeclaringClass().isAssignableFrom("
		 				+ "owner.getClass())");

		this.owner = owner;
		this.scheduler = scheduler;
		this.no = no;
		this.timeUnit = timeUnit;
		this.wcet = wcet;
		this.relativeDeadline = relativeDeadline;
		this.mutexTasks = mutexTasks == null ? new int[]{} : mutexTasks;
		this.method = method;
		this.method.setAccessible(true);

		assert 	AbstractRealTimeTask.implementationInvariants(this) :
				new ImplementationInvariantException(
						"AbstractRealTimeTask.implementationInvariants(this)");
		assert	AbstractRealTimeTask.invariants(this) :
				new InvariantException(
						"AbstractRealTimeTask.invariants(this)");
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	/**
	 * return the number associated with this task; a zero must be interpreted
	 * as no attributed number.
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
		return this.no;
	}

	/**
	 * return the name of the task, here the name of the method to be called
	 * on the owner component.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null && !return.isEmpty()}
	 * </pre>
	 *
	 * @return	the name of the task.
	 */
	public String		getName()
	{
		return this.method.getName();
	}

	/**
	 * return the task key associated with this task.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null}
	 * </pre>
	 *
	 * @return	the task key associated with this task.
	 */
	public TaskKeyI		getTaskKey()
	{
		return new TaskKey(this.method.getName(),
						   this.method.getParameterTypes(),
						   this.method.getReturnType());
	}

	/**
	 * return true if this task calls a method that has a variable number of
	 * parameters.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @return	true if this task calls a method that has a variable number of parameters.
	 */
	public boolean		isVarArgs()
	{
		return this.method.isVarArgs();
	}

	/**
	 * return the number of formal parameters of the method called by this task.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @return	the number of formal parameters of the method called by this task.
	 */
	public int			getNumberOfParameters()
	{
		return this.method.getParameterCount();
	}
 
	/**
	 * return time unit in which the times are expressed in this descriptor.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null}
	 * </pre>
	 *
	 * @return	time unit in which the times are expressed in this descriptor.
	 */
	public TimeUnit		getTimeUnit()
	{
		return this.timeUnit;
	}

	/**
	 * return the worst-case execution time of the task.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return > 0}
	 * </pre>
	 *
	 * @return	the worst-case execution time of the task.
	 */
	public long			getWcet()
	{
		return this.wcet;
	}

	/**
	 * return deadline for the task; for periodic tasks, this is relative to
	 * the scheduling cycle start time.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return > 0}
	 * </pre>
	 *
	 * @return	deadline for the task; for periodic tasks, this is relative to the scheduling cycle start time.
	 */
	public long			getRelativeDeadline()
	{
		return this.relativeDeadline;
	}

	/**
	 * return array of task numbers for tasks which must execute in mutual
	 * exclusion with this one.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null && Arrays.stream(return).allMatch(n -> n > 0 && n != getNumber())}
	 * </pre>
	 *
	 * @return	array of task numbers for tasks which must execute in mutual exclusion with this one.
	 */
	public int[]		getMutexTasks()
	{
		return this.mutexTasks;
	}

	/**
	 * activate the real time task with activation time now.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null}
	 * </pre>
	 *
	 * @param params	parameters of the task.
	 * @return			a new periodic task activation.
	 */
	public AbstractRealTimeTaskActivation	activateNow(Object[] params)
	{
		long activationTime =
				this.scheduler.convertToSchedulerTimeUnit(
											System.currentTimeMillis(),
											TimeUnit.MILLISECONDS);
		return this.activate(activationTime, params);
	}


	/**
	 * activate the periodic task at the given real time.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code activationTime >= 0}
	 * post	{@code ret != null}
	 * </pre>
	 *
	 * @param activationTime	the real time of activation of the task in the scheduler time unit.
	 * @param params			parameters of the task.
	 * @return					a new periodic task activation.
	 */
	public abstract AbstractRealTimeTaskActivation	activate(
			long activationTime,
			Object[] params
			);

	/**
	 * execute the real-time component service associated with this task.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code params != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param params		actual parameters for the method to be executed.
	 * @return				the result of the service method execution or null if void.
	 * @throws Exception	exceptions thrown by {@code Method::invoke}.
	 */
	public abstract Serializable	execute(Object[] params) throws Exception;

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

		sb.append("owner = ");
		sb.append(this.owner.getClass().getSimpleName());
		sb.append("; method = ");
		sb.append(this.method.toGenericString());
		sb.append("; no = ");
		sb.append(this.getNumber());
		sb.append("; time unit = ");
		sb.append(this.getTimeUnit());
		sb.append("; wcet = ");
		sb.append(this.getWcet());
		sb.append("; relativeDeadline = ");
		sb.append(this.getRelativeDeadline());
		if (mutexTasks != null && mutexTasks.length > 0) {
			sb.append("; mutexTasks = [");
			for (int i = 0 ; i < mutexTasks.length ; i++) {
				sb.append(mutexTasks[i]);
				if (i < mutexTasks.length - 1) {
					sb.append(", ");
				}
			}
			sb.append(']');
		}
	}
}
// -----------------------------------------------------------------------------
