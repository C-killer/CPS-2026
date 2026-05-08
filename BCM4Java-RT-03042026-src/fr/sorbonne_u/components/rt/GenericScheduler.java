package fr.sorbonne_u.components.rt;

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

import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import fr.sorbonne_u.components.AbstractComponent.AbstractTask;
import fr.sorbonne_u.components.ComponentI.ComponentTask;
import fr.sorbonne_u.components.exceptions.BCMRuntimeException;
import fr.sorbonne_u.components.rt.annotations.AperiodicTaskDescription;
import fr.sorbonne_u.components.rt.annotations.PeriodicTaskDescription;
import fr.sorbonne_u.components.rt.annotations.SporadicTaskDescription;
import fr.sorbonne_u.components.rt.scheduling.AbstractTaskActivationComparator;
import fr.sorbonne_u.components.rt.scheduling.SchedulingI;
import fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTaskActivation;
import fr.sorbonne_u.components.rt.scheduling.tasks.AperiodicTask;
import fr.sorbonne_u.components.rt.scheduling.tasks.PeriodicTask;
import fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTask;
import fr.sorbonne_u.components.rt.scheduling.tasks.SporadicTask;
import fr.sorbonne_u.exceptions.PostconditionException;
import fr.sorbonne_u.exceptions.PreconditionException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.PriorityQueue;

// -----------------------------------------------------------------------------
/**
 * The class <code>GenericScheduler</code> defines the core behaviour for a
 * scheduler attached to a BCM4Java real time component.
 *
 * <p><strong>Description</strong></p>
 * 
 * <p>
 * A scheduler is associated with a real time component that owns it. It has
 * a time unit used when expressing times and delays, which must also be used
 * in all tasks descriptors. A scheduler is started at some future real time
 * and then later stopped.
 * </p>
 * 
 * <p><strong>Implementation Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * <p><strong>Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * <p>Created on : 2021-03-03</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public abstract class	GenericScheduler
implements	SchedulingI
{
	// -------------------------------------------------------------------------
	// Inner classes and types
	// -------------------------------------------------------------------------

	/**
	 * The class <code>TaskKey</code> implements the key to find tasks and
	 * information about component methods that they run.
	 *
	 * <p><strong>Description</strong></p>
	 * 
	 * <p>
	 * Tasks in real time components correspond to methods implemented by the
	 * class defining the component code. To get a constant hashing code from
	 * relatively easily accessible information, the task key first concatenates
	 * the method name, the canonical names of all formal parameter types in
	 * their order of appearance in the method signature and the canonical name
	 * of the result type of the method. It then uses the hash code of the
	 * resulting string as its own hash code.
	 * </p>
	 * 
	 * <p><strong>Implementation Invariants</strong></p>
	 * 
	 * <pre>
	 * invariant	{@code true}	// no more invariant
	 * </pre>
	 * 
	 * <p><strong>Invariants</strong></p>
	 * 
	 * <pre>
	 * invariant	{@code true}	// no more invariant
	 * </pre>
	 * 
	 * <p>Created on : 2023-03-08</p>
	 * 
	 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
	 */
	public static class		TaskKey
	implements	TaskKeyI
	{
		// ---------------------------------------------------------------------
		// Constants and variables
		// ---------------------------------------------------------------------

		/** name of the method representing the aperiodic task in the owner
		 *  real time component.											*/
		protected final String		methodName;
		/** array of parameter types of the method representing the aperiodic
		 *  task in the owner real time component.							*/
		protected final Class<?>[]	formalParametersTypes;
		/** return type of the method representing the aperiodic task in
		 *  the owner real time component.									*/
		protected final Class<?>	returnType;
		/** string identifying the key used to compute the hash code of
		 *  the key.														*/
		protected final String		hashBase;
		/** the hash code of the key instance.								*/
		protected final int			hashCode;

		// ---------------------------------------------------------------------
		// Constructors
		// ---------------------------------------------------------------------

		/**
		 * create a task key.
		 * 
		 * <p><strong>Contract</strong></p>
		 * 
		 * <pre>
		 * pre	{@code methodName != null && !methodName.isEmpty()}
		 * pre	{@code formalParametersTypes != null}
		 * pre	{@code returnType != null}
		 * post	{@code true}	// no postcondition.
		 * </pre>
		 *
		 * @param methodName			name of the method called by the task.
		 * @param formalParametersTypes	types of the formal parameters of the method called by the task.
		 * @param returnType			type of the returned value of the method called by the task.
		 */
		public				TaskKey(
			String methodName,
			Class<?>[] formalParametersTypes,
			Class<?> returnType
			)
		{
			assert	methodName != null && !methodName.isEmpty() :
					new PreconditionException(
							"methodName != null && !methodName.isEmpty()");
			assert	formalParametersTypes != null :
					new PreconditionException("formalParametersTypes != null");
			assert	returnType != null :
					new PreconditionException("returnType != null");

			this.methodName = methodName;
			this.formalParametersTypes = formalParametersTypes;
			this.returnType = returnType;

			StringBuffer sb = new StringBuffer(returnType.getCanonicalName());
			sb.append(" ");
			sb.append(methodName);
			sb.append("(");
			for (int i = 0 ; i < formalParametersTypes.length ; i++) {
				sb.append(formalParametersTypes[i].getCanonicalName());
				if (i < formalParametersTypes.length - 1) {
					sb.append(", ");
				}
			}
			sb.append(")");
			this.hashBase = sb.toString();
			this.hashCode = this.hashBase.hashCode();
		}

		// ---------------------------------------------------------------------
		// Methods
		// ---------------------------------------------------------------------

		/**
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int		hashCode()
		{
			return this.hashCode;
		}

		/**
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean	equals(Object obj)
		{
			if (!(obj instanceof TaskKey)) return false;
			TaskKey tk = (TaskKey) obj;
			return this.hashBase.equals(tk.hashBase);
		}

		/**
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String	toString()
		{
			StringBuffer sb = new StringBuffer(this.getClass().getSimpleName());
			sb.append('[');
			sb.append(this.methodName);
			sb.append(", {");
			for (int i = 0 ; i < this.formalParametersTypes.length; i++) {
				sb.append(this.formalParametersTypes[i].getCanonicalName());
				if (i < this.formalParametersTypes.length - 1) {
					sb.append(", ");
				}
			}
			sb.append("}, ");
			sb.append(this.returnType.getCanonicalName());
			sb.append(']');
			return sb.toString();
		}
	}

	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	/** when true, the scheduler makes debugging traces of its actions
	 *  through the owner component tracing facility.						*/
	public static boolean				DEBUG = false;

	/** the real time component owning this scheduler.						*/
	protected final AbstractRTComponent	owner;
	/** index of the schedulable thread pool provided by the owner
	 *  component.															*/
	protected final int					schedulingThreadPoolIndex;
	/** the time unit used to schedule the tasks.							*/
	protected final TimeUnit			schedulingTimeUnit;

	/** set of periodic tasks to be scheduled.								*/
	protected final ConcurrentHashMap<TaskKey,PeriodicTask>		periodicTasks;
	/** set of aperiodic tasks that may have to be scheduled.				*/
	protected final ConcurrentHashMap<TaskKey,AperiodicTask>	aperiodicTasks;
	/** set of sporadic tasks that may have to be scheduled.				*/
	protected final ConcurrentHashMap<TaskKey,SporadicTask>		sporadicTasks;

	/** current real time of start.											*/
	protected long						schedulerRealStartTime;
	/** true when the scheduler has been stopped.							*/
	protected final AtomicBoolean		stopped;

	/** future variable allowing to cancel the currently dispatched task
	 *  scheduled on a Java scheduled pool of threads.						*/
	protected Future<?>					currentDispatch;
	/** time at which the current dispatch has occurred or will occur.		*/
	protected long						currentDispatchTime;
	/** currently activated tasks that are eligible for selection.			*/
	protected final PriorityQueue<AbstractRealTimeTaskActivation>
															eligibleTasks;
	
	/** currently activated tasks that are not yet eligible for selection.	*/
	protected final PriorityQueue<AbstractRealTimeTaskActivation>
															waitingTasks;
	
	/** lock protecting the variables {@code currentDispatch},
	 *  {@code currentDispatchTime}, {@code currentlyRunningTaskActivation}
	 *  and {@code eligibleTasks}.											*/
	protected final ReentrantLock		currentDispatchLock;

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	/**
	 * create a scheduler for the given owner component and using the given
	 * time unit when expressing times and durations.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code owner != null}
	 * pre	{@code schedulingThreadPoolIndex >= 0}
	 * pre	{@code owner.validExecutorServiceIndex(schedulingThreadPoolIndex)}
	 * pre	{@code owner.isSchedulable(schedulingThreadPoolIndex)}
	 * pre	{@code schedulingTimeUnit != null}
	 * post	{@code !hasPlannedTask()}
	 * </pre>
	 *
	 * @param owner						owner real time component.
	 * @param schedulingThreadPoolIndex	index of the schedulable thread pool provided by the owner component.
	 * @param schedulingTimeUnit		time unit used when expressing times and durations.
	 */
	public				GenericScheduler(
		AbstractRTComponent owner,
		int schedulingThreadPoolIndex,
		TimeUnit schedulingTimeUnit
		)
	{
		assert	owner != null : new PreconditionException("owner != null");
		assert	schedulingThreadPoolIndex >= 0 :
				new PreconditionException("schedulingThreadPoolIndex >= 0");
		assert	owner.validExecutorServiceIndex(schedulingThreadPoolIndex) :
				new PreconditionException("owner.validExecutorServiceIndex("
										  + "schedulingThreadPoolIndex)");
		assert	owner.isSchedulable(schedulingThreadPoolIndex) :
				new PreconditionException(
							"owner.isSchedulable(schedulingThreadPoolIndex)");
		assert	schedulingTimeUnit != null :
				new PreconditionException("schedulingTimeUnit != null");

		this.owner = owner; 
		this.schedulingThreadPoolIndex = schedulingThreadPoolIndex;
		this.schedulingTimeUnit = schedulingTimeUnit;
		this.stopped = new AtomicBoolean(false);

		this.periodicTasks = new ConcurrentHashMap<>();
		this.aperiodicTasks = new ConcurrentHashMap<>();
		this.sporadicTasks = new ConcurrentHashMap<>();

		this.eligibleTasks = new PriorityQueue<>(this.getEligibleComparator());
		this.waitingTasks = new PriorityQueue<>(this.getWaitingComparator());

		this.currentDispatch = null;
		this.currentDispatchTime = Long.MIN_VALUE;
		this.currentDispatchLock = new ReentrantLock();

		assert	!hasPlannedTask() :
				new PostconditionException("!hasPlannedTask()");
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.SchedulingI#getOwner()
	 */
	@Override
	public AbstractRTComponent	getOwner()
	{
		return this.owner;
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.SchedulingI#logMessage(java.lang.String)
	 */
	@Override
	public void			logMessage(String message)
	{
		this.getOwner().logMessage(message);
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.SchedulingI#getSchedulingTimeUnit()
	 */
	@Override
	public TimeUnit		getSchedulingTimeUnit()
	{
		return this.schedulingTimeUnit;
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.SchedulingI#convertToSchedulerTimeUnit(long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public long			convertToSchedulerTimeUnit(
		long sourceDuration,
		TimeUnit sourceTimeUnit
		)
	{
		return this.getSchedulingTimeUnit().
								convert(sourceDuration, sourceTimeUnit);
	}


	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.SchedulingI#taskExists(java.lang.reflect.Method)
	 */
	@Override
	public boolean		taskExists(Method method)
	{
		boolean ret = true;

		ret = method != null;
		ret = ret && method.getDeclaringClass().isAssignableFrom(
													this.getOwner().getClass());
		ret = ret &&
				(method.isAnnotationPresent(PeriodicTaskDescription.class)
				 || method.isAnnotationPresent(AperiodicTaskDescription.class)
				 || method.isAnnotationPresent(SporadicTaskDescription.class));

		if (ret) {
			TaskKey tk = new TaskKey(method.getName(),
									 method.getParameterTypes(),
									 method.getReturnType());
			return this.taskExists(tk);
		} else {
			return ret;
		}
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.SchedulingI#taskExists(fr.sorbonne_u.components.rt.scheduling.SchedulingI.TaskKeyI)
	 */
	@Override
	public boolean		taskExists(TaskKeyI key)
	{
		assert	key != null : new PreconditionException("key != null");
		
		return this.periodicTasks.containsKey(key) ||
			   this.aperiodicTasks.containsKey(key) ||
			   this.sporadicTasks.containsKey(key);
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.SchedulingI#createPeriodicTask(java.lang.reflect.Method)
	 */
	@Override
	public void			createPeriodicTask(Method method) throws Exception
	{
		assert	method != null : new PreconditionException("method != null");
		assert	method.isAnnotationPresent(PeriodicTaskDescription.class) :
				new PreconditionException(
						"method.isAnnotationPresent("
						+ "PeriodicTaskDescription.class)");
		assert	method.getDeclaringClass().isAssignableFrom(
													getOwner().getClass()) :
			 	new PreconditionException(
			 			"method.getDeclaringClass().isAssignableFrom("
			 			+ "getOwner().getClass())");
		assert	!this.taskExists(method) :
				new PreconditionException("!this.taskExist(method)");

		PeriodicTask pt = new PeriodicTask(owner, this, method);
		TaskKey tk = new TaskKey(method.getName(),
								 method.getParameterTypes(),
								 method.getReturnType());

		if (DEBUG) {
			this.logMessage("creating periodic task " + tk.toString());
		}

		this.periodicTasks.put(tk, pt);
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.SchedulingI#createAperiodicTask(java.lang.reflect.Method)
	 */
	@Override
	public void			createAperiodicTask(Method method) throws Exception 

	{
		assert	method != null : new PreconditionException("method != null");
		assert	method.isAnnotationPresent(AperiodicTaskDescription.class) :
				new PreconditionException(
						"method.isAnnotationPresent("
						+ "AperiodicTaskDescription.class)");
		assert	method.getDeclaringClass().isAssignableFrom(
													getOwner().getClass()) :
			 	new PreconditionException(
			 			"method.getDeclaringClass().isAssignableFrom("
			 			+ "getOwner().getClass())");
		assert	!this.taskExists(method) :
				new PreconditionException("!this.taskExist(method)");

		AperiodicTask st = new AperiodicTask(owner, this, method);
		TaskKey tk = new TaskKey(method.getName(),
								 method.getParameterTypes(),
								 method.getReturnType());

		if (DEBUG) {
			this.logMessage("creating aperiodic task " + tk.toString());
		}

		this.aperiodicTasks.put(tk, st);
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.SchedulingI#createSporadicTask(java.lang.reflect.Method)
	 */
	@Override
	public void			createSporadicTask(Method method) throws Exception 

	{
		assert	method != null : new PreconditionException("method != null");
		assert	method.isAnnotationPresent(SporadicTaskDescription.class) :
				new PreconditionException(
						"method.isAnnotationPresent("
						+ "SporadicTaskDescription.class)");
		assert	method.getDeclaringClass().isAssignableFrom(
													getOwner().getClass()) :
			 	new PreconditionException(
			 			"method.getDeclaringClass().isAssignableFrom("
			 			+ "getOwner().getClass())");
		assert	!this.taskExists(method) :
				new PreconditionException("!this.taskExist(method)");

		SporadicTask st = new SporadicTask(owner, this, method);
		TaskKey tk = new TaskKey(method.getName(),
								 method.getParameterTypes(),
								 method.getReturnType());

		if (DEBUG) {
			this.logMessage("creating sporadic task " + tk.toString());
		}

		this.sporadicTasks.put(tk, st);
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.SchedulingI#startScheduling(long)
	 */
	@Override
	public void			startScheduling(long realTimeOfStart)
	{
		if (DEBUG) {
			this.getOwner().logMessage(
					"Scheduler starting at " + realTimeOfStart
					+ " " + this.getSchedulingTimeUnit() + ".");
		}

		this.stopped.set(false);
		this.schedulerRealStartTime = realTimeOfStart;

		if (DEBUG) {
			this.logMessage("activating periodic tasks.");
		}
		for(Entry<TaskKey, PeriodicTask> e : this.periodicTasks.entrySet()) {
			if (DEBUG) {
				this.logMessage("activating " + e.getKey().toString()
								+ " as " + e.getValue());
			}
			this.addTaskActivation(e.getValue().activateNow(new Object[]{}));
		}
		if (DEBUG) {
			this.logMessage("periodic tasks activated.");
		}

		this.dispatchNow();
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.SchedulingI#stopScheduling()
	 */
	@Override
	public void			stopScheduling()
	{
		boolean b = this.stopped.compareAndSet(false, true);
		assert	b;
		this.logMessage("Scheduler stopped.");
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.SchedulingI#stopped()
	 */
	@Override
	public boolean		stopped()
	{
		return this.stopped.get();
	}

	@Override
	public AbstractRealTimeTask	getRealTimeTask(TaskKeyI tk)
	{
		assert	tk != null : new PreconditionException("tk != null");

		AbstractRealTimeTask ret = this.periodicTasks.get(tk);
		if (ret == null) {
			ret = this.aperiodicTasks.get(tk);
		}
		if (ret == null) {
			ret = this.sporadicTasks.get(tk);
		}
		return ret;
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.SchedulingI#addTaskActivation(fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTaskActivation)
	 */
	@Override
	public void			addTaskActivation(AbstractRealTimeTaskActivation rta)
	{
		assert	rta != null && taskExists(rta.getTaskKey()) :
				new PreconditionException(
						"rta != null && taskExist(rta.getTaskKey()");

		this.currentDispatchLock.lock();
		try {
			if (this.isEligible(rta)) {
				this.eligibleTasks.add(rta);
			} else {
				this.waitingTasks.add(rta);
			}
		} finally {
			this.currentDispatchLock.unlock();
		}
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.SchedulingI#addTaskActivationAndDispatch(fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTaskActivation)
	 */
	@Override
	public void			addTaskActivationAndDispatch(
		AbstractRealTimeTaskActivation rta
		)
	{
		assert	rta != null && taskExists(rta.getTaskKey()) :
				new PreconditionException(
						"rta != null && taskExist(rta.getTaskKey()");
	
		this.addTaskActivation(rta);
		this.dispatchNow();
	}

	// -------------------------------------------------------------------------
	// Internal methods
	// -------------------------------------------------------------------------

	/**
	 * return the absolute current time in the scheduler time unit.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @return	the absolute current time in the scheduler time unit.
	 */
	protected long			getCurrentTime()
	{
		return this.convertToSchedulerTimeUnit(System.currentTimeMillis(),
											   TimeUnit.MILLISECONDS);
	}

	/**
	 * return true if {@code rta} is eligible for selection.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param rta	task activation to be tested.
	 * @return		true if {@code rta} is eligible for selection.
	 */
	protected abstract boolean	isEligible(AbstractRealTimeTaskActivation rta);

	/**
	 * return the comparator to be used to insert task activations in the
	 * eligible tasks queue.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null}
	 * </pre>
	 *
	 * @return	the comparator to be used to insert task activations in the eligible tasks queue.
	 */
	protected abstract AbstractTaskActivationComparator	getEligibleComparator();

	/**
	 * return the comparator to be used to insert task activations in the
	 * waiting tasks queue.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null}
	 * </pre>
	 *
	 * @return	the comparator to be used to insert task activations in the eligible tasks queue.
	 */
	protected abstract AbstractTaskActivationComparator	getWaitingComparator();

	/**
	 * run {@code t} on the scheduling thread pool.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code t != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param t								component task to be executed as main task.
	 * @return								a future allowing to cancel and synchronise on the task execution.
	 * @throws AssertionError				if the preconditions are not satisfied.
	 * @throws RejectedExecutionException	if the task cannot be scheduled for execution.
	 */
	protected Future<?>		runTaskOnComponent(
		ComponentTask t
		) throws AssertionError, RejectedExecutionException
	{
		return this.getOwner().runTaskOnComponent(
										this.schedulingThreadPoolIndex, t);
	}

	/**
	 * schedule {@code t} after {@code delay tu} on the scheduling thread pool.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param t								component task to be executed as main task.
	 * @param delay							delay until the task can start, expressed in the scheduling time unit.
	 * @return								a future allowing to cancel and synchronise on the task execution.
	 * @throws AssertionError				if the preconditions are not satisfied.
	 * @throws RejectedExecutionException	if the task cannot be scheduled for execution.
	 */
	protected ScheduledFuture<?>	scheduleTaskOnComponent(
		ComponentTask t,
		long delay
		) throws AssertionError, RejectedExecutionException
	{
		return this.getOwner().scheduleTaskOnComponent(
									this.schedulingThreadPoolIndex, t, delay,
									this.getSchedulingTimeUnit());
	}

	/**
	 * return true if the scheduler currently has planned a dispatch in the
	 * future.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @return	true if the scheduler currently has planned a dispatch in the future.
	 */
	protected boolean	hasPlannedTask()
	{
		boolean ret = false;
		this.currentDispatchLock.lock();
		try {
			ret = this.currentDispatch != null;
		} finally {
			this.currentDispatchLock.unlock();
		}
		return ret;
	}

	/**
	 * trigger a dispatch for the next task immediately, delegating this to
	 * the owner component for scheduling on a schedulable pool of threads.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 */
	protected void		dispatchNow()
	{
		if (!this.stopped()) {
			this.runTaskOnComponent(
					new AbstractTask() {
						@Override
						public void run() {
							dispatchNextTask();
						}
					});

			if (DEBUG) {
				this.logMessage("dispatching scheduled.");
			}
		} else {
			this.logMessage("Scheduler stopped.");
		}
	}

	/**
	 * schedule the next dispatch of a task after the given delay, delegating
	 * this to the owner component for scheduling on a schedulable pool of
	 * threads.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code delay >= 0}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param delay	delay before the next dispatch expressed in the scheduler time unit.
	 */
	protected void		dispatchAfter(long delay)
	{
		if (DEBUG) {
			this.logMessage("dispatchAfter " + delay);
		}
		assert	delay >= 0 : new PreconditionException("delay >= 0");

		if (!this.stopped()) {
			this.currentDispatchLock.lock();
			try {
				assert	this.currentDispatch == null :
						new BCMRuntimeException("this.currentDispatch == null");

				this.currentDispatchTime = this.getCurrentTime() + delay;
				this.currentDispatch =
					this.scheduleTaskOnComponent(
						new AbstractTask() {
							@Override
							public void run() {
								currentDispatchTime = Long.MIN_VALUE;
								currentDispatch = null;
								dispatchNextTask();
							}
						},
						delay);
			} finally {
				this.currentDispatchLock.unlock();
			}
		} else {
			this.getOwner().logMessage("Scheduler stopped.");
		}
	}

	/**
	 * transfer all waiting task activations that are now eligible into the
	 * eligible task activations queue.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 */
	protected void		transferEligible()
	{
		this.currentDispatchLock.lock();
		try {
			synchronized (this.waitingTasks) {
				if (!this.waitingTasks.isEmpty()) {
					long currentTime = this.getCurrentTime();
					AbstractRealTimeTaskActivation rta = this.waitingTasks.peek();
					while (rta != null &&
								rta.getAbsoluteEarliestTime() <= currentTime) {
						this.waitingTasks.remove();
						this.eligibleTasks.add(rta);
						rta = this.waitingTasks.peek();
					}
				}
			}
		} finally {
			this.currentDispatchLock.unlock();
		}
	}

	/**
	 * if not stopped, dispatch the next task to be executed by the real time
	 * component and schedule the next call to this method.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 */
	protected abstract void		dispatchNextTask();

	/**
	 * 
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 */
	public void			logRegisteredTasks()
	{
		StringBuffer sb= new StringBuffer("Scheduler: Registered tasks\n");
		sb.append("                            Periodic tasks\n");
		for (Entry<TaskKey,PeriodicTask> e : this.periodicTasks.entrySet()) {
			sb.append("                     ");
			sb.append(e.getKey());
			sb.append(" -> ");
			sb.append(e.getValue().toString());
			sb.append('\n');
		}
		sb.append("                            Aperiodic tasks\n");
		for (Entry<TaskKey,AperiodicTask> e : this.aperiodicTasks.entrySet()) {
			sb.append("                     ");
			sb.append(e.getKey());
			sb.append(" -> ");
			sb.append(e.getValue().toString());
			sb.append('\n');
		}
		sb.append("                            Sporadic tasks\n");
		for (Entry<TaskKey,SporadicTask> e : this.sporadicTasks.entrySet()) {
			sb.append("                     ");
			sb.append(e.getKey());
			sb.append(" -> ");
			sb.append(e.getValue().toString());
			sb.append('\n');
		}
		this.logMessage(sb.toString());
	}

	/**
	 * log the content of the eligible and the waiting tasks queues.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 */
	protected void		logTaskActivationsQueues()
	{
		Iterator<AbstractRealTimeTaskActivation> iter =
												this.eligibleTasks.iterator();
		int size = this.eligibleTasks.size();
		int i = 0;
		StringBuffer sb = new StringBuffer("Scheduler: Eligible task activations queue = {");
		while (iter.hasNext()) {
			AbstractRealTimeTaskActivation t = iter.next();
			sb.append("(");
			sb.append(t.getName());
			sb.append(", activation time = ");
			sb.append(t.getActivationTime());
			sb.append(", absolute deadline = ");
			sb.append(t.getAbsoluteDeadline());
			sb.append(")");
			if (i < size - 1) {
				sb.append(", ");
			}
			i++;
		}
		sb.append("}\n");
		sb.append("                         Waiting task activations queue = {");
		iter = this.waitingTasks.iterator();
		size = this.waitingTasks.size();
		i = 0;
		while (iter.hasNext()) {
			AbstractRealTimeTaskActivation t = iter.next();
			sb.append("(");
			sb.append(t.getName());
			sb.append(", activation time = ");
			sb.append(t.getActivationTime());
			sb.append(", earliest starting time = ");
			sb.append(t.getAbsoluteEarliestTime());
			sb.append(")");
			if (i < size - 1) {
				sb.append(", ");
			}
			i++;
		}
		sb.append("}\n");
		this.logMessage(sb.toString());
	}
}
// -----------------------------------------------------------------------------
