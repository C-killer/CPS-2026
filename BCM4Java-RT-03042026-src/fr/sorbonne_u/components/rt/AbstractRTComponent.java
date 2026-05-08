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

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.exceptions.BCMException;
import fr.sorbonne_u.components.exceptions.ComponentShutdownException;
import fr.sorbonne_u.components.exceptions.ComponentStartException;
import fr.sorbonne_u.components.rt.annotations.SporadicTaskDescription;
import fr.sorbonne_u.components.rt.GenericScheduler.TaskKey;
import fr.sorbonne_u.components.rt.annotations.AperiodicTaskDescription;
import fr.sorbonne_u.components.rt.annotations.PeriodicTaskDescription;
import fr.sorbonne_u.components.rt.scheduling.SchedulingI;
import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.utils.URIGenerator;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

// -----------------------------------------------------------------------------
/**
 * The class <code>AbstractRTComponent</code> implements the core
 * functionalities to program real-time components in BCM4Java.
 *
 * <p><strong>Description</strong></p>
 * 
 * <p>
 * A real time component executes its real time services as tasks that are
 * scheduled using a real time scheduler associated with the component. This is
 * both an exploratory and educational implementation, not a realistic,
 * readily usable one for actual real time systems.
 * </p>
 * <p>
 * The tasks are represented by methods in the BCM4Java component definition
 * class that are annotated as periodic, aperiodic or sporadic tasks. Periodic
 * tasks, as their name indicates, will be repeatedly executed with some period.
 * Aperiodic tasks are represented by methods called from another component,
 * standard or real time and executed as real time tasks. Sporadic tasks
 * are represented by methods called internally from the real component
 * itself, to be executed as real time tasks. The real time scheduler is an
 * intermediate between the tasks to be executed and a scheduled executor
 * service from the standard Java concurrent package. The scheduler hence only
 * decide which task to be executed next using a selection criterion. Many
 * different schedulers can be defined and used, and some are immlemented in
 * the subpackage <code>fr.sorbonne_u.components.rt.scheduling</code>.
 * </p>
 * <p>
 * The main issue in the implementation of real time components is how the
 * different types of tasks are represented and managed. In this implementation,
 * all tasks and their code are represented by methods define in the real time
 * component definition class and tagged as such by annotations define in the
 * subpackage <code>fr.sorbonne_u.components.rt.annotations</code>. These
 * annotations allows the programmer to set constraints put on the tasks.
 * At initialisation time, the tasks are collected by processing the methods
 * in the component and their annotations. Instances of tasks are then created
 * and stored in the scheduler; they will be activated when their execution is
 * required. Task activations are kept in a priority queue, which comparator
 * implements the scheduling criterion, such as earliest deadline first.
 * </p>
 * <p>
 * Periodic tasks are automatically activated at the start of the scheduling and
 * automatically reactivated for their next period when the execution for the
 * previous period terminates. Sporadic tasks are activated manually. Hence,
 * when in the code of the real time component one needs to call another method
 * in he same component as a real time task to be activated, it first gets the
 * corresponding task from the scheduler and activate it immediately. The
 * scheduler will store it in its priority queue and execute it when its
 * selection criterion is validated.
 * </p>
 * <p>
 * Catering for calls to and from other components is a central issue in this
 * implementation. For calls made to other components, a real time component
 * cannot afford to be blocked by the call. For this reason, exchanges among
 * components are essentially asynchronous and the base tool used to implement
 * them is the <code>asynccall</code> plug-ins. A real time version of these
 * plug-ins are provided by the BCM4Java real time component implementation.
 * The client-side plug-in <code>RTAsyncCallClientPlugin</code> redefines the
 * methods <code>asyncCallWithResult</code>, <code>asyncCall</code> and
 * <code>receive</code> from the inherited <code>AsyncCallClientPlugin</code>
 * so that they use the preferred executor service set by its owner real time
 * component to execute their code. Hence, if the server-side does not implement
 * these methods as asynchronous method calls, the caller will not be too
 * severely impacted. For a real time component that offers services to other
 * components, it must create and install:
 * </p>
 * <ul>
 * <li>a <code>RTAsyncCallServerPlugin</code> if the caller uses the
 *   <code>RTAsyncCallClientPlugin</code> to call it;</li>
 * <li>a <code>RTCallServerPlugin</code> if the caller uses standard component
 *   interfaces, outbound ports and connectors, and in this case the caller
 *   thread is used to execute the plug-ins methods and will block to
 *   wait for the result in the case of synchronous calls.</li>
 * </ul>
 * <p>
 * The execution of the command that will call the server-side code is performed
 * by an aperiodic tasks which activations upon reception of the call will call
 * <code>executeAsAperiodicTaskWithResult</code> or
 * <code>executeAsAperiodicTask</code> respectively when a result is awaited
 * or not. These two methods are hence considered as aperiodic tasks which
 * constraints cannot be set until the while set of real time tasks for a
 * component is know and their own constraints determined. Hence, the two
 * methods are defined in <code>AbstractRTComponent</code> but the class
 * force its concrete subclasses to redefine and call them as super them to
 * add the appropriate aperiodic task annotation with the constraints
 * (wcet, deadline, etc.).
 * 
 * </p>
 * 
 * <p><strong>Implementation Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code numberOfSchedulingThreads > 0}
 * invariant	{@code realTimeOfStart > 0}
 * invariant	{@code executionDuration > 0}
 * invariant	{@code timeUnit != null}
 * </pre>
 * 
 * <p><strong>Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * <p>Created on : 2021-02-26</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public abstract class	AbstractRTComponent
extends		AbstractComponent
{
	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	/** when true, the component adds debugging traces to its outputs.		*/
	public static boolean			DEBUG = true;

	/** URI of the thread pool used to schedule the real time tasks.		*/
	protected static final String	SCHEDULING_THREAD_POOL_URI =
													URIGenerator.generateURI();

	/** name of the method used to execute calls from other components
	 *  as aperiodic tasks.													*/
	protected static final String	RESULT_RECEPTION_METHOD_NAME =
													"receiveResult";
	/** array of parameters types of the methods used to execute calls
	 *  to other components as aperiodic tasks.								*/
	protected static final Class<?>[]
									RESULT_RECEPTION_METHOD_PARAM_TYPES =
												new Class[]{String.class,
															Serializable.class};
	/** return type of the methods used to execute calls to other
	 *  components as aperiodic tasks.										*/
	protected static final Class<?>	RESULT_RECEPTION_METHOD_RETURN_TYPE =
																	void.class;
	protected static final TaskKey	RESULT_RECEPTION_TASK_KEY =
							new TaskKey(RESULT_RECEPTION_METHOD_NAME,
										RESULT_RECEPTION_METHOD_PARAM_TYPES,
										RESULT_RECEPTION_METHOD_RETURN_TYPE);

	/** the scheduler associated with this real time component.				*/
	protected GenericScheduler				scheduler;
	/** number of threads in the scheduling thread pool.					*/
	protected final int						numberOfSchedulingThreads;

	/** start time for the real time activities.							*/
	protected final long					realTimeOfStart;
	/** duration of the real time activities.								*/
	protected final long					executionDuration;
	/** time unit in which {@code executionDuration} is expressed.	*/
	protected final TimeUnit				timeUnit;

	protected AsyncCallRTClientSidePlugin	rtClientSidePlugin;

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	/**
	 * create a new real time component with the given number of scheduled
	 * threads and which provides the services identified as tasks in the
	 * given aperiodic task interface.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code nbSchedulingThreads > 0}
	 * pre	{@code realTimeOfStart >= System.currentTimeMillis() + SchedulingI.START_DELAY_TOLERANCE}
	 * pre	{@code executionDuration}
	 * pre	{@code timeUnit != null}
	 * post	{@code true}	// no more postcondition.
	 * </pre>
	 *
	 * @param nbSchedulingThreads	number of threads in the scheduling pool of threads.
	 * @param realTimeOfStart		real time at which the scheduling must be plan to start.
	 * @param executionDuration		duration of the real time activities.
	 * @param timeUnit				time unit in which {@code executionDuration} is expressed.
	 * @throws Exception			<i>to do</i>.
	 */
	protected			AbstractRTComponent(
		int nbSchedulingThreads,
		long realTimeOfStart,
		long executionDuration,
		TimeUnit timeUnit
		) throws Exception
	{
		// FIXME: given that the component has its own threads, it needs one
		// standard thread pool for AbstractCVM submitting the method execute...
		super(1, 0);

		assert	nbSchedulingThreads > 0 :
				new PreconditionException("nbSchedulableThreads > 0");
		assert	realTimeOfStart >= System.currentTimeMillis() +
											SchedulingI.START_DELAY_TOLERANCE :
				new PreconditionException(
						"realTimeOfStart >= "
						+ "System.currentTimeMillis() + "
						+ "SchedulingI.START_DELAY_TOLERANCE");
		assert	executionDuration > 0 :
				new PreconditionException("executionDuration");
		assert	timeUnit != null : new PreconditionException("tu != null");

		this.numberOfSchedulingThreads = nbSchedulingThreads;
		this.realTimeOfStart = realTimeOfStart;
		this.executionDuration = executionDuration;
		this.timeUnit = timeUnit;

		this.initialise();
	}

	/**
	 * create a new real time component with then given reflection inbound
	 * port URI, the given number of scheduled threads and which provides the
	 * services identified as tasks in the given aperiodic task interface.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code reflectionInboundPortURI != null && !reflectionInboundPortURI.isEmpty()}
	 * pre	{@code nbSchedulableThreads > 0}
	 * pre	{@code aperiodicTasksInterface != null}
	 * pre	{@code realTimeOfStart >= System.currentTimeMillis() + SchedulingI.START_DELAY_TOLERANCE}
	 * pre	{@code executionDuration > 0}
	 * pre	{@code timeUnit != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param reflectionInboundPortURI	the reflection inbound port URI to be used by this component.
	 * @param nbSchedulingThreads	 	number of threads in the scheduling pool of threads.
	 * @param realTimeOfStart			real time at which the scheduling must be plan to start.
	 * @param executionDuration			duration of the real time activities.
	 * @param timeUnit					time unit in which {@code executionDuration} is expressed.
	 * @throws Exception				<i>to do</i>.
	 */
	protected			AbstractRTComponent(
		String reflectionInboundPortURI,
		int nbSchedulingThreads,
		long realTimeOfStart,
		long executionDuration,
		TimeUnit timeUnit
		) throws Exception
	{
		// FIXME: given that the component has its own threads, it needs one
		// standard thread pool for AbstractCVM submitting the method execute...
		super(reflectionInboundPortURI, 1, 0);

		assert	nbSchedulingThreads > 0 :
				new PreconditionException("nbSchedulableThreads > 0");
		assert	realTimeOfStart >= System.currentTimeMillis() +
											SchedulingI.START_DELAY_TOLERANCE :
				new PreconditionException(
						"realTimeOfStart >= "
						+ "System.currentTimeMillis() + "
						+ "SchedulingI.START_DELAY_TOLERANCE");
		assert	executionDuration > 0 :
				new PreconditionException("executionDuration");
		assert	timeUnit != null : new PreconditionException("tu != null");

		this.numberOfSchedulingThreads = nbSchedulingThreads;
		this.realTimeOfStart = realTimeOfStart;
		this.executionDuration = executionDuration;
		this.timeUnit = timeUnit;

		this.initialise();
	}

	/**
	 * initialise the component.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code SCHEDULING_THREAD_POOL_URI != null && !SCHEDULING_THREAD_POOL_URI.isEmpty()}
	 * post	{@code validExecutorServiceURI(SCHEDULING_THREAD_POOL_URI)}
	 * </pre>
	 *
	 * @throws Exception	<i>to do</i>.
	 */
	protected void		initialise() throws Exception
	{
		// create the executor service that will be used by the scheduler
		// to schedule the real time task activations.
		this.createNewExecutorService(SCHEDULING_THREAD_POOL_URI,
									  this.numberOfSchedulingThreads,
									  true);
		assert	this.validExecutorServiceURI(SCHEDULING_THREAD_POOL_URI);
	}

	// -------------------------------------------------------------------------
	// Component life-cycle methods
	// -------------------------------------------------------------------------

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#start()
	 */
	@Override
	public synchronized void	start() throws ComponentStartException
	{
		super.start();

		try {
			this.logMessage("Starting task collection...");
			this.collectPeriodicTasks();
			this.collectAperiodicTasks();
			this.collectSporadicTasks();
			
			if (DEBUG) {
				this.scheduler.logRegisteredTasks();
			}

			this.logMessage("Task collection ending.");
		} catch (Exception e) {
			throw new ComponentStartException(e) ;
		}
	}

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#execute()
	 */
	@Override
	public void			execute() throws Exception
	{
		this.startScheduling();
	}

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#finalise()
	 */
	@Override
	public synchronized void	finalise() throws Exception
	{
		if (!this.scheduler.stopped()) {
			this.scheduler.stopScheduling();
		}
		super.finalise();
	}

	/**
	 * @see fr.sorbonne_u.components.AbstractComponent#shutdown()
	 */
	@Override
	public synchronized void	shutdown() throws ComponentShutdownException
	{
		super.shutdown();
	}

	// -------------------------------------------------------------------------
	// Internal methods and services
	// -------------------------------------------------------------------------

	/**
	 * return true if the component has set its real time scheduler.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @return	true if the component has set its real time scheduler.
	 */
	protected boolean	schedulerSet()
	{
		return this.getScheduler() != null;
	}

	/**
	 * return true if {@code s} is the scheduler associated to this component.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param s	a scheduler.
	 * @return	true if {@code s} is the scheduler associated to this component.
	 */
	public boolean		isScheduler(GenericScheduler s)
	{
		return this.schedulerSet() && this.getScheduler() == s;
	}

	/**
	 * return the scheduler associated with this real time component.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null}
	 * </pre>
	 *
	 * @return	the scheduler associated with this real time component.
	 */
	protected GenericScheduler	getScheduler()
	{
		return this.scheduler;
	}

	/**
	 * start the scheduler associated with this real time component at the
	 * given real time.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code schedulerSet()}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 */
	protected void		startScheduling()
	{
		assert	schedulerSet() : new PreconditionException("schedulerSet()");

		final GenericScheduler s = this.getScheduler();
		long rts = s.convertToSchedulerTimeUnit(this.realTimeOfStart, timeUnit);
		long current = s.convertToSchedulerTimeUnit(System.currentTimeMillis(),
													TimeUnit.MILLISECONDS);
		assert	rts >= current + SchedulingI.START_DELAY_TOLERANCE;

		long delay = rts - current;
		this.scheduleTask(
				SCHEDULING_THREAD_POOL_URI,
				new AbstractComponent.AbstractTask() {
					@Override
					public void run() {
						s.startScheduling(rts);
					}
			}, delay, s.getSchedulingTimeUnit());

		long d = delay + s.convertToSchedulerTimeUnit(this.executionDuration,
													  this.timeUnit);
		this.scheduleTask(
				SCHEDULING_THREAD_POOL_URI,
				new AbstractComponent.AbstractTask() {
					@Override
					public void run() {
						s.stopScheduling();
					}
			}, d, s.getSchedulingTimeUnit());

		if (DEBUG) {
			this.logMessage("starting in " + delay + " "
												+ s.getSchedulingTimeUnit());
			this.logMessage("stopping after " + d + " " +
													s.getSchedulingTimeUnit());
		}

	}

	/**
	 * stop the scheduler associated with this real time component.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 */
	protected void		stopScheduling()
	{
		this.scheduler.stopScheduling();
	}

	/**
	 * repeated here to make it visible to classes in the same package.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no more preconditions.
	 * post	{@code true}	// no more postconditions.
	 * </pre>
	 * 
	 * @see fr.sorbonne_u.components.AbstractComponent#runTaskOnComponent(int, fr.sorbonne_u.components.ComponentI.ComponentTask)
	 */
	@Override
	protected Future<?>		runTaskOnComponent(
		int executorServiceIndex,
		ComponentTask t
		) throws AssertionError, RejectedExecutionException
	{
		return super.runTaskOnComponent(executorServiceIndex, t);
	}

	/**
	 * repeated to make the method visible to classes in the same package.
	 * 
	 * @see fr.sorbonne_u.components.AbstractComponent#scheduleTaskOnComponent(int, fr.sorbonne_u.components.ComponentI.ComponentTask, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	protected ScheduledFuture<?>	scheduleTaskOnComponent(
		int executorServiceIndex,
		ComponentTask t,
		long delay,
		TimeUnit u
		) throws AssertionError, RejectedExecutionException
	{
		return super.scheduleTaskOnComponent(executorServiceIndex, t, delay, u);
	}

	/**
	 * collect all methods in the component that are annotated by the
	 * <code>PeriodicTaskDescription</code> annotation.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @throws Exception	<i>to do</i>.
	 */
	protected void		collectPeriodicTasks() throws Exception
	{
		// methods that have already been collected
		Set<Method> present = new HashSet<Method>();
		Class<?> clazz = this.getClass();
		while (!AbstractRTComponent.class.equals(clazz)) {
			Method[] methods = clazz.getDeclaredMethods();
			for (int i = 0 ; i < methods.length ; i++) {
				if (methods[i].isAnnotationPresent(
											PeriodicTaskDescription.class) &&
						!AbstractRTComponent.shadowed(methods[i], present)) {
					present.add(methods[i]);
					PeriodicTaskDescription ptd =
						methods[i].getAnnotation(PeriodicTaskDescription.class);

					assert	ptd.no() >= 0;
					assert	ptd.timeUnit() != null;
					assert	ptd.wcet() > 0;
					assert	ptd.relativeEarliestStart() >= 0;
					assert	ptd.relativeDeadline() >
									ptd.relativeEarliestStart() + ptd.wcet();
					assert	ptd.relativeDeadline() <= ptd.period();
					assert	ptd.period() > 0;
					assert	ptd.mutexTasks() == null ||
								Arrays.stream(ptd.mutexTasks()).allMatch(
															n -> n != ptd.no());
					assert	ptd.after() == null ||
								Arrays.stream(ptd.after()).allMatch(
															n -> n != ptd.no());
					this.scheduler.createPeriodicTask(methods[i]);
				}
			}
			clazz = clazz.getSuperclass();
		}
	}

	/**
	 * collect all methods in the component that are annotated by the
	 * <code>AperiodicTaskDescription</code> annotation.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @throws Exception	<i>to do</i>.
	 */
	protected void		collectAperiodicTasks() throws Exception
	{
		// methods that have already been collected
		Set<Method> present = new HashSet<Method>();
		Class<?> clazz = this.getClass();
		while (!AbstractRTComponent.class.equals(clazz)) {
			Method[] methods = clazz.getDeclaredMethods();
			for (int i = 0 ; i < methods.length ; i++) {
				if (methods[i].isAnnotationPresent(
											AperiodicTaskDescription.class) &&
						!AbstractRTComponent.shadowed(methods[i], present)) {
					present.add(methods[i]);
					AperiodicTaskDescription atd =
							methods[i].getAnnotation(
												AperiodicTaskDescription.class);

					assert	atd.no() >= 0;
					assert	atd.timeUnit() != null;
					assert	atd.wcet() > 0;
					assert	atd.relativeDeadline() > atd.wcet();
					assert	atd.mutexTasks() == null ||
								Arrays.stream(atd.mutexTasks()).allMatch(
															n -> n != atd.no());

					((GenericScheduler)this.scheduler).
												createAperiodicTask(methods[i]);
				}
			}
			clazz = clazz.getSuperclass();
		}
	}

	/**
	 * collect all methods in the component that are annotated by the
	 * <code>SporadicTaskDescription</code> annotation.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @throws Exception	<i>to do</i>.
	 */
	protected void		collectSporadicTasks() throws Exception
	{
		// methods that have already been collected
		Set<Method> present = new HashSet<Method>();
		Class<?> clazz = this.getClass();
		while (!AbstractRTComponent.class.equals(clazz)) {
			Method[] methods = clazz.getDeclaredMethods();
			for (int i = 0 ; i < methods.length ; i++) {
				if (methods[i].isAnnotationPresent(
											SporadicTaskDescription.class) &&
						!AbstractRTComponent.shadowed(methods[i], present)) {
					present.add(methods[i]);
					SporadicTaskDescription std =
						methods[i].getAnnotation(SporadicTaskDescription.class);

					assert	std.no() >= 0;
					assert	std.timeUnit() != null;
					assert	std.wcet() > 0;
					assert	std.relativeDeadline() > std.wcet();
					assert	std.mutexTasks() == null ||
								Arrays.stream(std.mutexTasks()).allMatch(
															n -> n != std.no());
					assert	std.minimumInterActivationDelay() > std.wcet();

					((GenericScheduler)this.scheduler).
												createSporadicTask(methods[i]);
				}
			}
			clazz = clazz.getSuperclass();
		}
	}

	/**
	 * return true if a method in <code>present</code> is shadowing
	 * <code>m</code>.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code m != null && present != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param m			method in a class which may be shadowed by one in <code>present</code>.
	 * @param present	set of methods already seen in subclasses.
	 * @return			true if a method in <code>present</code> is shadowing <code>m</code>.
	 */
	protected static boolean	shadowed(Method m, Set<Method> present)
	{
		for (Method m1 : present) {
			if (AbstractRTComponent.shadow(m1, m)) return true;
		}
		return false;
	}

	/**
	 * return true if <code>m1</code> of a class C1 shadows <code>m2</code> of
	 * a class C2 if C1 is a subclass of C2.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code m1 != null && m2 != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param m1	a method of a class C1.
	 * @param m2	a method of a class C2, superclass of C1.
	 * @return		true if <code>m1</code> of a class C1 shadows <code>m2</code> of a class C2 if C1 is a subclass of C2.
	 */
	protected static boolean	shadow(Method m1, Method m2)
	{
		assert	m1 != null && m2 != null;

		boolean ret = m1.getName().equals(m2.getName());
		ret = ret && m2.getReturnType().isAssignableFrom(m1.getReturnType());
		ret = ret && (m1.getParameterCount() == m2.getParameterCount());
		if (!ret) return false;
		Class<?>[] m1ParamTypes = m1.getParameterTypes();
		Class<?>[] m2ParamTypes = m2.getParameterTypes();
		for (int i = 0 ; ret && i < m1ParamTypes.length ; i++) {
			ret = ret && m1ParamTypes[i].equals(m2ParamTypes[i]);
		}
		return ret;
	}

	/**
	 * receive a result from an asynchronous call with future on another
	 * component; <b>BEWARE</b>, this method must be redefined by a concrete
	 * real time component definition classes to set the aperiodic task
	 * properties with the proper annotation otherwise a precondition exception
	 * will be thrown at the component creation time; the redefinition must
	 * call this method through a {@code super} call.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param callURI		URI attributed to the call when it was passed to the server.
	 * @param result		the result of the call coming back from the server.
	 * @throws BCMException	<i>to do</i>.
	 */
	protected void		receiveResult(String callURI, Serializable result)
	throws BCMException
	{
		if (DEBUG) {
			this.logMessage("AbstractRTComponent::receiveResult("
							+ callURI + ", " + result + ") "
							+ Thread.currentThread());
		}

		this.rtClientSidePlugin.performSuperReceived(callURI, result);
	}
}
// -----------------------------------------------------------------------------
