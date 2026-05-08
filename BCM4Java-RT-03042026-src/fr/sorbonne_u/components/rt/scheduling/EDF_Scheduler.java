package fr.sorbonne_u.components.rt.scheduling;

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
import fr.sorbonne_u.components.AbstractComponent.AbstractTask;
import fr.sorbonne_u.components.exceptions.BCMRuntimeException;
import fr.sorbonne_u.components.rt.AbstractRTComponent;
import fr.sorbonne_u.components.rt.GenericScheduler;
import fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTaskActivation;
import fr.sorbonne_u.exceptions.PreconditionException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

// -----------------------------------------------------------------------------
/**
 * The class <code>EDF_Scheduler</code> implements a simple earliest deadline
 * first real time task scheduler.
 *
 * <p><strong>Description</strong></p>
 * 
 * <p>
 * In EDF scheduler in general, tasks are expected to:
 * </p>
 * <ul>
 * <li>be periodic, preemptive and independent of each others;</li>
 * <li>be activated at each beginning of the scheduling cycle;</li>
 * <li>have an earliest start time equals to the activation time;</li>
 * <li>have a deadline at each end of scheduling cycle;</li>
 * <li>have a known worst-case execution time.</li>
 * </ul>
 * <p>
 * In this EDF scheduler, tasks are non preemptive; all tasks are executed until
 * termination (due to constraints imposed by the Java executor service).
 * Aperiodic and sporadic tasks are taken into account, hence the traditional
 * schedulability guarantees of an EDF scheduler are only true if the
 * application does not use aperiodic and sporadic tasks.
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
public class			EDF_Scheduler
extends		GenericScheduler
{
	// -------------------------------------------------------------------------
	// Inner types and classes
	// -------------------------------------------------------------------------

	/**
	 * The class <code>ActivationsTrace</code> implements a tracer for task
	 * activations that can provide the times at which each task has been
	 * activated as well as the delays between each successive activation of
	 * each task.
	 *
	 * <p><strong>Description</strong></p>
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
	 * <p>Created on : 2026-04-03</p>
	 * 
	 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
	 */
	public static class	ActivationsTrace
	{
		// ---------------------------------------------------------------------
		// Constants and variables
		// ---------------------------------------------------------------------

		/** start time of the scheduling in Unix Epoch time.				*/
		protected long						startTime;
		/** log of activation times of tasks.								*/
		protected final ArrayList<Long>		activationTimes;
		/** log of names of activated tasks.								*/
		protected final ArrayList<String>	taskNames;
		/** index of the next entry in the activations log.					*/
		protected int						nextIndex;

		// ---------------------------------------------------------------------
		// Constructors
		// ---------------------------------------------------------------------

		/**
		 * create a new instance.
		 * 
		 * <p><strong>Contract</strong></p>
		 * 
		 * <pre>
		 * pre	{@code true}	// no precondition.
		 * post	{@code true}	// no postcondition.
		 * </pre>
		 *
		 */
		public			ActivationsTrace()
		{
			this.activationTimes = new ArrayList<>();
			this.taskNames = new ArrayList<>();
			this.nextIndex = 0;
		}

		// -------------------------------------------------------------------------
		// Methods
		// -------------------------------------------------------------------------

		/**
		 * set the start time of the scheduling.
		 * 
		 * <p><strong>Contract</strong></p>
		 * 
		 * <pre>
		 * pre	{@code startTime > 0}
		 * post	{@code true}	// no postcondition.
		 * </pre>
		 *
		 * @param startTime
		 */
		public void		setStartTime(long startTime)
		{
			assert	startTime > 0 : new PreconditionException("startTime > 0");
			this.startTime = startTime;
		}

		/**
		 * return the start time of the scheduling.
		 * 
		 * <p><strong>Contract</strong></p>
		 * 
		 * <pre>
		 * pre	{@code true}	// no precondition.
		 * post	{@code return > 0}
		 * </pre>
		 *
		 * @return	the start time of the scheduling.
		 */
		public long		getStartTime()
		{
			return this.startTime;
		}

		/**
		 * log the information about a new activation.
		 * 
		 * <p><strong>Contract</strong></p>
		 * 
		 * <pre>
		 * pre	{@code name != null && !name.isEmpty()}
		 * pre	{@code activationTime > getStartTime()}
		 * post	{@code true}	// no postcondition.
		 * </pre>
		 *
		 * @param name				name of the activated task.
		 * @param activationTime	time at which the task has been activated.
		 */
		public void		addNewActivationTrace(String name, long activationTime)
		{
			assert	name != null && !name.isEmpty() :
					new PreconditionException("name != null && !name.isEmpty()");
			assert	activationTime > getStartTime() :
					new PreconditionException("activationTime > getStartTime()");

			this.activationTimes.add(this.nextIndex, activationTime);
			this.taskNames.add(this.nextIndex, name);
			this.nextIndex++;
		}

		/**
		 * add the traces accumulated in this activations trace instance to
		 * the given string buffer.
		 * 
		 * <p><strong>Contract</strong></p>
		 * 
		 * <pre>
		 * pre	{@code sb != null}
		 * post	{@code true}	// no postcondition.
		 * </pre>
		 *
		 * @param sb	string buffer to which the tracves must be added.
		 */
		public void		printTrace(StringBuffer sb)
		{
			assert	sb != null : new PreconditionException("sb != null");

			HashMap<String,Long> lastActivations = new HashMap<>();
			for (int i = 0 ; i < this.taskNames.size() ; i++) {
				if (!lastActivations.containsKey(this.taskNames.get(i))) {
					lastActivations.put(this.taskNames.get(i), this.startTime);
				}
			}
			sb.append("start | ");
			sb.append(this.startTime);
			sb.append('\n');
			for (int i = 0 ; i < this.taskNames.size() ; i++) {
				sb.append(this.taskNames.get(i));
				sb.append(" | ");
				sb.append(this.activationTimes.get(i));
				sb.append(" | ");
				sb.append(this.activationTimes.get(i) -
									lastActivations.get(this.taskNames.get(i)));
				sb.append('\n');
				lastActivations.put(this.taskNames.get(i),
									this.activationTimes.get(i));
			}
		}
	}

	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	/** when true, the scheduler traces the task activations.				*/
	public static boolean		TRACING = true;
	/** local trace of task activations.								 	*/
	protected ActivationsTrace	localTrace;
	/** local debugging log of the tasks dispatches.						*/
	protected ArrayList<String>	localLog = new ArrayList<>();

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	/**
	 * create a new EDF scheduler.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code owner != null}
	 * pre	{@code schedulingThreadPoolIndex >= 0}
	 * pre	{@code owner.validExecutorServiceIndex(schedulingThreadPoolIndex)}
	 * pre	{@code owner.isSchedulable(schedulingThreadPoolIndex)}
	 * pre	{@code schedulingTimeUnit != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param owner						owner real time component.
	 * @param schedulingThreadPoolIndex	index of the schedulable thread pool provided by the owner component.
	 * @param timeUnit					time unit used to express times and durations in this scheduler.
	 */
	public				EDF_Scheduler(
		AbstractRTComponent owner,
		int schedulingThreadPoolIndex,
		TimeUnit timeUnit
		)
	{
		super(owner, schedulingThreadPoolIndex, timeUnit);

		if (TRACING) {
			this.localTrace = new ActivationsTrace();
		}
	}

	// -------------------------------------------------------------------------
	// Internal methods
	// -------------------------------------------------------------------------

	/**
	 * @see fr.sorbonne_u.components.rt.GenericScheduler#isEligible(fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTaskActivation)
	 */
	@Override
	protected boolean	isEligible(AbstractRealTimeTaskActivation rta)
	{
		long currentTime = this.getCurrentTime();
		return rta.getAbsoluteEarliestTime() <= currentTime;
	}

	/**
	 * @see fr.sorbonne_u.components.rt.GenericScheduler#getEligibleComparator()
	 */
	@Override
	protected AbstractTaskActivationComparator	getEligibleComparator()
	{
		return new EDF_EligibleComparator();
	}

	/**
	 * @see fr.sorbonne_u.components.rt.GenericScheduler#getWaitingComparator()
	 */
	@Override
	protected AbstractTaskActivationComparator	getWaitingComparator()
	{
		return new WaitingTaskComparator();
	}

	/**
	 * @see fr.sorbonne_u.components.rt.GenericScheduler#dispatchNextTask()
	 */
	@Override
	protected void		dispatchNextTask()
	{
		if (this.stopped()) {
			if (GenericScheduler.DEBUG) {
				this.getOwner().traceMessage(
									"EDF_Scheduler::dispatchNext stopped\n");
			}
			return;
		}

		this.currentDispatchLock.lock();
		try {
			long currentTime = this.getCurrentTime();

			StringBuffer localLogEntry =  null;
			if (TRACING) {
				localLogEntry = new StringBuffer(currentTime + " | ");
				localLogEntry.append(" [1 ");
				this.logQueues(localLogEntry);
				localLogEntry.append(" 1];");
			}

			if (DEBUG) {
				this.logTaskActivationsQueues();
			}

			if (this.hasPlannedTask()) {
				this.logMessage(
						"EDF_Scheduler::dispatchNext replan next dispatch.");
				if (TRACING) {
					localLogEntry.append(
						" EDF_Scheduler::dispatchNext replan next dispatch;");
				}
				this.currentDispatch.cancel(false);
				this.currentDispatch = null;
				this.currentDispatchTime = Long.MIN_VALUE;
			}
			if (!this.eligibleTasks.isEmpty()) {
				final AbstractRealTimeTaskActivation
							selectedTaskActivation = this.eligibleTasks.peek();
				long delay =
						selectedTaskActivation.getAbsoluteEarliestTime()
																- currentTime;
				assert	delay <= 0 : new BCMRuntimeException("delay <= 0");
				if (DEBUG) {
					this.logMessage("EDF_Scheduler::dispatchNextTask = " +
									selectedTaskActivation.getName());
				}
				this.currentDispatchTime = currentTime;
				
				if (TRACING) {
					localLogEntry.append(" dispatching ");
					localLogEntry.append(selectedTaskActivation.getName());
					localLogEntry.append(";");
				}
				this.currentDispatch =
					this.runTaskOnComponent(
						new AbstractTask() {
							@Override
							public void run() {
								currentDispatchLock.lock();
								try {
									try {
										AbstractRealTimeTaskActivation ta =
														eligibleTasks.remove();
										assert	ta == selectedTaskActivation :
												new BCMRuntimeException(
														"ta == selectedTaskActivation");
										ta.execute();
										if (TRACING) {
											localTrace.addNewActivationTrace(
												ta.getName(),
												System.currentTimeMillis());
										}
									} catch (Exception e) {
										logMessage(
												"task activation " +
												selectedTaskActivation +
												" aborted with exception " + e);
									}
									currentDispatch = null;
									currentDispatchTime = Long.MIN_VALUE;
									if (DEBUG) {
										logMessage("dispatching next task");
									}
									transferEligible();
									dispatchNow();
								} finally {
									currentDispatchLock.unlock();
								}
							}
						});
				if (DEBUG) {
					this.getOwner().traceMessage(
										"EDF_Scheduler#dispatchNext executes " + 
										selectedTaskActivation + "\n");
				} else {}
			} else {
				// no eligible tasks, look for the next task waiting to become
				// eligible and plan the transfer at the right time
				if (!this.waitingTasks.isEmpty()) {
					this.currentDispatchTime =
								this.waitingTasks.peek().
													getAbsoluteEarliestTime();
					if (TRACING) {
						localLogEntry.append(" next waiting becoming eligible = ");
						localLogEntry.append(this.currentDispatchTime);
						localLogEntry.append(';');
					}
					long delay = this.currentDispatchTime - currentTime;
					this.currentDispatch =
						this.scheduleTaskOnComponent(
								new AbstractTask() {
									@Override
									public void run() {
										currentDispatchLock.lock();
										try {
											currentDispatch = null;
											currentDispatchTime = Long.MIN_VALUE;
										} finally {
											currentDispatchLock.unlock();
										}
										transferEligible();
										dispatchNextTask();
									}
								},
								delay);
				} // else no eligible nor waiting task so, stay idle
			}

			if (DEBUG) {
				this.logTaskActivationsQueues();
			}
			if (TRACING) {
				localLogEntry.append(" [2 ");
				this.logQueues(localLogEntry);
				localLogEntry.append(" 2]\n");
				this.localLog.add(localLogEntry.toString());
			}
		} finally {
			this.currentDispatchLock.unlock();
		}
	}

	/**
	 * @see fr.sorbonne_u.components.rt.GenericScheduler#startScheduling(long)
	 */
	@Override
	public void			startScheduling(long realTimeOfStart)
	{
		if (TRACING) {
			this.localTrace.setStartTime(realTimeOfStart);
		}
		super.startScheduling(realTimeOfStart);
	}

	/**
	 * @see fr.sorbonne_u.components.rt.GenericScheduler#stopScheduling()
	 */
	@Override
	public void			stopScheduling()
	{
		super.stopScheduling();

		if (TRACING) {
			StringBuffer sb =
					new StringBuffer("\nScheduling trace for the component ");
			sb.append(this.getOwner().getReflectionInboundPortURI());
			sb.append("\n\n");
			this.localTrace.printTrace(sb);
			for (String s : this.localLog) {
				sb.append(s);
			}
			String logOutput = sb.toString();
			this.logMessage(logOutput);
		}
	}

	/**
	 * log the eligible and waiting tasks queues in the given string buffer.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code sb != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param sb	string buffer into which the log is put.
	 */
	private void	logQueues(StringBuffer sb)
	{
		assert	sb != null : new PreconditionException("sb != null");

		Iterator<AbstractRealTimeTaskActivation> iter =
												this.eligibleTasks.iterator();
		int size = this.eligibleTasks.size();
		int i = 0;
		sb.append("Eligible = {");
		while (iter.hasNext()) {
			AbstractRealTimeTaskActivation t = iter.next();
			sb.append(t.getName());
			if (i < size - 1) {
				sb.append(", ");
			}
			i++;
		}
		sb.append("}; ");
		sb.append("Waiting = {");
		iter = this.waitingTasks.iterator();
		size = this.waitingTasks.size();
		i = 0;
		while (iter.hasNext()) {
			AbstractRealTimeTaskActivation t = iter.next();
			sb.append(t.getName());
			if (i < size - 1) {
				sb.append(", ");
			}
			i++;
		}
		sb.append("}");
	}
}
// -----------------------------------------------------------------------------
