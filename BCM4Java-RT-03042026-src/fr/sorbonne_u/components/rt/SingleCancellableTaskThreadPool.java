package fr.sorbonne_u.components.rt;

// Copyright Jacques Malenfant, Sorbonne Universite.
// Jacques.Malenfant@lip6.fr
//
// This software is a computer program whose purpose is to provide a
// basic component programming model to program with components
// distributed applications in the Java programming language.
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
import java.util.concurrent.TimeUnit;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.AbstractComponent.AbstractTask;
import fr.sorbonne_u.components.ComponentI.ComponentTask;
import fr.sorbonne_u.exceptions.AssertionChecking;
import fr.sorbonne_u.exceptions.PreconditionException;

// -----------------------------------------------------------------------------
/**
 * The class <code>SingleCancellableTaskThreadPool</code>
 *
 * <p><strong>Description</strong></p>
 * 
 * <p>
 * TODO: work in progress.
 * </p>
 * 
 * <p><strong>Implementation Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// TODO	// no more invariant
 * </pre>
 * 
 * <p><strong>Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// TODO	// no more invariant
 * </pre>
 * 
 * <p>Created on : 2025-07-08</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public class			SingleCancellableTaskThreadPool
{
	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	/** the real time component owning this scheduler.						*/
	protected final AbstractRTComponent	owner;
	/** index of the schedulable thread pool provided by the owner
	 *  component.															*/
	protected final int					schedulingThreadPoolIndex;
	/** the time unit used to schedule the tasks.							*/
	protected final TimeUnit			schedulingTimeUnit;

	/** future variable allowing to cancel the currently dispatched task
	 *  scheduled on a Java scheduled pool of threads.						*/
	protected Future<?>					currentDispatch;
	/** time at which the current dispatch has occurred or will occur.		*/
	protected long						currentDispatchTime;

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
		SingleCancellableTaskThreadPool instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		ret &= AssertionChecking.checkImplementationInvariant(
					true,
					SingleCancellableTaskThreadPool.class,
					instance, "");
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
		SingleCancellableTaskThreadPool instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		ret &= AssertionChecking.checkInvariant(
				true,
				SingleCancellableTaskThreadPool.class,
				instance,
				"");
		return ret;
	}

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	/**
	 * 
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code owner != null}
	 * pre	{@code owner.isSchedulable(schedulingThreadPoolIndex)}
	 * pre	{@code schedulingTimeUnit != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param owner
	 * @param schedulingThreadPoolIndex
	 * @param schedulingTimeUnit
	 */
	public					SingleCancellableTaskThreadPool(
		AbstractRTComponent owner,
		int schedulingThreadPoolIndex,
		TimeUnit schedulingTimeUnit
		)
	{
		// Preconditions checking
		assert	owner != null :
				new PreconditionException("owner != null");
		assert	owner.isSchedulable(schedulingThreadPoolIndex) :
				new PreconditionException(
						"owner.isSchedulable(schedulingThreadPoolIndex)");
		assert	schedulingTimeUnit != null :
				new PreconditionException("schedulingTimeUnit != null");

		this.owner = owner;
		this.schedulingThreadPoolIndex = schedulingThreadPoolIndex;
		this.schedulingTimeUnit = schedulingTimeUnit;
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

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
		
		long currentTime =
				this.schedulingTimeUnit.convert(System.currentTimeMillis(),
												TimeUnit.MILLISECONDS);
		return this.currentDispatch != null &&
									this.currentDispatchTime >= currentTime;
	}

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
	 * @throws AssertionError				if the preconditions are not satisfied.
	 * @throws RejectedExecutionException	if the task cannot be scheduled for execution.
	 */
	protected synchronized void	runTaskOnComponent(
		ComponentTask t
		) throws AssertionError, RejectedExecutionException
	{
		if (this.hasPlannedTask()) {
			this.currentDispatch.cancel(false);
			this.currentDispatch = null;
			this.currentDispatchTime = Long.MIN_VALUE;
		}
		this.currentDispatch =
			this.owner.runTaskOnComponent(this.schedulingThreadPoolIndex, t);
		this.currentDispatchTime =
			this.schedulingTimeUnit.convert(System.currentTimeMillis(),
											TimeUnit.MILLISECONDS);
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
	 * @throws AssertionError				if the preconditions are not satisfied.
	 * @throws RejectedExecutionException	if the task cannot be scheduled for execution.
	 */
	protected synchronized void	scheduleTaskOnComponent(
		ComponentTask t,
		long delay
		) throws AssertionError, RejectedExecutionException
	{
		if (this.hasPlannedTask()) {
			this.currentDispatch.cancel(false);
			this.currentDispatch = null;
			this.currentDispatchTime = Long.MIN_VALUE;
		}
		final SingleCancellableTaskThreadPool constantThis = this;
		AbstractTask internalTask =
				new AbstractComponent.AbstractTask() {
					@Override
					public void run() {
						synchronized (constantThis) {
							currentDispatch = null;
							currentDispatchTime = Long.MIN_VALUE;
						}
						t.run();
					}
				};
		this.currentDispatch =
				this.owner.scheduleTaskOnComponent(
									this.schedulingThreadPoolIndex,
									internalTask,
									delay,
									this.schedulingTimeUnit);
		this.currentDispatchTime =
				this.schedulingTimeUnit.convert(System.currentTimeMillis(),
												TimeUnit.MILLISECONDS)
				+ delay;
	}
}
// -----------------------------------------------------------------------------
