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

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import fr.sorbonne_u.components.rt.AbstractRTComponent;
import fr.sorbonne_u.components.rt.annotations.PeriodicTaskDescription;
import fr.sorbonne_u.components.rt.scheduling.SchedulingI;
import fr.sorbonne_u.exceptions.AssertionChecking;
import fr.sorbonne_u.exceptions.ImplementationInvariantException;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PreconditionException;

// -----------------------------------------------------------------------------
/**
 * The class <code>PeriodicTask</code> defines the information describing a
 * periodic real time task for a real time component.
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
 * invariant	{@code getRelativeDeadline() >= getRelativeEarliestStart() + getWcet()}
 * invariant	{@code getPeriod() >= getDeadline()}
 * invariant	{@code getAfter() == null || getNumber() > 0 || }
 * invariant	{@code getAfter() == null || Arrays.stream(getAfter()).allMatch(n -> n > 0 && n != getNumber())}
 * </pre>
 * 
 * <p>Created on : 2021-03-03</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public class			PeriodicTask
extends		AbstractRealTimeTask
{
	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	/** earliest time of start for the task relative to the activation time,
	 *  in the inherited {@code timeUnit}.									*/
	protected final long	relativeEarliestStart;
	/** period of the task in the inherited {@code timeUnit}.				*/
	protected final long	period;
	/** array of task numbers for tasks which this one is depending upon
	 *  hence must be executed after them in the cycle.						*/
	protected final int[]	after;

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
			PeriodicTask instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
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
		PeriodicTask instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		ret &= AssertionChecking.checkInvariant(
				instance.getRelativeDeadline() >=
						instance.getRelativeEarliestStart() + instance.getWcet(),
				PeriodicTask.class,
				instance,
				"getRelativeDeadline() >= getRelativeEarliestStart() + "
				+ "getWcet()");
		ret &= AssertionChecking.checkInvariant(
				instance.getPeriod() >= instance.getRelativeDeadline(),
				PeriodicTask.class,
				instance,
				"getPeriod() >= getDeadline()");
		ret &= AssertionChecking.checkInvariant(
				instance.getAfter() == null ||
						Arrays.stream(instance.getAfter()).allMatch(
											n -> n >= 0 && n != instance.no),
				PeriodicTask.class,
				instance,
				"getAfter() == null || Arrays.stream(getAfter()).allMatch("
				+ "n -> n >= 0 && n != no)");
		return ret;
	}

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	/**
	 * create a new periodic task.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code owner != null}
	 * pre	{@code scheduler != null}
	 * pre	{@code method != null}
	 * pre	{@code method.getDeclaringClass().isAssignableFrom(owner.getClass())}
	 * pre	{@code method.isAnnotationPresent(PeriodicTaskDescription.class)}
	 * post	{@code getNumber() == method.getAnnotation(AperiodicTaskDescription.class).no()}
	 * post	{@code getTimeUnit() == method.getAnnotation(AperiodicTaskDescription.class).timeUnit()}
	 * post	{@code getWcet() == method.getAnnotation(AperiodicTaskDescription.class).wcet()}
	 * post	{@code getRelativeDeadline() == method.getAnnotation(AperiodicTaskDescription.class).relativeDeadline()}
	 * post	{@code getMutexTasks() == null || getMutexTasks().equals(method.getAnnotation(AperiodicTaskDescription.class).mutexTasks())}
	 * post	{@code getName().equals(method.getName())}
	 * post	{@code isVarArgs() == method.isVarArgs()}
	 * post	{@code getNumberOfParameters() == method.getParameterCount()}
	 * post	{@code getRelativeEarliestStart() == method.getAnnotation(PeriodicTaskDescription.class).relativeEarliestStart()}
	 * post	{@code getPeriod() == method.getAnnotation(PeriodicTaskDescription.class).period()}
	 * post	{@code getAfter().equals(method.getAnnotation(PeriodicTaskDescription.class).after())}
	 * </pre>
	 *
	 * @param owner			real time component holding the code this task require to execute.
	 * @param scheduler		scheduler responsible to trigger the execution of the task activations; associated with the owner component.
	 * @param method		the method in the owner component that this task will execute.
	 * @throws Exception	when {@code method == null} or the {@code PeriodicTaskDescription} is not present on it.
	 */
	public				PeriodicTask(
		AbstractRTComponent owner,
		SchedulingI scheduler,
		Method method
		) throws Exception
	{
		super(owner,
			  scheduler,
			  AssertionChecking.assertTrueAndReturnOrThrow(
					method != null &&
						method.isAnnotationPresent(PeriodicTaskDescription.class),
					method,
					() -> new PreconditionException("method != null")).
				getAnnotation(PeriodicTaskDescription.class).no(),
			  method.getAnnotation(PeriodicTaskDescription.class).timeUnit(),
			  method.getAnnotation(PeriodicTaskDescription.class).wcet(),
			  method.getAnnotation(PeriodicTaskDescription.class).relativeDeadline(),
			  method.getAnnotation(PeriodicTaskDescription.class).mutexTasks(),
			  method);

		PeriodicTaskDescription pt =
						method.getAnnotation(PeriodicTaskDescription.class);
		this.period = pt.period();
		this.relativeEarliestStart = pt.relativeEarliestStart();
		this.after = pt.after();

		assert 	PeriodicTask.implementationInvariants(this) :
				new ImplementationInvariantException(
						"PeriodicTask.implementationInvariants(this)");
		assert	AbstractRealTimeTask.invariants(this) :
				new InvariantException("PeriodicTask.invariants(this)");
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	/**
	 * return earliest time of start for the task; for periodic tasks, this
	 * is relative to the scheduling cycle start time.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return >= 0}
	 * </pre>
	 *
	 * @return	earliest time of start for the task; for periodic tasks, this is relative to the scheduling cycle start time.
	 */
	public long			getRelativeEarliestStart()
	{
		return this.relativeEarliestStart;
	}

	/**
	 * return the period of the task.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return > 0}
	 * </pre>
	 *
	 * @return	the period of the task.
	 */
	public long			getPeriod()
	{
		return this.period;
	}

	/**
	 * return the array of the numbers of tasks this one depends upon.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null && Arrays.stream(return).allMatch(n -> n > 0 && n != getNumber())}
	 * </pre>
	 *
	 * @return	the array of the numbers of tasks this one depends upon.
	 */
	public int[]		getAfter()
	{
		return this.after;
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTask#activate(long, java.lang.Object[])
	 */
	@Override
	public PeriodicTaskActivation	activate(
		long activationTime,
		Object[] params
		)
	{
		assert	activationTime > 0 : 
				new PreconditionException("activationTime > 0");

		return new PeriodicTaskActivation(
								this, this.scheduler, activationTime, params);
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTask#execute(java.lang.Object[])
	 */
	@Override
	public Serializable		execute(Object[] params) throws Exception
	{
		assert	params != null : new PreconditionException("params != null");

		this.method.invoke(this.owner, params);
		return null;
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.tasks.AbstractRealTimeTask#toStringContent(java.lang.StringBuffer)
	 */
	@Override
	public void			toStringContent(StringBuffer sb)
	{
		super.toStringContent(sb);
		sb.append("; period = ");
		sb.append(this.getPeriod());
		sb.append("; relativeEarliestStart = ");
		sb.append(this.relativeEarliestStart);
		if (this.after != null) {
			sb.append("; preceding = [");
			for (int i = 0 ; i < this.getAfter().length; i++) {
				sb.append(this.getAfter()[i]);
				if (i < this.getAfter().length - 1) {
					sb.append(", ");
				}
			}
			sb.append(']');
		}
	}
}
// -----------------------------------------------------------------------------
