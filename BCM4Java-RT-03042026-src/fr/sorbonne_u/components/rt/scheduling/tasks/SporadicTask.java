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

import java.lang.reflect.Method;
import fr.sorbonne_u.components.rt.AbstractRTComponent;
import fr.sorbonne_u.components.rt.annotations.SporadicTaskDescription;
import fr.sorbonne_u.components.rt.asynccall.AbstractRTAsyncCall;
import fr.sorbonne_u.components.rt.scheduling.SchedulingI;
import fr.sorbonne_u.exceptions.AssertionChecking;
import fr.sorbonne_u.exceptions.ImplementationInvariantException;
import fr.sorbonne_u.exceptions.InvariantException;
import fr.sorbonne_u.exceptions.PreconditionException;

// -----------------------------------------------------------------------------
/**
 * The class <code>SporadicTask</code> implements a sporadic task activation
 * for BCM4Java real time schedulers that is executed by calling a method
 * (properly annotated with {@code SporadicTaskDescription}).
 *
 * <p><strong>Description</strong></p>
 * 
 * <p>
 * The basic and most standard way to use sporadic tasks in a real time
 * component is to activate it upon the occurrence of an internal event that
 * must be treated by the task. As the event is internal, the event handler is
 * a method called by the component itself, so it is possible to have a lowest
 * bound on the delay between two successive activations. Most of the time, no
 * result will be returned by such a task that rather calls an actuator or
 * prepare information for a future task that will do.
 * </p>
 * <p>
 * With this implementation, it is also possible to activate a sporadic task
 * and get a completable future from its {@code SporadicTaskActivation}, which
 * will be completed by the task when it will be available. Hence, for example,
 * a periodic task activation may start a computation as a sporadic task
 * activation and have one of its future activation look at the completable
 * future to get the result when it will be known. In a real time component,
 * looking for such a future result should never block the task
 * (no {@code get}). The usual pattern is either to call a non blocking test,
 * for example {@code getNow} or {@code isDone}, and be prepared to do
 * something else if the value is not available yet.
 * </p>
 * 
 * <p><strong>Implementation Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code method.isAnnotationPresent(SporadicTaskDescription.class)}
 * </pre>
 * 
 * <p><strong>Invariants</strong></p>
 * 
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * <p>Created on : 2021-04-20</p>
 * 
 * @author	<a href="mailto:Jacques.Malenfant@lip6.fr">Jacques Malenfant</a>
 */
public class			SporadicTask
extends		AbstractAperiodicTask
{
	// -------------------------------------------------------------------------
	// Constants and variables
	// -------------------------------------------------------------------------

	/** minimum delay between two successive activations of this task.		*/
	protected final long	minimumInterActivationDelay;
	/** last absolute time at which this task was activated.				*/
	protected long			lastActivationTime;

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
		SporadicTask instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		ret &= AssertionChecking.checkImplementationInvariant(
				instance.method.isAnnotationPresent(
											SporadicTaskDescription.class),
				SporadicTask.class,
				instance,
				"method.isAnnotationPresent(SporadicTaskDescription.class)");
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
		SporadicTask instance
		)
	{
		assert instance != null : new PreconditionException("instance != null");

		boolean ret = true;
		ret &= AssertionChecking.checkInvariant(
				instance.getMinimumInterActivationDelay() > instance.getWcet(),
				SporadicTask.class,
				instance,
				"getMinimumInterActivationDelay() > getWcet()");
		return ret;
	}

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	/**
	 * create a new sporadic task.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code owner != null}
	 * pre	{@code scheduler != null}
	 * pre	{@code method != null}
	 * pre	{@code method.getDeclaringClass().isAssignableFrom(owner.getClass())}
	 * pre	{@code method.isAnnotationPresent(SporadicTaskDescription.class)}
	 * post	{@code getNumber() == method.getAnnotation(AperiodicTaskDescription.class).no()}
	 * post	{@code getTimeUnit() == method.getAnnotation(AperiodicTaskDescription.class).timeUnit()}
	 * post	{@code getWcet() == method.getAnnotation(AperiodicTaskDescription.class).wcet()}
	 * post	{@code getRelativeDeadline() == method.getAnnotation(AperiodicTaskDescription.class).relativeDeadline()}
	 * post	{@code getMutexTasks() == null || getMutexTasks().equals(method.getAnnotation(AperiodicTaskDescription.class).mutexTasks())}
	 * post	{@code getName().equals(method.getName())}
	 * post	{@code isVarArgs() == method.isVarArgs()}
	 * post	{@code getNumberOfParameters() == method.getParameterCount()}
	 * post	{@code getMinimumInterActivationDelay() == method.getAnnotation(SporadicTaskDescription.class).minimumInterActivationDelay()}
	 * </pre>
	 *
	 * @param owner			real time component holding the code this task require to execute.
	 * @param scheduler		scheduler responsible to trigger the execution of the task activations; associated with the owner component.
	 * @param method		the method object to be invoked on the owner to execute the code of the task.
	 * @throws Exception	when {@code method == null} or the {@code SporadicTaskDescription} is not present on it.
	 */
	public				SporadicTask(
		AbstractRTComponent owner,
		SchedulingI scheduler,
		Method method
		) throws Exception
	{
		super(owner,
			  scheduler,
			  AssertionChecking.assertTrueAndReturnOrThrow(
					method != null &&
						method.isAnnotationPresent(SporadicTaskDescription.class),
					method,
					() -> new PreconditionException("method != null")).
			  getAnnotation(SporadicTaskDescription.class).no(),
			  method.getAnnotation(SporadicTaskDescription.class).timeUnit(),
			  method.getAnnotation(SporadicTaskDescription.class).wcet(),
			  method.getAnnotation(SporadicTaskDescription.class).relativeDeadline(),
			  method.getAnnotation(SporadicTaskDescription.class).mutexTasks(),
			  method);

		this.minimumInterActivationDelay =
				method.getAnnotation(SporadicTaskDescription.class).
												minimumInterActivationDelay();
		this.lastActivationTime = Long.MIN_VALUE;

		assert 	SporadicTask.implementationInvariants(this) :
				new ImplementationInvariantException(
						"SporadicTask.implementationInvariants(this)");
		assert	SporadicTask.invariants(this) :
				new InvariantException("SporadicTask.invariants(this)");
	}

	// -------------------------------------------------------------------------
	// Methods
	// -------------------------------------------------------------------------

	/**
	 * return the minimum delay between two successive activations of this task.
	 * 
	 * <p><strong>Contract</strong></p>
	 * 
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return > getWcet()}
	 * </pre>
	 *
	 * @return	the minimum delay between two successive activations of this task.
	 */
	public long			getMinimumInterActivationDelay()
	{
		return this.minimumInterActivationDelay;
	}

	/**
	 * activate the aperiodic task at the given real time.
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
	 * @return					a new aperiodic task activation.
	 */
	public AperiodicTaskActivation	activate(
		long activationTime,
		Object[] params
		)
	{
		return this.activate(activationTime, params, null);
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.tasks.AbstractAperiodicTask#activate(long, java.lang.Object[], fr.sorbonne_u.components.rt.asynccall.AbstractRTAsyncCall)
	 */
	@Override
	public AperiodicTaskActivation	activate(
		long activationTime,
		Object[] params,
		AbstractRTAsyncCall	resultSendingProxy
		)
	{
		assert	activationTime > 0 : 
				new PreconditionException("activationTime > 0");

		long d = this.scheduler.convertToSchedulerTimeUnit(
												minimumInterActivationDelay,
												this.getTimeUnit());
		if (activationTime < this.lastActivationTime + d) {
			throw new RuntimeException(
					"sporadic task " + this.getName()
					+ " activated at " + activationTime
					+ " rejected: delay too small since last activation "
					+ d + " " + this.scheduler.getSchedulingTimeUnit());
		}
		this.lastActivationTime = activationTime;

		assert 	SporadicTask.implementationInvariants(this) :
				new ImplementationInvariantException(
						"SporadicTask.implementationInvariants(this)");
		assert	SporadicTask.invariants(this) :
				new InvariantException("SporadicTask.invariants(this)");

		return new AperiodicTaskActivation(
							this, this.scheduler, activationTime, params,
							resultSendingProxy);
	}

	/**
	 * @see fr.sorbonne_u.components.rt.scheduling.tasks.AperiodicTask#toStringContent(java.lang.StringBuffer)
	 */
	@Override
	protected void		toStringContent(StringBuffer sb)
	{
		super.toStringContent(sb);
		sb.append("; minimum delay between successive activations = ");
		sb.append(this.getMinimumInterActivationDelay());
	}
}
// -----------------------------------------------------------------------------
