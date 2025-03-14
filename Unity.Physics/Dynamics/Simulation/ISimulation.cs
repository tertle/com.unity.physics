using System;
using Unity.Collections;
using Unity.Jobs;
using Unity.Mathematics;

namespace Unity.Physics
{
    /// <summary>   Implementations of ISimulation. </summary>
    public enum SimulationType
    {
        /// <summary>   A dummy implementation which does nothing. </summary>
        NoPhysics,
        /// <summary>   Default C# implementation. </summary>
        UnityPhysics,
#if HAVOK_PHYSICS_EXISTS
        /// <summary>   Havok implementation (using C++ plugin) </summary>
        HavokPhysics
#endif
    }

    internal enum SimulationScheduleStage
    {
        Idle,
        PostCreateBodyPairs,
        PostCreateContacts,
        PostCreateJacobians,
    }

    /// <summary>   Parameters for a simulation step. </summary>
    public struct SimulationStepInput
    {
        int m_NumSubsteps;

        /// <summary>   Physics world to be stepped. </summary>
        public PhysicsWorld World;
        /// <summary>   Portion of time to step the physics world for. This is the frame timestep. </summary>
        public float TimeStep;
        /// <summary>   Gravity in the physics world, a vector in m/s^2. </summary>
        public float3 Gravity;
        /// <summary>   Number of substep iterations to perform while solving constraints. No substepping will occur when set to 1. </summary>
        public int NumSubsteps { get => m_NumSubsteps; set => m_NumSubsteps = value <= 0 ? 1 : value; }

        /// <summary>   Number of Gauss-Seidel iterations to perform while solving constraints. </summary>
        public int NumSolverIterations;

        /// <summary>
        /// Whether to update the collision world after the step for more precise queries.
        /// </summary>
        public bool SynchronizeCollisionWorld;
        /// <summary>   Settings for solver stabilization heuristic in Unity.Physics. </summary>
        public Solver.StabilizationHeuristicSettings SolverStabilizationHeuristicSettings;
        /// <summary>   Used for optimization of static body synchronization. </summary>
        public NativeReference<int>.ReadOnly HaveStaticBodiesChanged;

        /// <summary>   Time step used by the Jacobians during build and solve for substepping. </summary>
        public float SubstepTimeStep => TimeStep / NumSubsteps;
    }

    /// <summary>   Result of ISimulation.ScheduleStepJobs() </summary>
    public struct SimulationJobHandles
    {
        /// <summary>   Final execution handle. Does not include dispose jobs. </summary>
        public JobHandle FinalExecutionHandle;

        /// <summary>   Constructor. </summary>
        ///
        /// <param name="handle">   The handle. </param>
        public SimulationJobHandles(JobHandle handle)
        {
            FinalExecutionHandle = handle;
        }
    }

    /// <summary>   Interface for simulations. </summary>
    public interface ISimulation : IDisposable
    {
        /// <summary>   The implementation type. </summary>
        ///
        /// <value> The type. </value>
        SimulationType Type { get; }

        /// <summary>   Step the simulation. </summary>
        ///
        /// <param name="input">    The input. </param>
        void Step(SimulationStepInput input);

        /// <summary>   Schedule a set of jobs to step the simulation. </summary>
        ///
        /// <param name="input">            The input. </param>
        /// <param name="inputDeps">        The input deps. </param>
        /// <param name="multiThreaded">    (Optional) True if multi threaded. </param>
        ///
        /// <returns>   The SimulationJobHandles. </returns>
        SimulationJobHandles ScheduleStepJobs(SimulationStepInput input, JobHandle inputDeps, bool multiThreaded = true);

        /// <summary>
        /// The final scheduled simulation job. Jobs which use the simulation results should depend on
        /// this.
        /// </summary>
        ///
        /// <value> The final simulation job handle. </value>
        JobHandle FinalSimulationJobHandle { get; }
    }

    // A simulation which does nothing
    internal struct DummySimulation : ISimulation
    {
        public SimulationType Type => SimulationType.NoPhysics;

        public void Dispose() {}
        public void Step(SimulationStepInput input) {}
        public SimulationJobHandles ScheduleStepJobs(SimulationStepInput input, JobHandle inputDeps, bool multiThreaded = true) =>
            new SimulationJobHandles(inputDeps);

        public JobHandle FinalSimulationJobHandle => new JobHandle();
        public JobHandle FinalJobHandle => new JobHandle();
    }
}
