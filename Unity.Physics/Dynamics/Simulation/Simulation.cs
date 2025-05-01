using System;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Jobs;
using Unity.Mathematics;

namespace Unity.Physics
{
    /// <summary>
    /// Holds temporary data in a storage that lives as long as simulation lives and is only re-
    /// allocated if necessary.
    /// </summary>
    public struct SimulationContext : IDisposable
    {
        private int m_NumDynamicBodies;
        private NativeArray<Velocity> m_InputVelocities;

        // Solver stabilization data (it's completely ok to be unallocated)
        [NativeDisableContainerSafetyRestriction]
        private NativeArray<Solver.StabilizationMotionData> m_SolverStabilizationMotionData;

        internal NativeArray<Solver.StabilizationMotionData> SolverStabilizationMotionData =>
            this.m_SolverStabilizationMotionData.GetSubArray(0, this.m_NumDynamicBodies);

        internal float TimeStep;

        internal NativeArray<Velocity> InputVelocities => this.m_InputVelocities.GetSubArray(0, this.m_NumDynamicBodies);

        internal NativeStream CollisionEventDataStream;
        internal NativeStream TriggerEventDataStream;
        internal NativeStream ImpulseEventDataStream;

        /// <summary>   Gets the collision events. </summary>
        ///
        /// <value> The collision events. </value>
        public CollisionEvents CollisionEvents => new(this.CollisionEventDataStream, this.InputVelocities, this.TimeStep);

        /// <summary>   Gets the trigger events. </summary>
        ///
        /// <value> The trigger events. </value>
        public TriggerEvents TriggerEvents => new(this.TriggerEventDataStream);

        /// <summary>   Gets the impulse events. </summary>
        ///
        /// <value> The impulse events. </value>
        public ImpulseEvents ImpulseEvents => new(this.ImpulseEventDataStream);

        private NativeArray<int> WorkItemCount;

        internal bool ReadyForEventScheduling => this.m_InputVelocities.IsCreated && this.CollisionEventDataStream.IsCreated &&
            this.TriggerEventDataStream.IsCreated && this.ImpulseEventDataStream.IsCreated;

        /// <summary>
        /// Resets the simulation storage
        /// - Reallocates input velocities storage if necessary
        /// - Disposes event streams and allocates new ones with a single work item
        /// NOTE: Reset or ScheduleReset needs to be called before passing the SimulationContext to a
        /// simulation step job. If you don't then you may get initialization errors.
        /// </summary>
        ///
        /// <param name="stepInput">    The step input. </param>
        public void Reset(SimulationStepInput stepInput)
        {
            this.m_NumDynamicBodies = stepInput.World.NumDynamicBodies;
            if (!this.m_InputVelocities.IsCreated || this.m_InputVelocities.Length < this.m_NumDynamicBodies)
            {
                if (this.m_InputVelocities.IsCreated)
                {
                    this.m_InputVelocities.Dispose();
                }

                this.m_InputVelocities = new NativeArray<Velocity>(this.m_NumDynamicBodies, Allocator.Persistent, NativeArrayOptions.UninitializedMemory);
            }

            // Solver stabilization data
            if (stepInput.SolverStabilizationHeuristicSettings.EnableSolverStabilization)
            {
                if (!this.m_SolverStabilizationMotionData.IsCreated || this.m_SolverStabilizationMotionData.Length < this.m_NumDynamicBodies)
                {
                    if (this.m_SolverStabilizationMotionData.IsCreated)
                    {
                        this.m_SolverStabilizationMotionData.Dispose();
                    }

                    this.m_SolverStabilizationMotionData =
                        new NativeArray<Solver.StabilizationMotionData>(this.m_NumDynamicBodies, Allocator.Persistent, NativeArrayOptions.ClearMemory);
                }
                else if (this.m_NumDynamicBodies > 0)
                {
                    unsafe
                    {
                        UnsafeUtility.MemClear(this.m_SolverStabilizationMotionData.GetUnsafePtr(),
                            this.m_NumDynamicBodies * UnsafeUtility.SizeOf<Solver.StabilizationMotionData>());
                    }
                }
            }

            if (this.CollisionEventDataStream.IsCreated)
            {
                this.CollisionEventDataStream.Dispose();
            }

            if (this.TriggerEventDataStream.IsCreated)
            {
                this.TriggerEventDataStream.Dispose();
            }

            if (this.ImpulseEventDataStream.IsCreated)
            {
                this.ImpulseEventDataStream.Dispose();
            }

            {
                if (!this.WorkItemCount.IsCreated)
                {
                    this.WorkItemCount = new NativeArray<int>(1, Allocator.Persistent);
                    this.WorkItemCount[0] = 1;
                }

                this.CollisionEventDataStream = new NativeStream(this.WorkItemCount[0], Allocator.Persistent);
                this.TriggerEventDataStream = new NativeStream(this.WorkItemCount[0], Allocator.Persistent);
                this.ImpulseEventDataStream = new NativeStream(this.WorkItemCount[0], Allocator.Persistent);
            }
        }

        // TODO: We need to make a public version of ScheduleReset for use with
        // local simulation calling StepImmediate and chaining jobs over a number
        // of steps. This becomes a problem if new bodies are added to the world
        // between simulation steps.
        // A public version could take the form:
        //         public JobHandle ScheduleReset(ref PhysicsWorld world, JobHandle inputDeps = default)
        //         {
        //             return ScheduleReset(ref world, inputDeps, true);
        //         }
        // However, to make that possible we need a why to allocate InputVelocities within a job.
        // The core simulation does not chain jobs across multiple simulation steps and so
        // will not hit this issue.
        internal JobHandle ScheduleReset(SimulationStepInput stepInput, JobHandle inputDeps, bool allocateEventDataStreams)
        {
            this.m_NumDynamicBodies = stepInput.World.NumDynamicBodies;
            if (!this.m_InputVelocities.IsCreated || this.m_InputVelocities.Length < this.m_NumDynamicBodies)
            {
                // TODO: can we find a way to setup InputVelocities within a job?
                if (this.m_InputVelocities.IsCreated)
                {
                    this.m_InputVelocities.Dispose();
                }

                this.m_InputVelocities = new NativeArray<Velocity>(this.m_NumDynamicBodies, Allocator.Persistent, NativeArrayOptions.UninitializedMemory);
            }

            // Solver stabilization data
            if (stepInput.SolverStabilizationHeuristicSettings.EnableSolverStabilization)
            {
                if (!this.m_SolverStabilizationMotionData.IsCreated || this.m_SolverStabilizationMotionData.Length < this.m_NumDynamicBodies)
                {
                    if (this.m_SolverStabilizationMotionData.IsCreated)
                    {
                        this.m_SolverStabilizationMotionData.Dispose();
                    }

                    this.m_SolverStabilizationMotionData =
                        new NativeArray<Solver.StabilizationMotionData>(this.m_NumDynamicBodies, Allocator.Persistent, NativeArrayOptions.ClearMemory);
                }
                else if (this.m_NumDynamicBodies > 0)
                {
                    unsafe
                    {
                        UnsafeUtility.MemClear(this.m_SolverStabilizationMotionData.GetUnsafePtr(),
                            this.m_NumDynamicBodies * UnsafeUtility.SizeOf<Solver.StabilizationMotionData>());
                    }
                }
            }

            var handle = inputDeps;
            if (this.CollisionEventDataStream.IsCreated)
            {
                handle = this.CollisionEventDataStream.Dispose(handle);
            }

            if (this.TriggerEventDataStream.IsCreated)
            {
                handle = this.TriggerEventDataStream.Dispose(handle);
            }

            if (this.ImpulseEventDataStream.IsCreated)
            {
                handle = this.ImpulseEventDataStream.Dispose(handle);
            }

            if (allocateEventDataStreams)
            {
                if (!this.WorkItemCount.IsCreated)
                {
                    this.WorkItemCount = new NativeArray<int>(1, Allocator.Persistent);
                    this.WorkItemCount[0] = 1;
                }

                handle = NativeStream.ScheduleConstruct(out this.CollisionEventDataStream, this.WorkItemCount, handle, Allocator.Persistent);
                handle = NativeStream.ScheduleConstruct(out this.TriggerEventDataStream, this.WorkItemCount, handle, Allocator.Persistent);
                handle = NativeStream.ScheduleConstruct(out this.ImpulseEventDataStream, this.WorkItemCount, handle, Allocator.Persistent);
            }

            return handle;
        }

        /// <summary>
        /// Disposes the simulation context.
        /// </summary>
        public void Dispose()
        {
            if (this.m_InputVelocities.IsCreated)
            {
                this.m_InputVelocities.Dispose();
            }

            if (this.m_SolverStabilizationMotionData.IsCreated)
            {
                this.m_SolverStabilizationMotionData.Dispose();
            }

            if (this.CollisionEventDataStream.IsCreated)
            {
                this.CollisionEventDataStream.Dispose();
            }

            if (this.TriggerEventDataStream.IsCreated)
            {
                this.TriggerEventDataStream.Dispose();
            }

            if (this.ImpulseEventDataStream.IsCreated)
            {
                this.ImpulseEventDataStream.Dispose();
            }

            if (this.WorkItemCount.IsCreated)
            {
                this.WorkItemCount.Dispose();
            }
        }
    }

    // Temporary data created and destroyed during the step
    internal struct StepContext
    {
        // Built by the scheduler. Groups body pairs into phases in which each
        // body appears at most once, so that the interactions within each phase can be solved
        // in parallel with each other but not with other phases. This is consumed by the
        // ProcessBodyPairsJob, which outputs contact and joint Jacobians.
        public NativeList<DispatchPairSequencer.DispatchPair> PhasedDispatchPairs;

        // Job handle for the scheduler's job that creates the phased dispatch pairs.
        // Results will appear in the SolverSchedulerInfo property upon job completion.
        public JobHandle CreatePhasedDispatchPairsJobHandle;

        // Built by the scheduler. Describes the grouping of phased dispatch pairs for parallel processing
        // of joints and contacts in the solver.
        // Informs how we can schedule the solver jobs and what data locations they read info from.
        public DispatchPairSequencer.SolverSchedulerInfo SolverSchedulerInfo;

        public NativeStream Contacts;
        public NativeStream Jacobians;
    }

    /// <summary>   Steps a physics world. </summary>
    public struct Simulation : ISimulation
    {
        /// <summary>   Gets the simulation type. </summary>
        ///
        /// <value> <see cref="SimulationType.UnityPhysics"/>. </value>
        public SimulationType Type => SimulationType.UnityPhysics;

        /// <summary>   Gets the handle of the final simulation job (not including dispose jobs). </summary>
        ///
        /// <value> The final simulation job handle. </value>
        public JobHandle FinalSimulationJobHandle => this.m_StepHandles.FinalExecutionHandle;

        internal SimulationScheduleStage m_SimulationScheduleStage;

        internal StepContext StepContext;

        /// <summary>   Gets the contacts stream. </summary>
        ///
        /// This value is only valid after the CreateContactsJob (Narrowphase System), and before BuildJacobiansJob (CreateJacobiansSystem)
        ///
        /// <value> The contacts stream-->. </value>
        public readonly NativeStream Contacts => this.StepContext.Contacts;

        /// <summary>   Gets the collision events. </summary>
        ///
        /// <value> The collision events. </value>
        public CollisionEvents CollisionEvents => this.SimulationContext.CollisionEvents;

        /// <summary>   Gets the trigger events. </summary>
        ///
        /// <value> The trigger events. </value>
        public TriggerEvents TriggerEvents => this.SimulationContext.TriggerEvents;

        /// <summary>   Gets the impulse events. </summary>
        ///
        /// <value> The impulse events. </value>
        public ImpulseEvents ImpulseEvents => this.SimulationContext.ImpulseEvents;

        internal SimulationContext SimulationContext;

        private DispatchPairSequencer m_Scheduler;
        internal SimulationJobHandles m_StepHandles;

        internal bool ReadyForEventScheduling => this.SimulationContext.ReadyForEventScheduling;

        /// <summary>   Creates a new Simulation. </summary>
        ///
        /// <returns>   A Simulation. </returns>
        public static Simulation Create()
        {
            var sim = new Simulation();
            sim.Init();
            return sim;
        }

        /// <summary>
        /// Disposes the simulation.
        /// </summary>
        public void Dispose()
        {
            this.m_Scheduler.Dispose();
            this.SimulationContext.Dispose();
        }

        private void Init()
        {
            this.StepContext = new StepContext();
            this.SimulationContext = new SimulationContext();
            this.m_Scheduler = DispatchPairSequencer.Create();
            this.m_StepHandles = new SimulationJobHandles(new JobHandle());
            this.m_SimulationScheduleStage = SimulationScheduleStage.Idle;
        }

        /// <summary>
        /// Steps the simulation immediately on a single thread without spawning any jobs.
        /// </summary>
        ///
        /// <param name="input">                The input. </param>
        /// <param name="simulationContext">    [in,out] Context for the simulation. </param>
        public static void StepImmediate(SimulationStepInput input, ref SimulationContext simulationContext)
        {
            SafetyChecks.CheckFiniteAndPositiveAndThrow(input.TimeStep, nameof(input.TimeStep));
            SafetyChecks.CheckInRangeAndThrow(input.NumSolverIterations, new int2(1, int.MaxValue), nameof(input.NumSolverIterations));

            if (input.World.NumDynamicBodies == 0)
            {
                // No need to do anything, since nothing can move
                return;
            }

            // Inform the context of the timeStep: this is a frame timestep
            simulationContext.TimeStep = input.TimeStep;

            // Find all body pairs that overlap in the broadphase
            var dynamicVsDynamicBodyPairs = new NativeStream(1, Allocator.Temp);
            var dynamicVsStaticBodyPairs = new NativeStream(1, Allocator.Temp);
            {
                var dynamicVsDynamicBodyPairsWriter = dynamicVsDynamicBodyPairs.AsWriter();
                var dynamicVsStaticBodyPairsWriter = dynamicVsStaticBodyPairs.AsWriter();
                input.World.CollisionWorld.FindOverlaps(ref dynamicVsDynamicBodyPairsWriter, ref dynamicVsStaticBodyPairsWriter);
            }

            // Create dispatch pairs
            var dispatchPairs = new NativeList<DispatchPairSequencer.DispatchPair>(Allocator.Temp);
            DispatchPairSequencer.CreateDispatchPairs(ref dynamicVsDynamicBodyPairs, ref dynamicVsStaticBodyPairs, input.World.NumDynamicBodies,
                input.World.Joints, ref dispatchPairs);

            var contacts = new NativeStream(1, Allocator.Temp);
            {
                if (input.NumSubsteps <= 1)
                {
                    // Integrate gravity using frame timestep
                    Solver.ApplyGravityAndUpdateInputVelocities(input.World.DynamicsWorld.MotionVelocities, simulationContext.InputVelocities, input.Gravity,
                        input.TimeStep);

                    var contactsWriter = contacts.AsWriter();
                    NarrowPhase.CreateContacts(ref input.World, simulationContext.InputVelocities, dispatchPairs.AsArray(), input.TimeStep,
                        ref contactsWriter); //Frame timestep
                }
                else // Using substeps
                {
                    // Predict linear velocity at the end of a frame under the influence of gravity
                    var copyInputVelocities = new NativeArray<Velocity>(input.World.DynamicsWorld.MotionVelocities.Length, Allocator.Temp);
                    var velocityFromGravity = input.TimeStep * input.Gravity;
                    Solver.UpdateInputVelocities(input.World.DynamicsWorld.MotionVelocities, copyInputVelocities, velocityFromGravity);

                    var contactsWriter = contacts.AsWriter();
                    NarrowPhase.CreateContacts(ref input.World, copyInputVelocities, dispatchPairs.AsArray(), input.TimeStep,
                        ref contactsWriter); //Frame timestep

                    copyInputVelocities.Dispose();

                    // Integrate gravity using substep timestep
                    Solver.ApplyGravityAndUpdateInputVelocities(input.World.DynamicsWorld.MotionVelocities, simulationContext.InputVelocities, input.Gravity,
                        input.SubstepTimeStep);
                }
            }

            // Build Jacobians
            var jacobians = new NativeStream(1, Allocator.Temp);
            {
                var contactsReader = contacts.AsReader();
                var jacobiansWriter = jacobians.AsWriter();
                Solver.BuildJacobians(ref input.World, input.SubstepTimeStep, math.length(input.Gravity), input.NumSubsteps, input.NumSolverIterations,
                    dispatchPairs.AsArray(), ref contactsReader, ref jacobiansWriter);
            }

            var stepInput = new Solver.StepInput()
            {
                Gravity = input.Gravity,
                Timestep = input.SubstepTimeStep,
                InvTimestep = Solver.CalculateInvTimeStep(input.SubstepTimeStep),
                InvNumSolverIterations = 1.0f / input.NumSolverIterations,
                NumSubsteps = input.NumSubsteps,
                NumSolverIterations = input.NumSolverIterations,
                CurrentSubstep = -1,
                CurrentSolverIteration = -1,
            };

            // Iterate through substeps
            for (var i = 0; i < input.NumSubsteps; i++)
            {
                stepInput.CurrentSubstep = i;

                var jacobiansReader = jacobians.AsReader();
                if (i > 0) // First substep will be covered by the Jacobians.Build stage
                {
                    Solver.ApplyGravityAndUpdateInputVelocities(input.World.DynamicsWorld.MotionVelocities, simulationContext.InputVelocities, input.Gravity,
                        input.SubstepTimeStep);

                    Solver.Update(0, input.World.DynamicsWorld.MotionDatas, input.World.DynamicsWorld.MotionVelocities, input.World.CollisionWorld.Bodies,
                        ref jacobiansReader, stepInput);
                }

                var collisionEventsWriter = simulationContext.CollisionEventDataStream.AsWriter();
                var triggerEventsWriter = simulationContext.TriggerEventDataStream.AsWriter();
                var impulseEventsWriter = simulationContext.ImpulseEventDataStream.AsWriter();
                var solverStabilizationData = new Solver.StabilizationData(input, simulationContext);

                Solver.SolveJacobians(ref jacobiansReader, input.World.DynamicsWorld.MotionVelocities, stepInput, ref collisionEventsWriter,
                    ref triggerEventsWriter, ref impulseEventsWriter, solverStabilizationData);

                // Integrate motions
                Integrator.Integrate(input.World.DynamicsWorld.MotionDatas, input.World.DynamicsWorld.MotionVelocities, input.SubstepTimeStep);
            }

            // Synchronize the collision world if asked for
            if (input.SynchronizeCollisionWorld)
            {
                input.World.CollisionWorld.UpdateDynamicTree(ref input.World, input.TimeStep, input.Gravity);
            }
        }

        /// <summary>   Schedule broadphase jobs. </summary>
        ///
        /// <param name="input">            The input. </param>
        /// <param name="inputDeps">        The input deps. </param>
        /// <param name="multiThreaded">    (Optional) True if multi threaded. </param>
        ///
        /// <returns>   The SimulationJobHandles. </returns>
        public SimulationJobHandles ScheduleBroadphaseJobs(SimulationStepInput input, JobHandle inputDeps, bool multiThreaded = true)
        {
            return this.ScheduleBroadphaseJobsInternal(input, inputDeps, multiThreaded, false, false);
        }

        internal SimulationJobHandles ScheduleBroadphaseJobsInternal(
            SimulationStepInput input, JobHandle inputDeps, bool multiThreaded, bool incrementalDynamicBroadphase, bool incrementalStaticBroadphase)
        {
            SafetyChecks.CheckFiniteAndPositiveAndThrow(input.TimeStep, nameof(input.TimeStep));
            SafetyChecks.CheckInRangeAndThrow(input.NumSolverIterations, new int2(1, int.MaxValue), nameof(input.NumSolverIterations));
            SafetyChecks.CheckSimulationStageAndThrow(this.m_SimulationScheduleStage, SimulationScheduleStage.Idle);
            this.m_SimulationScheduleStage = SimulationScheduleStage.PostCreateBodyPairs;

            // Dispose and reallocate input velocity buffer, if dynamic body count has increased.
            // Dispose previous collision, trigger and impulse event data streams.
            // New event streams are reallocated later when the work item count is known.
            var handle = this.SimulationContext.ScheduleReset(input, inputDeps, false);
            this.SimulationContext.TimeStep = input.TimeStep;

            this.StepContext = new StepContext();

            if (input.World.NumDynamicBodies == 0)
            {
                // No need to do anything, since nothing can move
                this.m_StepHandles = new SimulationJobHandles(handle);
                return this.m_StepHandles;
            }

            // Find all body pairs that overlap in the broadphase
            var handles = input.World.CollisionWorld.ScheduleFindOverlapsJobsInternal(out var dynamicVsDynamicBodyPairs, out var dynamicVsStaticBodyPairs,
                handle, multiThreaded, incrementalDynamicBroadphase, incrementalStaticBroadphase);

            handle = handles.FinalExecutionHandle;
            var postOverlapsHandle = handle;

            // Sort all overlapping and jointed body pairs into phases
            handles = this.m_Scheduler.ScheduleCreatePhasedDispatchPairsJob(ref input.World, ref dynamicVsDynamicBodyPairs, ref dynamicVsStaticBodyPairs,
                handle, ref this.StepContext.PhasedDispatchPairs, out this.StepContext.SolverSchedulerInfo, multiThreaded);

            this.StepContext.CreatePhasedDispatchPairsJobHandle = handles.FinalExecutionHandle;

            this.m_StepHandles.FinalExecutionHandle =
                multiThreaded ? JobHandle.CombineDependencies(handles.FinalExecutionHandle, postOverlapsHandle) : handles.FinalExecutionHandle;

            return this.m_StepHandles;
        }

        /// <summary>   Schedule narrowphase jobs. </summary>
        ///
        /// <param name="input">            The input. </param>
        /// <param name="inputDeps">        The input deps. </param>
        /// <param name="multiThreaded">    (Optional) True if multi threaded. </param>
        ///
        /// <returns>   The SimulationJobHandles. </returns>
        public SimulationJobHandles ScheduleNarrowphaseJobs(SimulationStepInput input, JobHandle inputDeps, bool multiThreaded = true)
        {
            SafetyChecks.CheckSimulationStageAndThrow(this.m_SimulationScheduleStage, SimulationScheduleStage.PostCreateBodyPairs);
            this.m_SimulationScheduleStage = SimulationScheduleStage.PostCreateContacts;

            if (input.World.NumDynamicBodies == 0)
            {
                // No need to do anything, since nothing can move
                this.m_StepHandles = new SimulationJobHandles(inputDeps);
                return this.m_StepHandles;
            }

            if (input.NumSubsteps <= 1)
            {
                // Integrate gravity so Jacobian Build is using same source data as Jacobian Solve
                var handle = Solver.ScheduleApplyGravityAndUpdateInputVelocitiesJob(ref input.World.DynamicsWorld, this.SimulationContext.InputVelocities,
                    input.Gravity, input.TimeStep, inputDeps, multiThreaded);

                this.m_StepHandles = NarrowPhase.ScheduleCreateContactsJobs(ref input.World, input.TimeStep, this.SimulationContext.InputVelocities,
                    ref this.StepContext.Contacts, ref this.StepContext.Jacobians, ref this.StepContext.PhasedDispatchPairs, handle,
                    ref this.StepContext.SolverSchedulerInfo, multiThreaded);
            }
            else // Using substeps
            {
                // Predict Linear Velocity at the end of a frame under the influence of gravity
                var copyInputVelocities = new NativeArray<Velocity>(input.World.DynamicsWorld.MotionVelocities.Length, Allocator.TempJob);
                var velocityFromGravity = input.TimeStep * input.Gravity;
                var handle = Solver.ScheduleUpdateInputVelocitiesJob(input.World.DynamicsWorld.MotionVelocities, copyInputVelocities, velocityFromGravity,
                    inputDeps, multiThreaded);

                // Create contacts using the velocity prediction data for the full frame timestep
                this.m_StepHandles = NarrowPhase.ScheduleCreateContactsJobs(ref input.World, input.TimeStep, copyInputVelocities, ref this.StepContext.Contacts,
                    ref this.StepContext.Jacobians, ref this.StepContext.PhasedDispatchPairs, handle, ref this.StepContext.SolverSchedulerInfo, multiThreaded);

                var copyHandle = copyInputVelocities.Dispose(this.m_StepHandles.FinalExecutionHandle);
                this.m_StepHandles.FinalExecutionHandle = JobHandle.CombineDependencies(copyHandle, this.m_StepHandles.FinalExecutionHandle);
            }

            return this.m_StepHandles;
        }

        /// <summary>   Schedule create jacobians jobs. </summary>
        ///
        /// <param name="input">            The input. </param>
        /// <param name="inputDeps">        The input deps. </param>
        /// <param name="multiThreaded">    (Optional) True if multi threaded. </param>
        ///
        /// <returns>   The SimulationJobHandles. </returns>
        public SimulationJobHandles ScheduleCreateJacobiansJobs(SimulationStepInput input, JobHandle inputDeps, bool multiThreaded = true)
        {
            SafetyChecks.CheckSimulationStageAndThrow(this.m_SimulationScheduleStage, SimulationScheduleStage.PostCreateContacts);
            this.m_SimulationScheduleStage = SimulationScheduleStage.PostCreateJacobians;

            if (input.World.NumDynamicBodies == 0)
            {
                // No need to do anything, since nothing can move
                this.m_StepHandles = new SimulationJobHandles(inputDeps);
                return this.m_StepHandles;
            }

            if (input.NumSubsteps > 1)
            {
                // Integrate gravity so Jacobian Build is using same source data as Jacobian Solve
                inputDeps = Solver.ScheduleApplyGravityAndUpdateInputVelocitiesJob(ref input.World.DynamicsWorld, this.SimulationContext.InputVelocities,
                    input.Gravity, input.SubstepTimeStep, inputDeps, multiThreaded);
            }

            // Create/Initialize Jacobians for first substep
            this.m_StepHandles = Solver.ScheduleBuildJacobiansJobs(ref input.World, input.SubstepTimeStep, math.length(input.Gravity), input.NumSubsteps,
                input.NumSolverIterations, inputDeps, ref this.StepContext.PhasedDispatchPairs, ref this.StepContext.SolverSchedulerInfo,
                ref this.StepContext.Contacts, ref this.StepContext.Jacobians, multiThreaded);

            return this.m_StepHandles;
        }

        /// <summary>   Schedule solve and integrate jobs. </summary>
        ///
        /// <param name="input">            The input. </param>
        /// <param name="inputDeps">        The input deps. </param>
        /// <param name="multiThreaded">    (Optional) True if multi threaded. </param>
        ///
        /// <returns>   The SimulationJobHandles. </returns>
        public SimulationJobHandles ScheduleSolveAndIntegrateJobs(SimulationStepInput input, JobHandle inputDeps, bool multiThreaded = true)
        {
            SafetyChecks.CheckSimulationStageAndThrow(this.m_SimulationScheduleStage, SimulationScheduleStage.PostCreateJacobians);
            this.m_SimulationScheduleStage = SimulationScheduleStage.Idle;

            if (input.World.NumDynamicBodies == 0)
            {
                // No need to do anything, since nothing can move
                this.m_StepHandles = new SimulationJobHandles(inputDeps);
                return this.m_StepHandles;
            }

            // Make sure we know the number of phased dispatch pairs so that we can efficiently schedule the solve jobs
            // in Solver.ScheduleSolveJacobiansJobs() below

            var executionHandle = this.m_StepHandles.FinalExecutionHandle;
            var jobHandle = inputDeps;
            var solverStabilizationData = new Solver.StabilizationData(input, this.SimulationContext);

            // Initialize the event streams outside of the substep loop. These should only be written to on the last
            // solver iteration of the last substep
            //TODO: Change NativeStream allocations to Allocator.TempJob when leaks are fixed https://github.com/Unity-Technologies/Unity.Physics/issues/7
            if (!multiThreaded)
            {
                this.SimulationContext.CollisionEventDataStream = new NativeStream(1, Allocator.Persistent);
                this.SimulationContext.TriggerEventDataStream = new NativeStream(1, Allocator.Persistent);
                this.SimulationContext.ImpulseEventDataStream = new NativeStream(1, Allocator.Persistent);
            }
            else
            {
                var workItemList = this.StepContext.SolverSchedulerInfo.NumWorkItems;
                var collisionEventStreamHandle = NativeStream.ScheduleConstruct(out this.SimulationContext.CollisionEventDataStream, workItemList, inputDeps,
                    Allocator.Persistent);

                var triggerEventStreamHandle = NativeStream.ScheduleConstruct(
                    out this.SimulationContext.TriggerEventDataStream, workItemList, inputDeps, Allocator.Persistent);

                var impulseEventStreamHandle = NativeStream.ScheduleConstruct(
                    out this.SimulationContext.ImpulseEventDataStream, workItemList, inputDeps, Allocator.Persistent);

                var streamJobHandle = JobHandle.CombineDependencies(collisionEventStreamHandle, triggerEventStreamHandle, impulseEventStreamHandle);
                jobHandle = JobHandle.CombineDependencies(jobHandle, streamJobHandle);
            }

            var stepInput = new Solver.StepInput()
            {
                Gravity = input.Gravity,
                Timestep = input.SubstepTimeStep,
                InvTimestep = Solver.CalculateInvTimeStep(input.SubstepTimeStep),
                InvNumSolverIterations = 1.0f / input.NumSolverIterations,
                NumSubsteps = input.NumSubsteps,
                NumSolverIterations = input.NumSolverIterations,
                CurrentSubstep = -1,
                CurrentSolverIteration = -1,
            };

            for (var i = 0; i < input.NumSubsteps; i++)
            {
                stepInput.CurrentSubstep = i;

                if (i > 0) // Gravity integration for first substep is covered in the Jacobians.Build stage
                {
                    jobHandle = Solver.ScheduleApplyGravityAndUpdateInputVelocitiesJob(ref input.World.DynamicsWorld, this.SimulationContext.InputVelocities,
                        input.Gravity, input.SubstepTimeStep, jobHandle, multiThreaded);

                    jobHandle = Solver.ScheduleUpdateJacobiansJobs(ref input.World, ref this.StepContext.Jacobians, ref this.StepContext.SolverSchedulerInfo,
                        stepInput, jobHandle, multiThreaded);
                }

                // Solve Jacobians
                this.m_StepHandles = Solver.ScheduleSolveJacobiansJobs(ref input.World.DynamicsWorld, stepInput, ref this.StepContext.Jacobians,
                    ref this.SimulationContext.CollisionEventDataStream, ref this.SimulationContext.TriggerEventDataStream,
                    ref this.SimulationContext.ImpulseEventDataStream, ref this.StepContext.SolverSchedulerInfo, solverStabilizationData, jobHandle,
                    multiThreaded);

                // Integrate motions (updates MotionDatas, MotionVelocities)
                jobHandle = Integrator.ScheduleIntegrateJobs(ref input.World.DynamicsWorld, input.SubstepTimeStep, this.m_StepHandles.FinalExecutionHandle,
                    multiThreaded);

                jobHandle = JobHandle.CombineDependencies(jobHandle, executionHandle, this.m_StepHandles.FinalExecutionHandle);

                if (stepInput.IsLastSubstep)
                {
                    this.m_StepHandles.FinalExecutionHandle = jobHandle;
                }
            }

            this.m_StepHandles.FinalExecutionHandle = JobHandle.CombineDependencies(this.m_StepHandles.FinalExecutionHandle, jobHandle);

            // Synchronize the collision world
            if (input.SynchronizeCollisionWorld)
            {
                this.m_StepHandles.FinalExecutionHandle = input.World.CollisionWorld.ScheduleUpdateDynamicTree(ref input.World, input.TimeStep, input.Gravity,
                    this.m_StepHandles.FinalExecutionHandle, multiThreaded);
            }

            // Different dispose logic for single threaded simulation compared to "standard" threading (multi threaded)
            if (!multiThreaded)
            {
                // Note: In the multithreaded case, StepContext.PhasedDispatchPairs is disposed in Solver.ScheduleBuildJacobiansJobs().
                this.StepContext.PhasedDispatchPairs.Dispose(this.m_StepHandles.FinalExecutionHandle);

                this.StepContext.Contacts.Dispose(this.m_StepHandles.FinalExecutionHandle);
                this.StepContext.Jacobians.Dispose(this.m_StepHandles.FinalExecutionHandle);
                this.StepContext.SolverSchedulerInfo.ScheduleDisposeJob(this.m_StepHandles.FinalExecutionHandle);
            }

            return this.m_StepHandles;
        }

        /// <summary>
        /// Resets the simulation storage
        /// - Reallocates input velocities storage if necessary
        /// - Disposes event streams and allocates new ones with a single work item
        /// </summary>
        /// <param name="input">    The input. </param>
        public void ResetSimulationContext(SimulationStepInput input)
        {
            this.SimulationContext.Reset(input);
        }

        /// <summary>   Steps the world immediately. </summary>
        ///
        /// <param name="input">    The input. </param>
        public void Step(SimulationStepInput input)
        {
            StepImmediate(input, ref this.SimulationContext);
        }

        /// <summary>
        /// Schedule all the jobs for the simulation step. Enqueued callbacks can choose to inject
        /// additional jobs at defined sync points. multiThreaded defines which simulation type will be
        /// called:
        ///     - true will result in default multithreaded simulation
        ///     - false will result in a very small number of jobs (1 per physics step phase) that are
        ///     scheduled sequentially
        /// Behavior doesn't change regardless of the multiThreaded argument provided.
        /// </summary>
        ///
        /// <param name="input">            The input. </param>
        /// <param name="inputDeps">        The input deps. </param>
        /// <param name="multiThreaded">    (Optional) True if multi threaded. </param>
        ///
        /// <returns>   The SimulationJobHandles. </returns>
        public unsafe SimulationJobHandles ScheduleStepJobs(SimulationStepInput input, JobHandle inputDeps, bool multiThreaded = true)
        {
            this.ScheduleBroadphaseJobs(input, inputDeps, multiThreaded);
            this.ScheduleNarrowphaseJobs(input, this.m_StepHandles.FinalExecutionHandle, multiThreaded);
            this.ScheduleCreateJacobiansJobs(input, this.m_StepHandles.FinalExecutionHandle, multiThreaded);
            this.ScheduleSolveAndIntegrateJobs(input, this.m_StepHandles.FinalExecutionHandle, multiThreaded);

            return this.m_StepHandles;
        }
    }
}
