// Generic Options: //

// When enabled, outputs processing information about the solver in each frame, such as which solver is used and the
// number of islands processed by the direct solver.
//#define DEBUG_LOG_SOLVER_INFO

// Direct Solver Options: //

// When enabled, the direct solver will solve independent islands in parallel. Disable this option for simplifying debugging activities.
#define DIRECT_SOLVER_SOLVE_ISLANDS_IN_PARALLEL

using System;
using System.Runtime.CompilerServices;
using Unity.Burst;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Jobs;
using Unity.Mathematics;
using UnityEngine;
using UnityEngine.Assertions;
using static Unity.Physics.Math;

namespace Unity.Physics
{
    /// <summary>
    /// <para> Solver types, used for calculating joint and contact forces.</para>
    /// <para> Types appear in order of decreasing solver performance but increasing accuracy.</para>
    /// </summary>
    public enum SolverType : byte
    {
        /// <summary>
        ///     <para> Iterative solver (default). Fast and approximate.</para>
        ///     <para> Ideal for situations involving many colliding rigid bodies.</para>
        /// </summary>
        Iterative = 0,

        /// <summary>
        ///     <para> Direct solver. Accurate but more computationally demanding.</para>
        ///     <para> Ideal for situations involving complex jointed mechanisms with
        ///     high mass or stiffness ratios, and for accurate contact resolution and friction modeling.
        ///     See also <see cref="Solver.DirectSolverSettings"/>.</para>
        /// </summary>
        Direct = 1
    }

    /// <summary>   A static class that exposes Solver configuration structures. </summary>
    public static partial class Solver
    {
        /// <summary> Default solver type </summary>
        public const SolverType kDefaultSolverType = SolverType.Iterative;

        /// <summary>
        /// Takes the time step in ms and returns the inverse of the time step (simulation frequency) in Hz.
        /// </summary>
        /// <param name="timestep"> The time step in ms. </param>
        /// <returns> The inverse time step (simulation frequency) in Hz. </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float CalculateInvTimestep(float timestep)
        {
            return math.select(0.0f, 1.0f / timestep, timestep > 0.0f);
        }

        /// <summary>   Settings for controlling the solver stabilization heuristic. </summary>
        public struct StabilizationHeuristicSettings
        {
            byte m_EnableSolverStabilization;

            /// <summary>   Global switch to enable/disable the whole heuristic (false by default) </summary>
            ///
            /// <value> True if enable solver stabilization, false if not. </value>
            public bool EnableSolverStabilization
            {
                get => m_EnableSolverStabilization > 0;
                set => m_EnableSolverStabilization = (byte)(value ? 1 : 0);
            }

            // Individual features control (only valid when EnableSolverStabilizationHeuristic is true)

            byte m_EnableFrictionVelocities;

            /// <summary>
            /// Switch to enable/disable heuristic when calculating friction velocities. Should be disabled
            /// only if it is causing behavior issues.
            /// </summary>
            ///
            /// <value> True if enable friction velocities, false if not. </value>
            public bool EnableFrictionVelocities
            {
                get => m_EnableFrictionVelocities > 0;
                set => m_EnableFrictionVelocities = (byte)(value ? 1 : 0);
            }

            /// <summary>
            /// Controls the intensity of the velocity clipping. Defaults to 1.0f, while other values will
            /// scale the intensity up/down. Shouldn't go higher than 5.0f, as it will result in bad behavior
            /// (too aggressive velocity clipping). Set it to 0.0f to disable the feature.
            /// </summary>
            public float VelocityClippingFactor;

            /// <summary>
            /// Controls the intensity of inertia scaling. Defaults to 1.0f, while other values will scale
            /// the intensity up/down. Shouldn't go higher than 5.0f, as it will result in bad behavior (too
            /// high inertia of bodies). Set it to 0.0f to disable the feature.
            /// </summary>
            public float InertiaScalingFactor;

            /// <summary>   The defualt stabilization options. </summary>
            public static readonly StabilizationHeuristicSettings Default = new StabilizationHeuristicSettings
            {
                m_EnableSolverStabilization = 0,
                m_EnableFrictionVelocities = 1,
                VelocityClippingFactor = 1.0f,
                InertiaScalingFactor = 1.0f
            };
        }

        /// <summary>   Data used for solver stabilization. </summary>
        public struct StabilizationData
        {
            /// <summary>   Constructor. </summary>
            ///
            /// <param name="stepInput">    The step input. </param>
            /// <param name="context">      The context. </param>
            public StabilizationData(SimulationStepInput stepInput, SimulationContext context)
            {
                StabilizationHeuristicSettings = stepInput.SolverStabilizationHeuristicSettings;
                Gravity = stepInput.Gravity;
                if (stepInput.SolverStabilizationHeuristicSettings.EnableSolverStabilization)
                {
                    InputVelocities = context.InputVelocities;
                    MotionData = context.SolverStabilizationMotionData;
                }
                else
                {
                    InputVelocities = default;
                    MotionData = default;
                }
            }

            // Settings for stabilization heuristics
            internal StabilizationHeuristicSettings StabilizationHeuristicSettings;

            // Disable container safety restriction because it will complain about aliasing
            // with SimulationContext buffers, and it is aliasing, but completely safe.
            // Also, we need the ability to have these not allocated when the feature is not used.

            // Data source: copy of gravity integrated MotionData from output of UpdateInputVelocitiesJob
            [NativeDisableParallelForRestriction]
            [NativeDisableContainerSafetyRestriction]
            internal NativeArray<Velocity> InputVelocities;

            [NativeDisableParallelForRestriction]
            [NativeDisableContainerSafetyRestriction]
            internal NativeArray<StabilizationMotionData> MotionData;

            // Gravity is used to define thresholds for stabilization,
            // and it's not needed in the solver unless stabilization is required.
            internal float3 Gravity;
        }

        // Per motion data for solver stabilization
        internal struct StabilizationMotionData
        {
            public float InverseInertiaScale;
            public byte NumPairs;
        }

        // Internal motion data input for the solver stabilization
        internal struct MotionStabilizationInput
        {
            public Velocity InputVelocity;
            public float InverseInertiaScale;

            public static readonly MotionStabilizationInput Default = new MotionStabilizationInput
            {
                InputVelocity = Velocity.Zero,
                InverseInertiaScale = 1.0f
            };
        }

        internal struct StepInput
        {
            /// <summary> Vector acceleration due to gravity in m/s^2 </summary>
            public float3 Gravity;
            /// <summary> TimeStep in seconds. </summary>
            public float Timestep;
            /// <summary> Enables gyroscopic torque, which will be added to dynamic bodies in every simulation step. </summary>
            public bool EnableGyroscopicTorque;
            /// <summary> The total number of substeps per frame. </summary>
            public int NumSubsteps;
            /// <summary> The total number of solver iterations per substep. </summary>
            public int NumSolverIterations;
            /// <summary> The direct solver settings. </summary>
            public DirectSolverSettings DirectSolverSettings;
            /// <summary> The current substep. Is -1 when uninitialized. </summary>
            public int CurrentSubstep;
            /// <summary> The current solver iteration. Is -1 when uninitialized. </summary>
            public int CurrentSolverIteration;
            /// <summary> Disable exporting events (TriggerEvents, CollisionEvents or ImpulseEvents). Default is false. </summary>
            public bool DisableExportEvents;

            public bool IsLastSubstep => CurrentSubstep == NumSubsteps - 1;
            public bool IsFirstSolverIteration => CurrentSolverIteration == 0;
            public bool IsLastSolverIteration => CurrentSolverIteration == NumSolverIterations - 1;

            // Note: events are only exported in the last iteration of the last sub-step, unless events export is disabled.
            public bool ExportEventsInThisIteration => IsLastSubstep && IsLastSolverIteration && !DisableExportEvents;

            // Note: events are only exported in the last sub-step, unless events export is disabled.
            public bool ExportEventsInThisSubstep => IsLastSubstep && !DisableExportEvents;
        }

        #region BuildJacobians

        // Schedule jobs to build Jacobians from the contacts stored in the simulation context for the first substep
        // where timeStep = frame timestep / numSubsteps
        internal static SimulationJobHandles ScheduleBuildJacobiansJobs(in SimulationStepInput input, JobHandle inputDeps,
            ref NativeList<DispatchPairSequencer.DispatchPair> dispatchPairs,
            ref DispatchPairSequencer.SolverSchedulerInfo solverSchedulerInfo,
            ref NativeStream contacts, ref NativeStream jacobians, bool multiThreaded = true)
        {
            SimulationJobHandles returnHandles = default;

            if (!multiThreaded)
            {
                var buildJob = new BuildJacobiansJob
                {
                    Input = input,
                    ContactsReader = contacts.AsReader(),
                    JacobiansWriter = jacobians.AsWriter(),
                    DispatchPairs = dispatchPairs.AsDeferredJobArray(),
                    SolverSchedulerInfo = solverSchedulerInfo
                }.Schedule(inputDeps);

                buildJob = new BuildDirectSolverJacobiansMapJob
                {
                    JacobiansReader = jacobians.AsReader(),
                    SolverSchedulerInfo = solverSchedulerInfo,
                    PhasedDispatchPairs = dispatchPairs.AsDeferredJobArray()
                }.Schedule(buildJob);

                returnHandles.FinalExecutionHandle = buildJob;
            }
            else
            {
                var gravityMagnitude = math.length(input.Gravity);

                var iterativeJacobiansHandle = new ParallelBuildJacobiansJob
                {
                    Input = input,
                    GravityMagnitude = gravityMagnitude,
                    ContactsReader = contacts.AsReader(),
                    JacobiansWriter = jacobians.AsWriter(),
                    DispatchPairs = dispatchPairs.AsDeferredJobArray(),
                    IterativeSolverSchedulerInfo = solverSchedulerInfo.IterativePairsIterativeScheduling
                }.ScheduleUnsafeIndex0(solverSchedulerInfo.IterativePairsIterativeScheduling.NumWorkItems, 1, inputDeps);

                var couplingJacobiansHandle = new ParallelBuildJacobiansJob
                {
                    Input = input,
                    GravityMagnitude = gravityMagnitude,
                    ContactsReader = contacts.AsReader(),
                    JacobiansWriter = jacobians.AsWriter(),
                    DispatchPairs = dispatchPairs.AsDeferredJobArray(),
                    IterativeSolverSchedulerInfo = solverSchedulerInfo.CouplingPairsIterativeScheduling
                }.ScheduleUnsafeIndex0(solverSchedulerInfo.CouplingPairsIterativeScheduling.NumWorkItems, 1, inputDeps);

                var directJacobiansHandle = new ParallelBuildJacobiansJob
                {
                    Input = input,
                    GravityMagnitude = gravityMagnitude,
                    ContactsReader = contacts.AsReader(),
                    JacobiansWriter = jacobians.AsWriter(),
                    DispatchPairs = dispatchPairs.AsDeferredJobArray(),
                    IterativeSolverSchedulerInfo = solverSchedulerInfo.DirectPairsIterativeScheduling
                }.ScheduleUnsafeIndex0(solverSchedulerInfo.DirectPairsIterativeScheduling.NumWorkItems, 1, inputDeps);

                directJacobiansHandle = new ParallelBuildDirectSolverJacobiansMapJob
                {
                    JacobiansReader = jacobians.AsReader(),
                    SolverSchedulerInfo = solverSchedulerInfo,
                    PhasedDispatchPairs = dispatchPairs.AsDeferredJobArray()
                }.ScheduleUnsafeIndex0(solverSchedulerInfo.DirectPairsIterativeScheduling.NumWorkItems, 1, directJacobiansHandle);

                returnHandles.FinalExecutionHandle = JobHandle.CombineDependencies(iterativeJacobiansHandle, couplingJacobiansHandle, directJacobiansHandle);
            }

            returnHandles.FinalDisposeHandle = JobHandle.CombineDependencies(
                dispatchPairs.Dispose(returnHandles.FinalExecutionHandle),
                contacts.Dispose(returnHandles.FinalExecutionHandle));

            return returnHandles;
        }

        [BurstCompile]
        struct BuildJacobiansJob : IJob
        {
            [ReadOnly] public SimulationStepInput Input;

            public NativeStream.Reader ContactsReader;
            public NativeStream.Writer JacobiansWriter;
            public NativeArray<DispatchPairSequencer.DispatchPair> DispatchPairs;

            [ReadOnly] public DispatchPairSequencer.SolverSchedulerInfo SolverSchedulerInfo;

            public void Execute()
            {
                BuildJacobians(Input, DispatchPairs, ref SolverSchedulerInfo, ref ContactsReader, ref JacobiansWriter);
            }
        }

        [BurstCompile]
        struct ParallelBuildJacobiansJob : IJobParallelForDefer
        {
            [ReadOnly]
            public SimulationStepInput Input;
            public float GravityMagnitude;

            public NativeStream.Reader ContactsReader;

            [NativeDisableContainerSafetyRestriction]
            public NativeStream.Writer JacobiansWriter;

            [NativeDisableContainerSafetyRestriction]
            public NativeArray<DispatchPairSequencer.DispatchPair> DispatchPairs;

            [ReadOnly]
            public DispatchPairSequencer.IterativeSolverSchedulerInfo IterativeSolverSchedulerInfo;

            public void Execute(int workItemIndexOffset)
            {
                var firstDispatchPairIndex = IterativeSolverSchedulerInfo.FirstDispatchPairIndex.Value + IterativeSolverSchedulerInfo.GetWorkItemReadOffset(workItemIndexOffset, out int dispatchPairCount);
                var workItemIndex = IterativeSolverSchedulerInfo.FirstWorkItemIndex.Value + workItemIndexOffset;

                BuildJacobians(Input.World, Input.SubstepTimeStep, GravityMagnitude, Input.NumSubsteps, Input.NumSolverIterations,
                    Input.MaxDynamicDepenetrationVelocity, Input.MaxStaticDepenetrationVelocity, Input.DirectSolverSettings,
                    workItemIndex, DispatchPairs, firstDispatchPairIndex, dispatchPairCount, ref ContactsReader, ref JacobiansWriter);
            }
        }

        /// <summary>
        /// Build Jacobians from the contacts and joints stored in the simulation context.
        /// </summary>
        internal static void BuildJacobians(in SimulationStepInput input,
            NativeArray<DispatchPairSequencer.DispatchPair> dispatchPairs, ref DispatchPairSequencer.SolverSchedulerInfo solverSchedulerInfo,
            ref NativeStream.Reader contactsReader, ref NativeStream.Writer jacobiansWriter)
        {
            var timeStep = input.SubstepTimeStep; // Note: need to use substep timestep for each sub-solve and for building/updating the required Jacobians
            var gravityMagnitude = math.length(input.Gravity);
            var numSubsteps = input.NumSubsteps;
            var numSolverIterations = input.NumSolverIterations;
            var maxDynamicDepenetrationVelocity = input.MaxDynamicDepenetrationVelocity;
            var maxStaticDepenetrationVelocity = input.MaxStaticDepenetrationVelocity;

            var workItemIndex = 0;

            // pure iterative pairs:
            if (solverSchedulerInfo.IterativePairsIterativeScheduling.NumDispatchPairs.Value > 0)
            {
                BuildJacobians(input.World, timeStep, gravityMagnitude, numSubsteps, numSolverIterations,
                    maxDynamicDepenetrationVelocity, maxStaticDepenetrationVelocity, input.DirectSolverSettings,
                    workItemIndex++, dispatchPairs,
                    solverSchedulerInfo.IterativePairsIterativeScheduling.FirstDispatchPairIndex.Value,
                    solverSchedulerInfo.IterativePairsIterativeScheduling.NumDispatchPairs.Value, ref contactsReader, ref jacobiansWriter);
            }

            // iterative coupling pairs:
            if (solverSchedulerInfo.CouplingPairsIterativeScheduling.NumDispatchPairs.Value > 0)
            {
                BuildJacobians(input.World, timeStep, gravityMagnitude, numSubsteps, numSolverIterations,
                    maxDynamicDepenetrationVelocity, maxStaticDepenetrationVelocity, input.DirectSolverSettings,
                    workItemIndex++, dispatchPairs,
                    solverSchedulerInfo.CouplingPairsIterativeScheduling.FirstDispatchPairIndex.Value,
                    solverSchedulerInfo.CouplingPairsIterativeScheduling.NumDispatchPairs.Value, ref contactsReader, ref jacobiansWriter);
            }

            // direct solver pairs:
            if (solverSchedulerInfo.DirectPairsIterativeScheduling.NumDispatchPairs.Value > 0)
            {
                BuildJacobians(input.World, timeStep, gravityMagnitude, numSubsteps, numSolverIterations,
                    maxDynamicDepenetrationVelocity, maxStaticDepenetrationVelocity, input.DirectSolverSettings,
                    workItemIndex, dispatchPairs,
                    solverSchedulerInfo.DirectPairsIterativeScheduling.FirstDispatchPairIndex.Value,
                    solverSchedulerInfo.DirectPairsIterativeScheduling.NumDispatchPairs.Value, ref contactsReader, ref jacobiansWriter);
            }
        }

        static unsafe void BuildJacobians(
            in PhysicsWorld world,
            float timestep, // the substep timestep
            float gravityMagnitude,
            int numSubsteps,
            int numSolverIterations,
            float maxDynamicDepenetrationVelocity,
            float maxStaticDepenetrationVelocity,
            in DirectSolverSettings directSolverSettings,
            int workItemIndex,
            NativeArray<DispatchPairSequencer.DispatchPair> dispatchPairs,
            int firstDispatchPairIndex,
            int dispatchPairCount,
            ref NativeStream.Reader contactReader,
            ref NativeStream.Writer jacobianWriter)
        {
            contactReader.BeginForEachIndex(workItemIndex);
            jacobianWriter.BeginForEachIndex(workItemIndex);

            var invTimestep = CalculateInvTimestep(timestep);

            // fall back to defaults if depenetration velocities not set or invalid.
            maxDynamicDepenetrationVelocity = math.select(maxDynamicDepenetrationVelocity,
                SimulationStepInput.DefaultMaxDynamicDepenetrationVelocity, maxDynamicDepenetrationVelocity <= 0);
            maxStaticDepenetrationVelocity = math.select(maxStaticDepenetrationVelocity,
                SimulationStepInput.DefaultMaxStaticDepenetrationVelocity, maxStaticDepenetrationVelocity <= 0);

            // Source: Narrowphase uses predicted velocity to generate dispatchPairs and contactReader data
            for (int i = 0; i < dispatchPairCount; i++)
            {
                var dispatchPairIndex = i + firstDispatchPairIndex;
                var pair = dispatchPairs[dispatchPairIndex];
                if (!pair.IsValid)
                {
                    continue;
                }

                var motionDatas = world.MotionDatas; // Motion data has gravity applied
                var motionVelocities = world.MotionVelocities;
                var bodies = world.Bodies;

                if (pair.IsContact)
                {
                    bool pairHasContacts = false;

                    // At some point during this frame, there MAY be a contact between these bodies
                    while (contactReader.RemainingItemCount > 0)
                    {
                        // Check if this is the matching contact
                        {
                            var header = contactReader.Peek<ContactHeader>();
                            if (pair.BodyIndexA != header.BodyPair.BodyIndexA ||
                                pair.BodyIndexB != header.BodyPair.BodyIndexB)
                            {
                                break;
                            }
                        }

                        ref ContactHeader contactHeader = ref contactReader.Read<ContactHeader>();
                        GetMotions(contactHeader.BodyPair, ref motionDatas, ref motionVelocities,
                            out MotionVelocity velocityA, out MotionVelocity velocityB,
                            out MTransform worldFromA, out MTransform worldFromB);

                        float sumInvMass = velocityA.InverseMass + velocityB.InverseMass;

                        // Note that for the velocity of static bodies, velocity.IsKinematic also returns true since
                        // for static bodies we obtain a MotionVelocity.Zero in GetMotions above. So the check below
                        // is true for pairs of type kinematic / kinematic and kinematic / static.
                        // Note also that static / static pairs are not generated by the collision detection by design
                        // and thus don't appear here.
                        bool bothMotionsAreKinematic = velocityA.IsKinematic && velocityB.IsKinematic;

                        // Skip contact between infinite mass bodies which don't want to raise events. These cannot have any effect during solving.
                        // These should not normally appear, because the collision detector doesn't generate such contacts.
                        if (bothMotionsAreKinematic)
                        {
                            if ((contactHeader.JacobianFlags & (JacobianFlags.IsTrigger | JacobianFlags.EnableCollisionEvents)) == 0)
                            {
                                for (int j = 0; j < contactHeader.NumContacts; j++)
                                {
                                    contactReader.Read<ContactPoint>();
                                }
                                continue;
                            }
                        }
                        else
                        {
                            contactHeader.JacobianFlags |= JacobianFlags.IsContactDynamic;
                        }

                        JacobianType jacType = ((int)(contactHeader.JacobianFlags) & (int)(JacobianFlags.IsTrigger)) != 0 ?
                            JacobianType.Trigger : JacobianType.Contact;

                        JacobianFlags jacFlags = contactHeader.JacobianFlags;

                        if (jacType == JacobianType.Contact && pair.SolverType == SolverType.Direct)
                        {
                            // @todo direct solver: for now we need to set this flag so that the contact manifold is stored in the jacobian,
                            // for later access in the direct solver. Once we build the direct solver's Jacobian matrices in the BuildJacobians phase,
                            // we won't need to do this anymore.
                            jacFlags |= JacobianFlags.EnableCollisionEvents;
                        }

                        // Write size before every jacobian and allocate all necessary data for this jacobian
                        int jacobianSize = JacobianHeader.CalculateSize(jacType, jacFlags, contactHeader.NumContacts);
                        jacobianWriter.Write(jacobianSize);
                        byte* jacobianPtr = jacobianWriter.Allocate(jacobianSize);

#if DEVELOPMENT_BUILD
                        SafetyChecks.Check4ByteAlignmentAndThrow(jacobianPtr, nameof(jacobianPtr));
#endif
                        ref JacobianHeader jacobianHeader = ref UnsafeUtility.AsRef<JacobianHeader>(jacobianPtr);
                        jacobianHeader.BodyPair = contactHeader.BodyPair;
                        jacobianHeader.Type = jacType;
                        jacobianHeader.Flags = jacFlags;
                        jacobianHeader.ConstraintBlockInfo = new ConstraintBlockInfo { Index = 0, Length = 1};

                        var baseJac = new BaseContactJacobian
                        {
                            NumContacts = contactHeader.NumContacts,
                            Normal = contactHeader.Normal
                        };

                        // Body A must be dynamic
                        Assert.IsTrue(contactHeader.BodyPair.BodyIndexA < motionVelocities.Length);
                        bool isDynamicStaticPair = contactHeader.BodyPair.BodyIndexB >= motionVelocities.Length;

                        // If contact distance is negative, use an artificially reduced penetration depth to prevent the
                        // dynamic-dynamic contacts from depenetrating too quickly
                        float maxDepenetrationVelocity = math.select(maxDynamicDepenetrationVelocity, maxStaticDepenetrationVelocity, isDynamicStaticPair);

                        if (jacobianHeader.Type == JacobianType.Contact)
                        {
                            ref ContactJacobian contactJacobian = ref jacobianHeader.AccessBaseJacobian<ContactJacobian>();
                            contactJacobian.BaseJacobian = baseJac;
                            contactJacobian.CoefficientOfFriction = contactHeader.CoefficientOfFriction;
                            contactJacobian.CoefficientOfRestitution = contactHeader.CoefficientOfRestitution;
                            contactJacobian.SumImpulsesOverSubsteps = 0.0f;
                            contactJacobian.SumImpulsesOverSolverIterations = 0.0f;

                            // Write polygons from colliders if they are required for detailed static mesh collision detection.
                            WriteJacobianPolygonData(ref jacobianHeader, bodies, contactHeader, worldFromA, worldFromB, motionVelocities.Length);

                            // Initialize modifier data (in order from JacobianModifierFlags) before angular jacobians
                            InitModifierData(ref jacobianHeader, contactHeader.ColliderKeys, new EntityPair
                            {
                                EntityA = bodies[contactHeader.BodyPair.BodyIndexA].Entity,
                                EntityB = bodies[contactHeader.BodyPair.BodyIndexB].Entity
                            });

                            // Build ContactJacobian for each contact in the dispatch pair
                            var centerA = new float3(0.0f);
                            var centerB = new float3(0.0f);
                            int solveCount = 0; // tracks if a bounce has been solved for the current iteration. Used to update friction
                            for (int j = 0; j < contactHeader.NumContacts; j++)
                            {
                                ContactJacobian.BuildIndividualContactJacobians(j, contactJacobian.BaseJacobian.Normal,
                                    worldFromA, worldFromB, invTimestep, numSubsteps, velocityA, velocityB,
                                    sumInvMass, maxDepenetrationVelocity, ref jacobianHeader, ref centerA, ref centerB,
                                    ref contactReader);

                                if (contactJacobian.CoefficientOfRestitution > 0.0f)
                                {
                                    ref ContactJacAngAndVelToReachCp jacAngular = ref jacobianHeader.AccessAngularJacobian(j);

                                    float dv = ContactJacobian.CalculateRelativeVelocityAlongNormal(velocityA, velocityB, ref jacAngular,
                                        contactJacobian.BaseJacobian.Normal, out float relativeVelocity);

                                    bool applyRestitution = false;
                                    if (numSubsteps > 1)
                                    {
                                        // Determine if the contact is reached during this substep: if the distance
                                        // travelled by the end of this substep results in a penetration, then we need
                                        // to bounce now.
                                        float distanceTraveled = timestep * relativeVelocity;
                                        float newDistanceToCp = jacAngular.ContactDistance + distanceTraveled;
                                        if (newDistanceToCp < 0.0f)
                                        {
                                            applyRestitution = ContactJacobian.CalculateRestitution(timestep,
                                                gravityMagnitude, contactJacobian.CoefficientOfRestitution,
                                                ref jacAngular, relativeVelocity, jacAngular.ContactDistance, dv);
                                        }

                                        // If VelToReachCp was updated in CalculateRestution, update contact distance to 0.
                                        jacAngular.ContactDistance = applyRestitution ? 0.0f : newDistanceToCp;
                                        if (!applyRestitution) jacAngular.ApplyImpulse = false;
                                    }
                                    else
                                    {
                                        applyRestitution = ContactJacobian.CalculateRestitution(timestep,
                                            gravityMagnitude, contactJacobian.CoefficientOfRestitution,
                                            ref jacAngular, relativeVelocity, jacAngular.ContactDistance, dv);
                                    }

                                    if (applyRestitution) solveCount++;
                                }
                            }

                            contactJacobian.CenterA = centerA;
                            contactJacobian.CenterB = centerB;

                            // Build friction jacobians (skip friction between two infinite-mass objects)
                            if (!bothMotionsAreKinematic)
                            {
                                ContactJacobian.BuildFrictionJacobians(
                                    ref contactJacobian,
                                    ref centerA, ref centerB,
                                    worldFromA, worldFromB,
                                    velocityA, velocityB,
                                    sumInvMass);

                                // Reduce friction by 1/4 if there was restitution applied on any contact point
                                if (solveCount > 0)
                                {
                                    contactJacobian.Friction0.EffectiveMass *= 0.25f;
                                    contactJacobian.Friction1.EffectiveMass *= 0.25f;
                                    contactJacobian.AngularFriction.EffectiveMass *= 0.25f;
                                    contactJacobian.FrictionEffectiveMassOffDiag *= 0.25f;
                                }
                            }
                        }
                        // Much less data needed for triggers
                        else
                        {
                            ref TriggerJacobian triggerJacobian = ref jacobianHeader.AccessBaseJacobian<TriggerJacobian>();

                            triggerJacobian.BaseJacobian = baseJac;
                            triggerJacobian.ColliderKeys = contactHeader.ColliderKeys;
                            triggerJacobian.Entities = new EntityPair
                            {
                                EntityA = bodies[contactHeader.BodyPair.BodyIndexA].Entity,
                                EntityB = bodies[contactHeader.BodyPair.BodyIndexB].Entity
                            };

                            // Build normal jacobians
                            var centerA = new float3(0.0f);
                            var centerB = new float3(0.0f);
                            for (int j = 0; j < contactHeader.NumContacts; j++)
                            {
                                // Build the jacobian
                                ContactJacobian.BuildIndividualContactJacobians(
                                    j, triggerJacobian.BaseJacobian.Normal, worldFromA, worldFromB,
                                    invTimestep, numSubsteps, velocityA, velocityB, sumInvMass, maxDepenetrationVelocity,
                                    ref jacobianHeader, ref centerA, ref centerB, ref contactReader);
                            }
                        }

                        pairHasContacts = true;
                    }

                    if (!pairHasContacts)
                    {
                        // invalidate dispatch pair since it has no contacts and thus shouldn't be processed
                        dispatchPairs[dispatchPairIndex] = DispatchPairSequencer.DispatchPair.Invalid;
                    }
                }
                else
                {
                    Joint joint = world.Joints[pair.JointIndex];
                    // Need to fetch the real body indices from the joint, as the scheduler may have reordered them
                    int bodyIndexA = joint.BodyPair.BodyIndexA;
                    int bodyIndexB = joint.BodyPair.BodyIndexB;

                    GetMotion(world, bodyIndexA, out MotionVelocity velocityA, out MotionData motionA);
                    GetMotion(world, bodyIndexB, out MotionVelocity velocityB, out MotionData motionB);

                    BuildJointJacobian(ref joint, pair.SolverType, velocityA, velocityB, motionA, motionB, timestep,
                        numSolverIterations, directSolverSettings, ref jacobianWriter);
                }
            }

            contactReader.EndForEachIndex();
            jacobianWriter.EndForEachIndex();
        }

        private static unsafe void WriteJacobianPolygonData(ref JacobianHeader jacobianHeader, NativeArray<RigidBody> bodies, in ContactHeader contactHeader, in MTransform worldFromA, in MTransform worldFromB, int dynamicBodiesCount)
        {
            bool isBodyAStatic = contactHeader.BodyPair.BodyIndexA >= dynamicBodiesCount;
            bool isBodyBStatic = contactHeader.BodyPair.BodyIndexB >= dynamicBodiesCount;

            if (jacobianHeader.HasDetailedStaticMeshCollision && (isBodyAStatic || isBodyBStatic))
            {
                ref ContactJacobianPolygonData jacobianPolygonContact = ref jacobianHeader.AccessJacobianContactData();

                // setup conditional variables.
                RigidBody body = default;
                float3 centerOfMass = default; // The center of mass is based on the opposite collider. A over B, and B over A.
                ColliderKey colliderKey = default;

                if (isBodyAStatic)
                {
                    centerOfMass = worldFromB.Translation;
                    body = bodies[contactHeader.BodyPair.BodyIndexA];
                    colliderKey = contactHeader.ColliderKeys.ColliderKeyA;
                }
                else
                {
                    centerOfMass = worldFromA.Translation;
                    body = bodies[contactHeader.BodyPair.BodyIndexB];
                    colliderKey = contactHeader.ColliderKeys.ColliderKeyB;
                }

                // assigning conditional variables
                jacobianPolygonContact.CenterOfMass = centerOfMass;
                jacobianPolygonContact.Transform = new AffineTransform(body.WorldFromBody.pos, body.WorldFromBody.rot, body.Scale);

                Collider* collider = (Collider*)body.Collider.GetUnsafePtr();
                SafetyChecks.CheckAreEqualAndThrow(true, collider != null);
                jacobianPolygonContact.IsValid = collider->GetLeaf(colliderKey, out ChildCollider leafCollider);
                if (jacobianPolygonContact.IsValid)
                {
                    PolygonCollider* polygon = (PolygonCollider*)leafCollider.Collider;
                    jacobianPolygonContact.Vertex0 = polygon->Vertices[0];
                    jacobianPolygonContact.Vertex1 = polygon->Vertices[1];
                    jacobianPolygonContact.Vertex2 = polygon->Vertices[2];
                    jacobianPolygonContact.LeafTransform = new AffineTransform(leafCollider.TransformFromChild);
                    jacobianPolygonContact.IsBodyAStatic = isBodyAStatic;
                    jacobianPolygonContact.IsBodyBStatic = isBodyBStatic;
                }
            }
        }

        #endregion //BuildJacobians

        #region BuildIndividualJacobians

        internal static unsafe void BuildJointJacobian(ref Joint joint, SolverType solverTypeOverride,
            in MotionVelocity velocityA, in MotionVelocity velocityB, in MotionData motionA, in MotionData motionB,
            float timestep, int numSolverIterations, in DirectSolverSettings directSolverSettings, [NoAlias] ref NativeStream.Writer jacobianWriter)
        {
            var bodyAFromMotionA = new MTransform(motionA.BodyFromMotion);
            MTransform motionAFromJoint = Mul(Inverse(bodyAFromMotionA), joint.AFromJoint);

            var bodyBFromMotionB = new MTransform(motionB.BodyFromMotion);
            MTransform motionBFromJoint = Mul(Inverse(bodyBFromMotionB), joint.BFromJoint);

            ref var constraintBlock = ref joint.Constraints;
            fixed(void* ptr = &constraintBlock)
            {
                var constraintPtr = (Constraint*)ptr;

                // count effective number of constraints in block
                var numConstraints = 0;
                for (var i = 0; i < constraintBlock.Length; i++)
                {
                    numConstraints += math.select(0, 1, constraintPtr[i].Dimension > 0);
                }

                var constraintIndex = 0;
                for (var i = 0; i < constraintBlock.Length; i++)
                {
                    Constraint constraint = constraintPtr[i];
                    int constraintDimension = constraint.Dimension;
                    if (0 == constraintDimension)
                    {
                        // Unconstrained, so no need to create a header.
                        continue;
                    }

                    JacobianType jacType;
                    switch (constraint.Type)
                    {
                        case ConstraintType.Linear:
                            jacType = JacobianType.LinearLimit;
                            break;
                        case ConstraintType.Angular:
                            switch (constraintDimension)
                            {
                                case 1:
                                    jacType = JacobianType.AngularLimit1D;
                                    break;
                                case 2:
                                    jacType = JacobianType.AngularLimit2D;
                                    break;
                                case 3:
                                    jacType = JacobianType.AngularLimit3D;
                                    break;
                                default:
                                    SafetyChecks.ThrowNotImplementedException();
                                    return;
                            }

                            break;
                        case ConstraintType.RotationMotor:
                            jacType = JacobianType.RotationMotor;
                            break;
                        case ConstraintType.AngularVelocityMotor:
                            jacType = JacobianType.AngularVelocityMotor;
                            break;
                        case ConstraintType.PositionMotor:
                            jacType = JacobianType.PositionMotor;
                            break;
                        case ConstraintType.LinearVelocityMotor:
                            jacType = JacobianType.LinearVelocityMotor;
                            break;
                        default:
                            SafetyChecks.ThrowNotImplementedException();
                            return;
                    }

                    // Write size before every jacobian
                    JacobianFlags jacFlags = constraint.ShouldRaiseImpulseEvents ? JacobianFlags.EnableImpulseEvents : 0;
                    int jacobianSize = JacobianHeader.CalculateJointSize(jacType, jacFlags, solverTypeOverride);
                    jacobianWriter.Write(jacobianSize);

                    // Allocate all necessary data for this jacobian
                    byte* jacobianPtr = jacobianWriter.Allocate(jacobianSize);
#if DEVELOPMENT_BUILD
                    SafetyChecks.Check4ByteAlignmentAndThrow(jacobianPtr, nameof(jacobianPtr));
#endif
                    ref JacobianHeader header = ref UnsafeUtility.AsRef<JacobianHeader>(jacobianPtr);
                    header.BodyPair = joint.BodyPair;
                    header.Type = jacType;
                    header.Flags = jacFlags;
                    header.ConstraintBlockInfo = new ConstraintBlockInfo { Index = constraintIndex++, Length = numConstraints };

                    // Prepare joint regularization parameters:

                    JacobianUtilities.CalculateConstraintTauAndDamping(constraint.SpringFrequency, constraint.DampingRatio,
                        timestep, numSolverIterations, out float tau, out float damping);

                    if (solverTypeOverride == SolverType.Direct)
                    {
                        header.AccessDirectSolverRegularizationData() = JacobianUtilities.CalculateDirectSolverRegularizationData(
                            header, constraint, velocityA, velocityB, directSolverSettings, timestep);
                    }

                    // Build the Jacobian:
                    switch (constraint.Type)
                    {
                        case ConstraintType.Linear:
                            header.AccessBaseJacobian<LinearLimitJacobian>().Build(
                                motionAFromJoint, motionBFromJoint, velocityA, velocityB,
                                motionA, motionB, constraint, tau, damping);
                            break;
                        case ConstraintType.Angular:
                            switch (constraintDimension)
                            {
                                case 1:
                                    header.AccessBaseJacobian<AngularLimit1DJacobian>().Build(
                                        motionAFromJoint, motionBFromJoint,
                                        velocityA, velocityB, motionA, motionB, constraint, tau, damping);
                                    break;
                                case 2:
                                    header.AccessBaseJacobian<AngularLimit2DJacobian>().Build(
                                        motionAFromJoint, motionBFromJoint,
                                        velocityA, velocityB, motionA, motionB,
                                        constraint, tau, damping);
                                    break;
                                case 3:
                                    header.AccessBaseJacobian<AngularLimit3DJacobian>().Build(
                                        motionAFromJoint, motionBFromJoint,
                                        velocityA, velocityB, motionA, motionB, constraint, tau, damping);
                                    break;
                                default:
                                    SafetyChecks.ThrowNotImplementedException();
                                    return;
                            }

                            break;
                        case ConstraintType.RotationMotor:
                            header.AccessBaseJacobian<RotationMotorJacobian>().Build(
                                motionAFromJoint, motionBFromJoint,
                                motionA, motionB, constraint, tau, damping);
                            break;
                        case ConstraintType.AngularVelocityMotor:
                            header.AccessBaseJacobian<AngularVelocityMotorJacobian>().Build(
                                motionAFromJoint, motionBFromJoint,
                                motionA, motionB, constraint, tau, damping);
                            break;
                        case ConstraintType.PositionMotor:
                            header.AccessBaseJacobian<PositionMotorJacobian>().Build(
                                motionAFromJoint, motionBFromJoint,
                                motionA, motionB, constraint, tau, damping);
                            break;
                        case ConstraintType.LinearVelocityMotor:
                            header.AccessBaseJacobian<LinearVelocityMotorJacobian>().Build(
                                motionAFromJoint, motionBFromJoint,
                                motionA, motionB, constraint, tau, damping);
                            break;
                        default:
                            SafetyChecks.ThrowNotImplementedException();
                            return;
                    }

                    if ((jacFlags & JacobianFlags.EnableImpulseEvents) != 0)
                    {
                        ref ImpulseEventSolverData impulseEventData = ref header.AccessImpulseEventSolverData();
                        impulseEventData.AccumulatedImpulse = float3.zero;
                        impulseEventData.JointEntity = joint.Entity;
                        impulseEventData.MaxImpulse = math.abs(constraint.MaxImpulse);
                    }
                }
            }
        }

        #endregion //BuildIndividualJacobians

        #region UpdateJacobians

        internal static JobHandle ScheduleUpdateJacobiansJobs(ref PhysicsWorld physicsWorld,
            ref NativeStream jacobians, ref DispatchPairSequencer.SolverSchedulerInfo solverSchedulerInfo,
            StepInput stepInput, JobHandle inputDeps, bool multiThreaded = true)
        {
            var updateHandle = inputDeps;

            if (!multiThreaded)
            {
                updateHandle = new UpdateJacobiansJob
                {
                    JacobiansReader = jacobians.AsReader(),
                    MotionDatas = physicsWorld.MotionDatas,
                    MotionVelocities = physicsWorld.MotionVelocities,
                    Bodies = physicsWorld.Bodies,
                    stepInput = stepInput,
                }.Schedule(updateHandle);
            }
            else
            {
                updateHandle = new ParallelUpdateJacobiansJob
                {
                    JacobiansReader = jacobians.AsReader(),
                    MotionDatas = physicsWorld.MotionDatas,
                    MotionVelocities = physicsWorld.MotionVelocities,
                    Bodies = physicsWorld.Bodies,
                    stepInput = stepInput,
                }.ScheduleUnsafe(solverSchedulerInfo.NumIterativeWorkItems, 1, updateHandle);
            }

            return updateHandle;
        }

        [BurstCompile]
        [NoAlias]
        struct UpdateJacobiansJob : IJob
        {
            [NoAlias][ReadOnly] public NativeStream.Reader JacobiansReader;
            [NoAlias][ReadOnly] public NativeArray<MotionData> MotionDatas;
            [NoAlias][ReadOnly] public NativeArray<MotionVelocity> MotionVelocities;
            [NoAlias][ReadOnly] public NativeArray<RigidBody> Bodies;
            [ReadOnly] public StepInput stepInput;

            public void Execute()
            {
                for (int i = 0; i < JacobiansReader.ForEachCount; ++i)
                {
                    UpdateJacobians(workItemIndex: i, MotionDatas, MotionVelocities, Bodies, ref JacobiansReader, stepInput);
                }
            }
        }

        [BurstCompile]
        [NoAlias]
        struct ParallelUpdateJacobiansJob : IJobParallelForDefer
        {
            [NoAlias][ReadOnly] public NativeStream.Reader JacobiansReader;
            [NoAlias][ReadOnly] public NativeArray<MotionData> MotionDatas;
            [NoAlias][ReadOnly] public NativeArray<MotionVelocity> MotionVelocities;
            [NoAlias][ReadOnly] public NativeArray<RigidBody> Bodies;
            [ReadOnly] public StepInput stepInput;

            public void Execute(int workItemIndex)
            {
                UpdateJacobians(workItemIndex, MotionDatas, MotionVelocities, Bodies,
                    ref JacobiansReader, stepInput);
            }
        }

        internal static void UpdateJacobians(int workItemIndex,
            [NoAlias] NativeArray<MotionData> motionDatas,
            [NoAlias] NativeArray<MotionVelocity> motionVelocities,
            [NoAlias] NativeArray<RigidBody> bodies,
            [NoAlias] ref NativeStream.Reader jacobianReader,
            StepInput stepInput)
        {
            var jacIterator = new JacobianIterator(jacobianReader, workItemIndex);
            while (jacIterator.HasJacobiansLeft())
            {
                ref JacobianHeader header = ref jacIterator.ReadJacobianHeader();

                // Static-static pairs should have been filtered during broadphase overlap test
                Assert.IsTrue(header.BodyPair.BodyIndexA < motionDatas.Length || header.BodyPair.BodyIndexB < motionDatas.Length);

                // Update the jacobians
                switch (header.Type)
                {
                    case JacobianType.Trigger:
                        break;

                    case JacobianType.Contact:
                        GetMotions(header.BodyPair, ref motionDatas, ref motionVelocities,
                            out MotionVelocity velocityA, out MotionVelocity velocityB,
                            out MTransform worldFromA, out MTransform worldFromB);

                        header.UpdateContact(in velocityA, in velocityB, in worldFromA, in worldFromB, stepInput);
                        break;

                    default: // for everything else
                        // Get the motion pair
                        MotionData motionDataA = header.BodyPair.BodyIndexA < motionDatas.Length ?
                            motionDatas[header.BodyPair.BodyIndexA] :
                            new MotionData
                        {
                            WorldFromMotion = bodies[header.BodyPair.BodyIndexA].WorldFromBody,
                            BodyFromMotion = RigidTransform.identity
                        };

                        MotionData motionDataB = header.BodyPair.BodyIndexB < motionDatas.Length ?
                            motionDatas[header.BodyPair.BodyIndexB] : new MotionData
                        {
                            WorldFromMotion = bodies[header.BodyPair.BodyIndexB].WorldFromBody,
                            BodyFromMotion = RigidTransform.identity
                        };

                        header.UpdateJoints(in motionDataA, in motionDataB);
                        break;
                }
            }
        }

        #endregion //UpdateBuildJacobians

        #region SolveJacobians

        /// <summary>   Schedule jobs to solve the Jacobians stored in the simulation context. </summary>
        internal static SimulationJobHandles ScheduleSolveJacobiansJobs(
            in DynamicsWorld dynamicsWorld, StepInput stepInput,
            in NativeStream jacobians, in NativeStream collisionEvents, in NativeStream triggerEvents,
            in NativeStream impulseEvents, in DispatchPairSequencer.SolverSchedulerInfo solverSchedulerInfo,
            StabilizationData solverStabilizationData, JobHandle inputDeps, bool multiThreaded = true)
        {
            SimulationJobHandles returnHandles = default;

            if (!multiThreaded)
            {
                returnHandles.FinalExecutionHandle = new SolverJob
                {
                    CollisionEventsWriter = collisionEvents.AsWriter(),
                    JacobiansReader = jacobians.AsReader(),
                    StepInput = stepInput,
                    SolverSchedulerInfo = solverSchedulerInfo,
                    TriggerEventsWriter = triggerEvents.AsWriter(),
                    ImpulseEventsWriter = impulseEvents.AsWriter(),
                    MotionVelocities = dynamicsWorld.MotionVelocities,
                    MotionDatas = dynamicsWorld.MotionDatas,
                    SolverStabilizationData = solverStabilizationData,
                }.Schedule(inputDeps);
            }
            else
            {
                JobHandle handle = inputDeps;

                // early out if there are no work items to process
                bool needSolver = (solverSchedulerInfo.NumIterativeWorkItems.Value + solverSchedulerInfo.DirectPairsDirectScheduling.NumIslands) > 0;

                if (needSolver)
                {
                    bool needCoupling = solverSchedulerInfo.CouplingPairsIterativeScheduling.NumWorkItems[0] > 0;

                    if (!needCoupling)
                    {
                        // run iterative and direct solver independently:
                        var motionVelocities = dynamicsWorld.MotionVelocities;

                        // direct solver constraints:
#if DIRECT_SOLVER_SOLVE_ISLANDS_IN_PARALLEL
                        var directHandle = new ParallelDirectSolverJob
#else
                        var directHandle = new DirectSolverJob
#endif
                        {
                            DirectSolverSchedulerInfo = solverSchedulerInfo.DirectPairsDirectScheduling,
                            JacobiansReader = jacobians.AsReader(),
                            MotionVelocities = motionVelocities,
                            MotionDatas = dynamicsWorld.MotionDatas,
                            StepInput = stepInput,
                        }
#if DIRECT_SOLVER_SOLVE_ISLANDS_IN_PARALLEL
                        .Schedule(solverSchedulerInfo.DirectPairsDirectScheduling.NumIslands, 1, handle);
#else
                        .Schedule(handle);
#endif
                        float3 gravityNormalized = float3.zero;
                        if (solverStabilizationData.StabilizationHeuristicSettings.EnableSolverStabilization)
                        {
                            gravityNormalized = math.normalizesafe(solverStabilizationData.Gravity);

                            // @todo direct solver (DOTS-7639): assign directHandle as input dependencies for iterative
                            // solver since if solver stabilization is enabled, we can't run the direct solver in parallel
                            // to the iterative solver as the motion velocities that the direct solver is reading and writing
                            // are modified in place by the stabilization.
                            // The stabilization should only apply to motion velocities on iterative bodies. We can use
                            // the "direct solver body" flag to filter out the direct ones. For this we need to store
                            // the direct body flag in the world, which is fine.
                            handle = directHandle;
                        }

                        JobHandle exportDirectEventsHandle = default;
                        if (stepInput.ExportEventsInThisSubstep)
                        {
                            // Export events for joints and contacts processed by the direct solver:
                            exportDirectEventsHandle = new ParallelExportEventsJob
                            {
                                JacobiansReader = jacobians.AsReader(),
                                MotionVelocities = motionVelocities,
                                CollisionEventsWriter = collisionEvents.AsWriter(),
                                TriggerEventsWriter = triggerEvents.AsWriter(),
                                ImpulseEventsWriter = impulseEvents.AsWriter(),
                                IterativeSolverSchedulerInfo = solverSchedulerInfo.DirectPairsIterativeScheduling,
                                Timestep = stepInput.Timestep
                            }.Schedule(solverSchedulerInfo.DirectPairsIterativeScheduling.NumWorkItems[0], 1, directHandle);
                        }

                        var iterativeHandle = handle;
                        for (int iSolverIteration = 0; iSolverIteration < stepInput.NumSolverIterations; iSolverIteration++)
                        {
                            stepInput.CurrentSolverIteration = iSolverIteration;

                            // pure iterative constraints:
                            iterativeHandle = ScheduleIterativeSolverPhasedIteration(solverSchedulerInfo.IterativePairsIterativeScheduling,
                                jacobians, motionVelocities, stepInput, solverStabilizationData, collisionEvents, triggerEvents, impulseEvents,
                                iterativeHandle);

                            // stabilize velocities:
                            if (solverStabilizationData.StabilizationHeuristicSettings.EnableSolverStabilization)
                            {
                                iterativeHandle = new StabilizeVelocitiesJob
                                {
                                    MotionVelocities = motionVelocities,
                                    SolverStabilizationData = solverStabilizationData,
                                    GravityPerStep = solverStabilizationData.Gravity * stepInput.Timestep,
                                    GravityNormalized = gravityNormalized,
                                    IsFirstIteration = stepInput.IsFirstSolverIteration
                                }.Schedule(dynamicsWorld.NumMotions, 64, iterativeHandle);
                            }
                        }

                        handle = JobHandle.CombineDependencies(iterativeHandle, directHandle, exportDirectEventsHandle);
                    }
                    else
                    {
                        // iterative solver coupled with direct solver:

                        // Disable exporting events during the first phase since events are exported by the iterative solver
                        // in the last iteration during the post-solve.
                        // This includes events for constraints processed by the direct solver.
                        var oldDisableExportEvents = stepInput.DisableExportEvents;
                        stepInput.DisableExportEvents = true;

                        // Phase 1: Pre-Solve
                        // iterative solver only, on all constraints, to get a good initial guess for all constraints,
                        // including coupling and direct.

                        handle = ScheduleIterativeSolver(dynamicsWorld, solverSchedulerInfo,
                            jacobians, stepInput, solverStabilizationData, collisionEvents, triggerEvents, impulseEvents, handle);

                        // Phase 2: Solve
                        // direct solver on direct constraints, for accurate solution of direct constraints.
                        // Run after iterative solver has completed to avoid race conditions.

#if DIRECT_SOLVER_SOLVE_ISLANDS_IN_PARALLEL
                        handle = new ParallelDirectSolverJob
#else
                        handle = new DirectSolverJob
#endif
                        {
                            DirectSolverSchedulerInfo = solverSchedulerInfo.DirectPairsDirectScheduling,
                            JacobiansReader = jacobians.AsReader(),
                            MotionVelocities = dynamicsWorld.MotionVelocities,
                            MotionDatas = dynamicsWorld.MotionDatas,
                            StepInput = stepInput,
#if DIRECT_SOLVER_SOLVE_ISLANDS_IN_PARALLEL
                        }.Schedule(solverSchedulerInfo.DirectPairsDirectScheduling.NumIslands, 1, handle);
#else
                        }.Schedule(handle);
#endif

                        // Phase 3: Post-Solve
                        // Again, iterative solver on all constraints, to refine and combine solutions for direct and iterative constraints

                        // re-enable exporting events for this phase.
                        stepInput.DisableExportEvents = oldDisableExportEvents;

                        handle = ScheduleIterativeSolver(dynamicsWorld, solverSchedulerInfo,
                            jacobians, stepInput, solverStabilizationData, collisionEvents, triggerEvents, impulseEvents, handle);
                    }
                }

                returnHandles.FinalDisposeHandle = handle;
                returnHandles.FinalExecutionHandle = handle;
            }

            // Dispose processed data
            if (stepInput.IsLastSubstep)
            {
                returnHandles.FinalDisposeHandle = JobHandle.CombineDependencies(
                    jacobians.Dispose(returnHandles.FinalExecutionHandle),
                    solverSchedulerInfo.ScheduleDisposeJob(returnHandles.FinalExecutionHandle),
                    returnHandles.FinalDisposeHandle);
            }

            return returnHandles;
        }

        [BurstCompile]
        [NoAlias]
        struct SolverJob : IJob
        {
            public NativeArray<MotionVelocity> MotionVelocities;
            public NativeArray<MotionData> MotionDatas;

            public StabilizationData SolverStabilizationData;

            [NoAlias]
            public NativeStream.Reader JacobiansReader;

            [NativeDisableContainerSafetyRestriction]
            [NoAlias]
            public NativeStream.Writer CollisionEventsWriter;

            [NativeDisableContainerSafetyRestriction]
            [NoAlias]
            public NativeStream.Writer TriggerEventsWriter;

            [NativeDisableContainerSafetyRestriction]
            [NoAlias]
            public NativeStream.Writer ImpulseEventsWriter;

            public StepInput StepInput;
            public DispatchPairSequencer.SolverSchedulerInfo SolverSchedulerInfo;

            public void Execute()
            {
                SolveJacobians( SolverSchedulerInfo, JacobiansReader, ref MotionVelocities, MotionDatas, StepInput,
                    ref CollisionEventsWriter, ref TriggerEventsWriter, ref ImpulseEventsWriter, SolverStabilizationData);
            }
        }

        /// <summary>   Solve the Jacobians stored in the simulation context. </summary>
        internal static void SolveJacobians(in DispatchPairSequencer.SolverSchedulerInfo solverSchedulerInfo, in NativeStream.Reader jacobiansReader,
            ref NativeArray<MotionVelocity> motionVelocities, in NativeArray<MotionData> motionDatas, StepInput stepInput,
            ref NativeStream.Writer collisionEventsWriter, ref NativeStream.Writer triggerEventsWriter,
            ref NativeStream.Writer impulseEventsWriter, StabilizationData solverStabilizationData)
        {
#if DEBUG_LOG_SOLVER_INFO
            {
                int numIslands;
                if ((numIslands = solverSchedulerInfo.DirectPairsDirectScheduling.DispatchPairIslandInfoCounts.Length) > 0)
                {
                    Debug.Log($"Direct Solver: processing {numIslands} islands");
                }
            }
#endif

            // In the single-threaded solver, we expect at max 1 work item for the iterative solvers
            SafetyChecks.CheckInRangeAndThrow(solverSchedulerInfo.IterativePairsIterativeScheduling.NumWorkItems[0], new int2(0, 1), "NumWorkItems");
            SafetyChecks.CheckInRangeAndThrow(solverSchedulerInfo.CouplingPairsIterativeScheduling.NumWorkItems[0], new int2(0, 1), "NumWorkItems");
            SafetyChecks.CheckInRangeAndThrow(solverSchedulerInfo.DirectPairsIterativeScheduling.NumWorkItems[0], new int2(0, 1), "NumWorkItems");

            // Check if coupling between direct and iterative solver is needed.
            // If not, simply solve direct and iterative constraints separately, and only once.
            // Otherwise, use coupling approach.
            if (solverSchedulerInfo.CouplingPairsIterativeScheduling.NumDispatchPairs.Value == 0)
            {
                if (solverSchedulerInfo.IterativePairsIterativeScheduling.NumDispatchPairs.Value > 0)
                {
                    IterativeSolver(ref motionVelocities, jacobiansReader,
                        ref collisionEventsWriter, ref triggerEventsWriter, ref impulseEventsWriter,
                        solverSchedulerInfo.IterativePairsIterativeScheduling.FirstWorkItemIndex.Value,
                        stepInput, solverStabilizationData);
                }

                for (int islandIndex = 0; islandIndex < solverSchedulerInfo.DirectPairsDirectScheduling.DispatchPairIslandInfoCounts.Length; ++islandIndex)
                {
                    DirectSolver(solverSchedulerInfo.DirectPairsDirectScheduling, islandIndex, jacobiansReader, ref motionVelocities, motionDatas, stepInput);
                }

                if (stepInput.ExportEventsInThisSubstep)
                {
                    // Export events for all constraints processed by the direct solver:
                    var workItemIndex = solverSchedulerInfo.DirectPairsIterativeScheduling.FirstWorkItemIndex.Value;
                    var workItemIndexEnd = workItemIndex + solverSchedulerInfo.DirectPairsIterativeScheduling.NumWorkItems[0];
                    for (; workItemIndex < workItemIndexEnd; ++workItemIndex)
                    {
                        ExportEvents(workItemIndex, stepInput.Timestep, jacobiansReader, motionVelocities,
                            ref collisionEventsWriter, ref triggerEventsWriter, ref impulseEventsWriter);
                    }
                }
            }
            else // coupling between iterative and direct constraints required:
            {
#if DEBUG_LOG_SOLVER_INFO
                Debug.Log("Coupled Direct and Iterative Solver");
#endif

                // Phase 1: Pre-solve
                // all iterative constraints

                // Disable exporting events during the first phase since events are exported by the iterative solver
                // in the last iteration during the post-solve.
                // This includes events for constraints processed by the direct solver.
                var oldDisableEvents = stepInput.DisableExportEvents;
                stepInput.DisableExportEvents = true;

                IterativeSolver(solverSchedulerInfo, jacobiansReader, ref motionVelocities, stepInput,
                    ref collisionEventsWriter, ref triggerEventsWriter, ref impulseEventsWriter, solverStabilizationData);

                // Phase 2: Solve
                // direct solver constraints
                for (int islandIndex = 0; islandIndex < solverSchedulerInfo.DirectPairsDirectScheduling.DispatchPairIslandInfoCounts.Length; ++islandIndex)
                {
                    DirectSolver(solverSchedulerInfo.DirectPairsDirectScheduling, islandIndex, jacobiansReader, ref motionVelocities, motionDatas, stepInput);
                }

                // Phase 3: Post-solve
                // all iterative constraints again

                // re-enable events for this phase
                stepInput.DisableExportEvents = oldDisableEvents;

                IterativeSolver(solverSchedulerInfo, jacobiansReader, ref motionVelocities, stepInput,
                    ref collisionEventsWriter, ref triggerEventsWriter, ref impulseEventsWriter, solverStabilizationData);
            }
        }

        static void IterativeSolver(in DispatchPairSequencer.SolverSchedulerInfo solverSchedulerInfo,
            in NativeStream.Reader jacobiansReader, ref NativeArray<MotionVelocity> motionVelocities, StepInput stepInput,
            ref NativeStream.Writer collisionEventsWriter, ref NativeStream.Writer triggerEventsWriter,
            ref NativeStream.Writer impulseEventsWriter, StabilizationData solverStabilizationData)
        {
            for (int solverIterationId = 0; solverIterationId < stepInput.NumSolverIterations; solverIterationId++)
            {
                stepInput.CurrentSolverIteration = solverIterationId;

                // pure iterative constraints:
                if (solverSchedulerInfo.IterativePairsIterativeScheduling.NumDispatchPairs.Value > 0)
                {
                    IterativeSolverIteration(ref motionVelocities, jacobiansReader,
                        ref collisionEventsWriter, ref triggerEventsWriter, ref impulseEventsWriter,
                        solverSchedulerInfo.IterativePairsIterativeScheduling.FirstWorkItemIndex.Value,
                        stepInput, solverStabilizationData);
                }

                // direct constraints run with iterative solver:
                if (solverSchedulerInfo.DirectPairsIterativeScheduling.NumDispatchPairs.Value > 0)
                {
                    IterativeSolverIteration(ref motionVelocities, jacobiansReader,
                        ref collisionEventsWriter, ref triggerEventsWriter, ref impulseEventsWriter,
                        solverSchedulerInfo.DirectPairsIterativeScheduling.FirstWorkItemIndex.Value,
                        stepInput, solverStabilizationData);
                }

                // iterative coupling constraints:
                if (solverSchedulerInfo.CouplingPairsIterativeScheduling.NumDispatchPairs.Value > 0)
                {
                    IterativeSolverIteration(ref motionVelocities, jacobiansReader,
                        ref collisionEventsWriter, ref triggerEventsWriter, ref impulseEventsWriter,
                        solverSchedulerInfo.CouplingPairsIterativeScheduling.FirstWorkItemIndex.Value,
                        stepInput, solverStabilizationData);
                }

                if (solverStabilizationData.StabilizationHeuristicSettings.EnableSolverStabilization)
                {
                    StabilizeVelocities(motionVelocities, stepInput.IsFirstSolverIteration, stepInput.Timestep, solverStabilizationData);
                }
            }
        }

        /// <summary>
        /// Sequential impulse solver with constraint regularization for modeling visco-elastic constraint behavior using
        /// spring frequency and damping ratio as model parameters. For details of the regularization procedure see
        /// <see cref="JacobianUtilities.CalculateConstraintTauAndDamping"/>.
        /// </summary>
        static void IterativeSolver(ref NativeArray<MotionVelocity> motionVelocities, in NativeStream.Reader jacobiansReader,
            ref NativeStream.Writer collisionEventsWriter, ref NativeStream.Writer triggerEventsWriter, ref NativeStream.Writer impulseEventsWriter,
            int workItemIndex, StepInput stepInput, in StabilizationData solverStabilizationData)
        {
#if DEBUG_LOG_SOLVER_INFO
            Debug.Log("Sequential Impulse Solver");
#endif
            for (int solverIterationId = 0; solverIterationId < stepInput.NumSolverIterations; solverIterationId++)
            {
                stepInput.CurrentSolverIteration = solverIterationId;

                IterativeSolverIteration(ref motionVelocities, jacobiansReader,
                    ref collisionEventsWriter, ref triggerEventsWriter, ref impulseEventsWriter,
                    workItemIndex, stepInput, solverStabilizationData);

                if (solverStabilizationData.StabilizationHeuristicSettings.EnableSolverStabilization)
                {
                    StabilizeVelocities(motionVelocities, stepInput.IsFirstSolverIteration, stepInput.Timestep, solverStabilizationData);
                }
            }
        }

        [BurstCompile]
        [NoAlias]
        struct ParallelIterativeSolverJob : IJobParallelFor
        {
            [NativeDisableParallelForRestriction]
            public NativeArray<MotionVelocity> MotionVelocities;

            [NativeDisableParallelForRestriction]
            public StabilizationData SolverStabilizationData;

            [NoAlias]
            public NativeStream.Reader JacobiansReader;

            [NativeDisableContainerSafetyRestriction]
            [NoAlias]
            public NativeStream.Writer CollisionEventsWriter;

            [NativeDisableContainerSafetyRestriction]
            [NoAlias]
            public NativeStream.Writer TriggerEventsWriter;

            [NativeDisableContainerSafetyRestriction]
            [NoAlias]
            public NativeStream.Writer ImpulseEventsWriter;

            [ReadOnly]
            public DispatchPairSequencer.IterativeSolverSchedulerInfo IterativeSolverSchedulerInfo;

            public int PhaseIndex;
            public StepInput stepInput;

            public void Execute(int phaseWorkItemOffset)
            {
                int firstWorkItemIndex = IterativeSolverSchedulerInfo.FirstWorkItemIndex.Value + IterativeSolverSchedulerInfo.PhaseInfo[PhaseIndex].FirstWorkItemIndexOffset;
                int workItemIndex = firstWorkItemIndex + phaseWorkItemOffset;
                if (stepInput.ExportEventsInThisIteration)
                {
                    CollisionEventsWriter.PatchMinMaxRange(workItemIndex);
                    TriggerEventsWriter.PatchMinMaxRange(workItemIndex);
                    ImpulseEventsWriter.PatchMinMaxRange(workItemIndex);
                }

                IterativeSolverIteration(ref MotionVelocities, JacobiansReader,
                    ref CollisionEventsWriter, ref TriggerEventsWriter, ref ImpulseEventsWriter,
                    workItemIndex, stepInput, SolverStabilizationData);
            }
        }

        static JobHandle ScheduleIterativeSolver(in DynamicsWorld dynamicsWorld, in DispatchPairSequencer.SolverSchedulerInfo solverSchedulerInfo,
            in NativeStream jacobians, StepInput stepInput, in StabilizationData solverStabilizationData,
            in NativeStream collisionEvents, in NativeStream triggerEvents, in NativeStream impulseEvents, JobHandle inputDependencies)
        {
            float3 gravityNormalized = float3.zero;
            if (solverStabilizationData.StabilizationHeuristicSettings.EnableSolverStabilization)
            {
                gravityNormalized = math.normalizesafe(solverStabilizationData.Gravity);
            }

            var handle = inputDependencies;

            for (int iSolverIteration = 0; iSolverIteration < stepInput.NumSolverIterations; iSolverIteration++)
            {
                stepInput.CurrentSolverIteration = iSolverIteration;

                // pure iterative constraints:
                handle = ScheduleIterativeSolverPhasedIteration(solverSchedulerInfo.IterativePairsIterativeScheduling,
                    jacobians, dynamicsWorld.MotionVelocities, stepInput, solverStabilizationData, collisionEvents, triggerEvents, impulseEvents,
                    handle);

                // direct constraints run with iterative solver:
                handle = ScheduleIterativeSolverPhasedIteration(solverSchedulerInfo.DirectPairsIterativeScheduling,
                    jacobians, dynamicsWorld.MotionVelocities, stepInput, solverStabilizationData, collisionEvents, triggerEvents, impulseEvents,
                    handle);

                // iterative coupling constraints:
                handle = ScheduleIterativeSolverPhasedIteration(solverSchedulerInfo.CouplingPairsIterativeScheduling,
                    jacobians, dynamicsWorld.MotionVelocities, stepInput, solverStabilizationData, collisionEvents, triggerEvents, impulseEvents,
                    handle);

                // Stabilize velocities
                if (solverStabilizationData.StabilizationHeuristicSettings.EnableSolverStabilization)
                {
                    var stabilizeVelocitiesJob = new StabilizeVelocitiesJob
                    {
                        MotionVelocities = dynamicsWorld.MotionVelocities,
                        SolverStabilizationData = solverStabilizationData,
                        GravityPerStep = solverStabilizationData.Gravity * stepInput.Timestep, //job doesn't need gravity in stabilize struct
                        GravityNormalized = gravityNormalized,
                        IsFirstIteration = stepInput.IsFirstSolverIteration
                    };

                    handle = stabilizeVelocitiesJob.Schedule(dynamicsWorld.NumMotions, 64, handle);
                }
            }
            return handle;
        }

        static JobHandle ScheduleIterativeSolverPhasedIteration(in DispatchPairSequencer.IterativeSolverSchedulerInfo solverSchedulerInfo,
            in NativeStream jacobians, in NativeArray<MotionVelocity> motionVelocities, in StepInput stepInput, in StabilizationData solverStabilizationData,
            in NativeStream collisionEvents, in NativeStream triggerEvents, in NativeStream impulseEvents, JobHandle inputDependencies)
        {
            var numPhases = solverSchedulerInfo.NumActivePhases[0];
            var solverHandle = inputDependencies;
            for (int phaseId = 0; phaseId < numPhases; phaseId++)
            {
                var job = new ParallelIterativeSolverJob
                {
                    JacobiansReader = jacobians.AsReader(),
                    PhaseIndex = phaseId,
                    IterativeSolverSchedulerInfo = solverSchedulerInfo,
                    MotionVelocities = motionVelocities,
                    SolverStabilizationData = solverStabilizationData,
                    stepInput = stepInput
                };

                // Only initialize event writers if required
                if (stepInput.ExportEventsInThisIteration)
                {
                    job.CollisionEventsWriter = collisionEvents.AsWriter();
                    job.TriggerEventsWriter = triggerEvents.AsWriter();
                    job.ImpulseEventsWriter = impulseEvents.AsWriter();
                }

                var phaseInfo = solverSchedulerInfo.PhaseInfo[phaseId];

                // Note: If we have duplicate body indices across work items in this phase we need to process the phase
                // sequentially to prevent data races. In this case, we choose a large batch size (batch equal to number of work items)
                // to prevent any parallelization of the work.
                int batchSize = phaseInfo.ContainsDuplicateIndices ? phaseInfo.NumWorkItems : 1;
                solverHandle = job.Schedule(phaseInfo.NumWorkItems, batchSize, solverHandle);
            }

            return solverHandle;
        }

        static void IterativeSolverIteration(
            ref NativeArray<MotionVelocity> motionVelocities,
            [NoAlias] in NativeStream.Reader jacobianReader,
            [NoAlias] ref NativeStream.Writer collisionEventsWriter,
            [NoAlias] ref NativeStream.Writer triggerEventsWriter,
            [NoAlias] ref NativeStream.Writer impulseEventsWriter,
            int workItemIndex,
            in StepInput stepInput,
            in StabilizationData solverStabilizationData)
        {
            if (stepInput.ExportEventsInThisIteration)
            {
                collisionEventsWriter.BeginForEachIndex(workItemIndex);
                triggerEventsWriter.BeginForEachIndex(workItemIndex);
                impulseEventsWriter.BeginForEachIndex(workItemIndex);
            }

            MotionStabilizationInput motionStabilizationSolverInputA = MotionStabilizationInput.Default;
            MotionStabilizationInput motionStabilizationSolverInputB = MotionStabilizationInput.Default;

            var jacIterator = new JacobianIterator(jacobianReader, workItemIndex);
            while (jacIterator.HasJacobiansLeft())
            {
                ref JacobianHeader header = ref jacIterator.ReadJacobianHeader();

                // Static-static pairs should have been filtered during broadphase overlap test
                SafetyChecks.CheckAreEqualAndThrow(true, header.BodyPair.BodyIndexA < motionVelocities.Length || header.BodyPair.BodyIndexB < motionVelocities.Length);

                // Get the motion pair
                MotionVelocity velocityA = header.BodyPair.BodyIndexA < motionVelocities.Length ?
                    motionVelocities[header.BodyPair.BodyIndexA] : MotionVelocity.Zero;
                MotionVelocity velocityB = header.BodyPair.BodyIndexB < motionVelocities.Length ?
                    motionVelocities[header.BodyPair.BodyIndexB] : MotionVelocity.Zero;

                // For Contacts Only: Populate the Solver stabilization data
                if (solverStabilizationData.StabilizationHeuristicSettings.EnableSolverStabilization
                    && header.Type == JacobianType.Contact)
                {
                    SolverStabilizationUpdate(ref header, stepInput.IsFirstSolverIteration, velocityA, velocityB,
                        solverStabilizationData, ref motionStabilizationSolverInputA, ref motionStabilizationSolverInputB);
                }

                // Solve the jacobian
                header.Solve(ref velocityA, ref velocityB, stepInput,
                    ref collisionEventsWriter, ref triggerEventsWriter, ref impulseEventsWriter,
                    solverStabilizationData.StabilizationHeuristicSettings.EnableSolverStabilization &&
                    solverStabilizationData.StabilizationHeuristicSettings.EnableFrictionVelocities,
                    motionStabilizationSolverInputA, motionStabilizationSolverInputB);

                // Write back velocity for dynamic bodies
                if (header.BodyPair.BodyIndexA < motionVelocities.Length)
                {
                    motionVelocities[header.BodyPair.BodyIndexA] = velocityA;
                }
                if (header.BodyPair.BodyIndexB < motionVelocities.Length)
                {
                    motionVelocities[header.BodyPair.BodyIndexB] = velocityB;
                }
            }

            if (stepInput.ExportEventsInThisIteration)
            {
                collisionEventsWriter.EndForEachIndex();
                triggerEventsWriter.EndForEachIndex();
                impulseEventsWriter.EndForEachIndex();
            }
        }

        #endregion // SolveJacobians

        #region Events

        [BurstCompile]
        struct ParallelExportEventsJob : IJobParallelFor
        {
            public NativeStream.Reader JacobiansReader;

            [NativeDisableContainerSafetyRestriction]
            public NativeArray<MotionVelocity> MotionVelocities;

            public NativeStream.Writer CollisionEventsWriter;
            public NativeStream.Writer TriggerEventsWriter;
            public NativeStream.Writer ImpulseEventsWriter;

            [ReadOnly]
            public DispatchPairSequencer.IterativeSolverSchedulerInfo IterativeSolverSchedulerInfo;

            public float Timestep;

            public void Execute(int workItemIndexOffset)
            {
                var workItemIndex = IterativeSolverSchedulerInfo.FirstWorkItemIndex.Value + workItemIndexOffset;

                CollisionEventsWriter.PatchMinMaxRange(workItemIndex);
                TriggerEventsWriter.PatchMinMaxRange(workItemIndex);
                ImpulseEventsWriter.PatchMinMaxRange(workItemIndex);

                ExportEvents(workItemIndex, Timestep, JacobiansReader, MotionVelocities,
                    ref CollisionEventsWriter, ref TriggerEventsWriter, ref ImpulseEventsWriter);
            }
        }

        static void ExportEvents(int workItemIndex, float timestep, in NativeStream.Reader jacobiansReader, in NativeArray<MotionVelocity> motionVelocities,
            ref NativeStream.Writer collisionEventsWriter, ref NativeStream.Writer triggerEventsWriter, ref NativeStream.Writer impulseEventsWriter)
        {
            collisionEventsWriter.BeginForEachIndex(workItemIndex);
            triggerEventsWriter.BeginForEachIndex(workItemIndex);
            impulseEventsWriter.BeginForEachIndex(workItemIndex);

            var iterator = new JacobianIterator(jacobiansReader, workItemIndex);
            while (iterator.HasJacobiansLeft())
            {
                ref var header = ref iterator.ReadJacobianHeader();

                switch (header.Type)
                {
                    case JacobianType.Contact:
                        if ((header.Flags & JacobianFlags.EnableCollisionEvents) != 0)
                        {
                            ref var contactJac = ref header.AccessBaseJacobian<ContactJacobian>();

                            if (!header.IsContactDynamic)
                            {
                                MotionVelocity velA = header.BodyPair.BodyIndexA < motionVelocities.Length ?
                                    motionVelocities[header.BodyPair.BodyIndexA] : MotionVelocity.Zero;
                                MotionVelocity velB = header.BodyPair.BodyIndexB < motionVelocities.Length ?
                                    motionVelocities[header.BodyPair.BodyIndexB] : MotionVelocity.Zero;

                                contactJac.ExportInfMassEvent(ref header, velA, velB, timestep, ref collisionEventsWriter);
                            }
                            else
                            {
                                bool forceCollisionEvent = false;
                                for (int i = 0; i < contactJac.BaseJacobian.NumContacts; ++i)
                                {
                                    ref var contactAngularJac = ref header.AccessAngularJacobian(i);

                                    // Force contact event even when no impulse is applied, but there is penetration.
                                    // This includes kinematic/kinematic or kinematic/static pairs.
                                    forceCollisionEvent |= contactAngularJac.VelToReachCp > 0.0f;
                                }

                                if (contactJac.SumImpulsesOverSubsteps > 0.0f || forceCollisionEvent)
                                {
                                    contactJac.ExportCollisionEvent(contactJac.SumImpulsesOverSubsteps, ref header, ref collisionEventsWriter);
                                }
                            }
                        }
                        break;

                    case JacobianType.Trigger:
                        MotionVelocity velocityA = header.BodyPair.BodyIndexA < motionVelocities.Length ?
                            motionVelocities[header.BodyPair.BodyIndexA] : MotionVelocity.Zero;
                        MotionVelocity velocityB = header.BodyPair.BodyIndexB < motionVelocities.Length ?
                            motionVelocities[header.BodyPair.BodyIndexB] : MotionVelocity.Zero;

                        ref var triggerJac = ref header.AccessBaseJacobian<TriggerJacobian>();
                        triggerJac.ExportEvent(ref header, velocityA, velocityB, ref triggerEventsWriter);

                        break;

                    case JacobianType.LinearLimit:
                    case JacobianType.AngularLimit1D:
                    case JacobianType.AngularLimit2D:
                    case JacobianType.AngularLimit3D:
                        if ((header.Flags & JacobianFlags.EnableImpulseEvents) != 0)
                        {
                            ref ImpulseEventSolverData impulseEventData = ref header.AccessImpulseEventSolverData();

                            if (math.any(math.abs(impulseEventData.AccumulatedImpulse) > impulseEventData.MaxImpulse))
                            {
                                impulseEventsWriter.Write(new ImpulseEventData
                                {
                                    Type = header.Type == JacobianType.LinearLimit ? ConstraintType.Linear : ConstraintType.Angular,
                                    Impulse = impulseEventData.AccumulatedImpulse,
                                    JointEntity = impulseEventData.JointEntity,
                                    BodyIndices = header.BodyPair
                                });
                            }
                        }
                        break;
                    default:
                        // Nothing to do for motor joints. These don't support events.
                        break;
                }
            }

            collisionEventsWriter.EndForEachIndex();
            triggerEventsWriter.EndForEachIndex();
            impulseEventsWriter.EndForEachIndex();
        }

        #endregion // Events

        #region UpdateInputVelocities

        /// <summary>
        /// Updates input velocities array with gravity integration for all dynamic bodies. The gravity integrated data
        /// is written to the linear velocity of inputVelocities. The AngularVelocity is copied without modification
        /// from the MotionVelocity data. The MotionVelocity data is not modified. MotionVelocities is not modified.
        /// </summary>
        internal static JobHandle ScheduleUpdateInputVelocitiesJob(NativeArray<MotionVelocity> motionVelocities,
            NativeArray<Velocity> inputVelocities,  float3 velocityFromGravity, JobHandle inputDeps, bool multiThreaded = true)
        {
            if (!multiThreaded)
            {
                var job = new UpdateInputVelocitiesJob
                {
                    MotionVelocities = motionVelocities,
                    InputVelocities = inputVelocities,
                    GravityVelocity = velocityFromGravity // Units: m/s. Input as: g * t
                };
                return job.Schedule(inputDeps);
            }
            else
            {
                var job = new ParallelUpdateInputVelocitiesJob
                {
                    MotionVelocities = motionVelocities,
                    InputVelocities = inputVelocities,
                    GravityVelocity = velocityFromGravity
                };
                return job.Schedule(motionVelocities.Length, 64, inputDeps);
            }
        }

        [BurstCompile]
        private struct UpdateInputVelocitiesJob : IJob
        {
            public NativeArray<MotionVelocity> MotionVelocities;
            public NativeArray<Velocity> InputVelocities;
            public float3 GravityVelocity;

            public void Execute()
            {
                UpdateInputVelocities(MotionVelocities, InputVelocities, GravityVelocity);
            }
        }

        /// Predict gravity for all dynamic bodies. The predicted motion due to gravity is written to
        /// inputVelocities.
        internal static void UpdateInputVelocities(NativeArray<MotionVelocity> motionVelocities,
            NativeArray<Velocity> inputVelocities, float3 gravityVelocity)
        {
            for (int i = 0; i < motionVelocities.Length; i++)
            {
                ParallelUpdateInputVelocitiesJob.ExecuteImpl(i, gravityVelocity, motionVelocities,
                    inputVelocities);
            }
        }

        [BurstCompile]
        private struct ParallelUpdateInputVelocitiesJob : IJobParallelFor
        {
            public NativeArray<MotionVelocity> MotionVelocities;
            public NativeArray<Velocity> InputVelocities;
            public float3 GravityVelocity;

            public void Execute(int i)
            {
                ExecuteImpl(i, GravityVelocity, MotionVelocities, InputVelocities);
            }

            internal static void ExecuteImpl(int i, float3 velocityFromGravity,
                NativeArray<MotionVelocity> motionVelocities, NativeArray<Velocity> inputVelocities)
            {
                MotionVelocity motionVelocity = motionVelocities[i];

                // Predict what linear velocity will be due to gravity at end of frame
                inputVelocities[i] = new Velocity
                {
                    Linear = motionVelocity.LinearVelocity + velocityFromGravity * motionVelocity.GravityFactor,
                    Angular = motionVelocity.AngularVelocity
                };
            }
        }

        #endregion // UpdateInputVelocities

        #region ApplyGravityAndUpdateInputVelocities

        /// Schedules jobs that apply gravity to all dynamic bodies and update InputVelocities. This method:
        /// 1) updates MotionVelocities with the gravity integrated linear velocity.
        /// 2) updates InputVelocities with the new MotionVelocities Linear and Angular data.
        internal static JobHandle ScheduleApplyGravityAndUpdateInputVelocitiesJob(ref DynamicsWorld world, NativeArray<Velocity> inputVelocities,
            float3 gravityAcceleration, float timestep, bool enableGyroscopicTorque, JobHandle inputDeps, bool multiThreaded = true)
        {
            if (!multiThreaded)
            {
                var job = new ApplyGravityAndUpdateInputVelocitiesJob
                {
                    MotionVelocities = world.MotionVelocities,
                    InputVelocities = inputVelocities,
                    GravityAcceleration = gravityAcceleration,
                    Timestep = timestep,
                    EnableGyroscopicTorque = enableGyroscopicTorque
                };
                return job.Schedule(inputDeps);
            }
            else
            {
                var job = new ParallelApplyGravityAndUpdateInputVelocitiesJob
                {
                    MotionVelocities = world.MotionVelocities,
                    InputVelocities = inputVelocities,
                    GravityAcceleration = gravityAcceleration,
                    Timestep = timestep,
                    EnableGyroscopicTorque = enableGyroscopicTorque
                };
                return job.Schedule(world.MotionVelocities.Length, 64, inputDeps);
            }
        }

        [BurstCompile]
        private struct ApplyGravityAndUpdateInputVelocitiesJob : IJob
        {
            public NativeArray<MotionVelocity> MotionVelocities;
            public NativeArray<Velocity> InputVelocities;
            public float3 GravityAcceleration;
            public float Timestep;
            public bool EnableGyroscopicTorque;

            public void Execute()
            {
                ApplyGravityAndUpdateInputVelocities(MotionVelocities, InputVelocities, GravityAcceleration, Timestep, EnableGyroscopicTorque);
            }
        }

        /// <summary>   Apply gravity to all dynamic bodies and update InputVelocities. This method:
        /// 1) updates MotionVelocities with the gravity integrated linear velocity.
        /// 2) updates InputVelocities with the new MotionVelocities Linear and Angular data.
        /// </summary>
        internal static void ApplyGravityAndUpdateInputVelocities(NativeArray<MotionVelocity> motionVelocities,
            NativeArray<Velocity> inputVelocities, float3 gravityAcceleration, float timestep, bool enableGyroscopicTorque)
        {
            for (int i = 0; i < motionVelocities.Length; i++)
            {
                ParallelApplyGravityAndUpdateInputVelocitiesJob.ExecuteImpl(i, motionVelocities, inputVelocities,
                    gravityAcceleration, timestep, enableGyroscopicTorque);
            }
        }

        [BurstCompile]
        private struct ParallelApplyGravityAndUpdateInputVelocitiesJob : IJobParallelFor
        {
            public NativeArray<MotionVelocity> MotionVelocities;
            public NativeArray<Velocity> InputVelocities;
            public float3 GravityAcceleration;
            public float Timestep;
            public bool EnableGyroscopicTorque;

            public void Execute(int i)
            {
                ExecuteImpl(i, MotionVelocities, InputVelocities, GravityAcceleration, Timestep, EnableGyroscopicTorque);
            }

            internal static void ExecuteImpl(int i, NativeArray<MotionVelocity> motionVelocities,
                NativeArray<Velocity> inputVelocities, float3 gravityAcceleration, float timestep, bool enableGyroscopicTorque)
            {
                MotionVelocity motionVelocity = motionVelocities[i];

                // Apply gravity
                motionVelocity.LinearVelocity += gravityAcceleration * motionVelocity.GravityFactor * timestep;

                if (enableGyroscopicTorque && !motionVelocity.IsKinematic)
                {
                    // Apply gyroscopic torque to generalized velocity.
                    ApplyGyroscopicTorque(ref motionVelocity, timestep);
                }

                // Write back
                motionVelocities[i] = motionVelocity;

                // Make a copy
                inputVelocities[i] = new Velocity
                {
                    Linear = motionVelocity.LinearVelocity,
                    Angular = motionVelocity.AngularVelocity
                };
            }
        }

        #endregion // ApplyGravityAndUpdateInputVelocities

        #region StabilizationHeuristics

        private static void StabilizeVelocities(NativeArray<MotionVelocity> motionVelocities,
            bool isFirstIteration, float timeStep, StabilizationData solverStabilizationData)
        {
            float3 gravityPerStep = solverStabilizationData.Gravity * timeStep;
            float3 gravityNormalized = math.normalizesafe(solverStabilizationData.Gravity);

            for (int i = 0; i < motionVelocities.Length; i++)
            {
                StabilizeVelocitiesJob.ExecuteImpl(i, motionVelocities, isFirstIteration,
                    gravityPerStep, gravityNormalized, solverStabilizationData);
            }
        }

        [BurstCompile]
        private struct StabilizeVelocitiesJob : IJobParallelFor
        {
            public NativeArray<MotionVelocity> MotionVelocities;
            public StabilizationData SolverStabilizationData;
            public float3 GravityPerStep;
            public float3 GravityNormalized;
            public bool IsFirstIteration;

            public void Execute(int i)
            {
                ExecuteImpl(i, MotionVelocities, IsFirstIteration, GravityPerStep, GravityNormalized, SolverStabilizationData);
            }

            internal static void ExecuteImpl(int i, NativeArray<MotionVelocity> motionVelocities,
                bool isFirstIteration, float3 gravityPerStep, float3 gravityNormalized,
                StabilizationData solverStabilizationData)
            {
                var motionData = solverStabilizationData.MotionData[i];
                int numPairs = motionData.NumPairs;
                if (numPairs == 0)
                {
                    return;
                }

                MotionVelocity motionVelocity = motionVelocities[i];

                // Skip kinematic bodies
                if (motionVelocity.InverseMass == 0.0f)
                {
                    return;
                }

                // Scale up inertia for other iterations
                if (isFirstIteration && numPairs > 1)
                {
                    float inertiaScale = 1.0f + 0.2f * (numPairs - 1) * solverStabilizationData.StabilizationHeuristicSettings.InertiaScalingFactor;
                    motionData.InverseInertiaScale = math.rcp(inertiaScale);
                    solverStabilizationData.MotionData[i] = motionData;
                }

                // Don't stabilize velocity component along the gravity vector
                float3 linVelVertical = math.dot(motionVelocity.LinearVelocity, gravityNormalized) * gravityNormalized;
                float3 linVelSideways = motionVelocity.LinearVelocity - linVelVertical;

                // Choose a very small gravity coefficient for clipping threshold
                float gravityCoefficient = (numPairs == 1 ? 0.1f : 0.25f) * solverStabilizationData.StabilizationHeuristicSettings.VelocityClippingFactor;

                // Linear velocity threshold
                float smallLinVelThresholdSq = math.lengthsq(gravityPerStep * motionVelocity.GravityFactor * gravityCoefficient);

                // Stabilize the velocities
                if (math.lengthsq(linVelSideways) < smallLinVelThresholdSq)
                {
                    motionVelocity.LinearVelocity = linVelVertical;

                    // Only clip angular if in contact with at least 2 bodies
                    if (numPairs > 1)
                    {
                        // Angular velocity threshold
                        if (motionVelocity.AngularExpansionFactor > 0.0f)
                        {
                            float angularFactorSq = math.rcp(motionVelocity.AngularExpansionFactor * motionVelocity.AngularExpansionFactor) * 0.01f;
                            float smallAngVelThresholdSq = smallLinVelThresholdSq * angularFactorSq;
                            if (math.lengthsq(motionVelocity.AngularVelocity) < smallAngVelThresholdSq)
                            {
                                motionVelocity.AngularVelocity = float3.zero;
                            }
                        }
                    }

                    // Write back
                    motionVelocities[i] = motionVelocity;
                }
            }
        }

        // Updates data for solver stabilization heuristic.
        // Updates number of pairs for dynamic bodies and resets inverse inertia scale in first iteration.
        // Also prepares motion stabilization solver data for current Jacobian to solve.
        private static void SolverStabilizationUpdate(
            ref JacobianHeader header, bool isFirstIteration,
            MotionVelocity velocityA, MotionVelocity velocityB,
            StabilizationData solverStabilizationData,
            ref MotionStabilizationInput motionStabilizationSolverInputA,
            ref MotionStabilizationInput motionStabilizationSolverInputB)
        {
            // Solver stabilization heuristic, count pairs and reset inverse inertia scale only in first iteration
            var inputVelocities = solverStabilizationData.InputVelocities;
            var motionData = solverStabilizationData.MotionData;
            if (isFirstIteration)
            {
                // Only count heavier (or up to 2 times lighter) bodies as pairs
                // Also reset inverse inertia scale
                if (header.BodyPair.BodyIndexA < motionData.Length)
                {
                    var data = motionData[header.BodyPair.BodyIndexA];
                    if (0.5f * velocityB.InverseMass <= velocityA.InverseMass)
                    {
                        data.NumPairs++;
                    }
                    data.InverseInertiaScale = 1.0f;
                    motionData[header.BodyPair.BodyIndexA] = data;
                }
                if (header.BodyPair.BodyIndexB < motionData.Length)
                {
                    var data = motionData[header.BodyPair.BodyIndexB];
                    if (0.5f * velocityA.InverseMass <= velocityB.InverseMass)
                    {
                        data.NumPairs++;
                    }
                    data.InverseInertiaScale = 1.0f;
                    motionData[header.BodyPair.BodyIndexB] = data;
                }
            }

            // Motion solver input stabilization data
            {
                if (solverStabilizationData.StabilizationHeuristicSettings.EnableFrictionVelocities)
                {
                    motionStabilizationSolverInputA.InputVelocity = header.BodyPair.BodyIndexA < inputVelocities.Length ?
                        inputVelocities[header.BodyPair.BodyIndexA] : Velocity.Zero;
                    motionStabilizationSolverInputB.InputVelocity = header.BodyPair.BodyIndexB < inputVelocities.Length ?
                        inputVelocities[header.BodyPair.BodyIndexB] : Velocity.Zero;
                }

                motionStabilizationSolverInputA.InverseInertiaScale = header.BodyPair.BodyIndexA < motionData.Length ?
                    motionData[header.BodyPair.BodyIndexA].InverseInertiaScale : 1.0f;
                motionStabilizationSolverInputB.InverseInertiaScale = header.BodyPair.BodyIndexB < motionData.Length ?
                    motionData[header.BodyPair.BodyIndexB].InverseInertiaScale : 1.0f;
            }
        }

        #endregion // StabilizationHeuristics

        #region Implementation

        private static void InitModifierData(ref JacobianHeader jacobianHeader, ColliderKeyPair colliderKeys, EntityPair entities)
        {
            if (jacobianHeader.HasContactManifold)
            {
                jacobianHeader.AccessColliderKeys() = colliderKeys;
                jacobianHeader.AccessEntities() = entities;
            }
            if (jacobianHeader.HasSurfaceVelocity)
            {
                jacobianHeader.AccessSurfaceVelocity() = new SurfaceVelocity();
            }
            if (jacobianHeader.HasMassFactors)
            {
                jacobianHeader.AccessMassFactors() = MassFactors.Default;
            }
        }

        private static void GetMotions(
            BodyIndexPair pair,
            ref NativeArray<MotionData> motionDatas,
            ref NativeArray<MotionVelocity> motionVelocities,
            out MotionVelocity velocityA,
            out MotionVelocity velocityB,
            out MTransform worldFromA,
            out MTransform worldFromB)
        {
            bool bodyAIsStatic = pair.BodyIndexA >= motionVelocities.Length;
            bool bodyBIsStatic = pair.BodyIndexB >= motionVelocities.Length;

            if (bodyAIsStatic)
            {
                if (bodyBIsStatic)
                {
                    Assert.IsTrue(false); // static-static pairs should have been filtered during broadphase overlap test
                    velocityA = MotionVelocity.Zero;
                    velocityB = MotionVelocity.Zero;
                    worldFromA = MTransform.Identity;
                    worldFromB = MTransform.Identity;
                    return;
                }

                velocityA = MotionVelocity.Zero;
                velocityB = motionVelocities[pair.BodyIndexB];

                worldFromA = MTransform.Identity;
                worldFromB = new MTransform(motionDatas[pair.BodyIndexB].WorldFromMotion);
            }
            else if (bodyBIsStatic)
            {
                velocityA = motionVelocities[pair.BodyIndexA];
                velocityB = MotionVelocity.Zero;

                worldFromA = new MTransform(motionDatas[pair.BodyIndexA].WorldFromMotion);
                worldFromB = MTransform.Identity;
            }
            else
            {
                velocityA = motionVelocities[pair.BodyIndexA];
                velocityB = motionVelocities[pair.BodyIndexB];

                worldFromA = new MTransform(motionDatas[pair.BodyIndexA].WorldFromMotion);
                worldFromB = new MTransform(motionDatas[pair.BodyIndexB].WorldFromMotion);
            }
        }

        // Gets a body's motion, even if the body is static
        private static void GetMotion([NoAlias] in PhysicsWorld world, int bodyIndex,
            [NoAlias] out MotionVelocity velocity, [NoAlias] out MotionData motion)
        {
            if (bodyIndex >= world.MotionVelocities.Length)
            {
                // Body is static
                velocity = MotionVelocity.Zero;
                motion = new MotionData
                {
                    WorldFromMotion = world.Bodies[bodyIndex].WorldFromBody,
                    BodyFromMotion = RigidTransform.identity
                        // remaining fields all zero
                };
            }
            else
            {
                // Body is dynamic
                velocity = world.MotionVelocities[bodyIndex];
                motion = world.MotionDatas[bodyIndex];
            }
        }

        #endregion // Implementation
    }
}
