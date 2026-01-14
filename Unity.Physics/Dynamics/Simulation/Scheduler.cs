//#define SERIAL_RADIX_SORT

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Unity.Burst;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Jobs;
using Unity.Jobs.LowLevel.Unsafe;
using Unity.Mathematics;
using UnityEngine;
using UnityEngine.Assertions;

namespace Unity.Physics
{
    /// <summary>
    /// Builds phased pairs of interacting bodies, used to parallelize work items during the
    /// simulation step.
    /// </summary>
    struct DispatchPairSequencer
    {
        internal const int kMaxNumPhases = 64;

        /// <summary>
        /// A pair of interacting bodies (either potentially colliding, or constrained together using a
        /// Joint). The joints and flags are compressed into a single 32 bit value as follows:
        ///
        ///      30        20        10
        ///    21098765432109876543210987654321
        /// 0b_11111111111111111111111111111111
        ///    ECD[     Joint:29 bits         ]
        ///
        ///
        /// This gives a limit of
        ///    2^32-1 / 2 Rigid bodies (/2 to remove negative values), with a collision pair supported for each body pair.
        ///    2^29-1 / 2 Joints (1 bit used for [E]nable Collision flag, 1 bit used for solver [C]oupling pair flag, 1 bit used for [D]irect Solver flag)
        ///
        /// Note: The [E]nable Collision flag must appear before any other bits, to ensure collisions will always have a higher
        /// hash value than joints, making them appear after joints in the sorted dispatch pair array, which is important for the
        /// joints' "Enable Collision" feature and for solving collisions last.
        ///
        /// We additionally choose indices so that BodyIndexA < BodyIndexB. This has subtle side-effects:
        /// * If one body in the pair is static, it will be body B.
        /// * Indices used for jointed pairs are not necessarily the same as selected in the joint
        /// * For some body A, all its static collisions will be contiguous.
        /// </summary>
        [DebuggerDisplay("{IsValid ? \"Valid\" : \"Invalid\"}, {IsContact ? \"Contact\" : \"Joint\"}, {SolverType}, Enable Collision: {IsCollisionEnabled}, [{BodyIndexA}, {BodyIndexB}]")]
        internal struct DispatchPair
            : IEquatable<DispatchPair>
        {
            int m_BodyIndexA;
            int m_BodyIndexB;
            uint m_Data;

            static readonly uint k_InvalidJointIndex = 0b_0001_1111_1111_1111_1111_1111_1111_1111; // 29 bits
            static readonly uint k_JointMask = ~k_InvalidJointIndex;
            static readonly uint k_EnableCouplingBit = 1u << math.countbits(k_InvalidJointIndex);
            static readonly uint k_DirectSolverBit = k_EnableCouplingBit << 1;
            static readonly uint k_EnableCollisionBit = k_DirectSolverBit << 1;

            /// <summary>   Indicates whether this pair is valid. </summary>
            ///
            /// <value> True if this pair is valid, false if not. </value>
            public bool IsValid => m_BodyIndexA != DispatchPair.Invalid.BodyIndexA || BodyIndexASubArraySortKey != DispatchPair.Invalid.BodyIndexASubArraySortKey;

            /// <summary>   Indicates whether this pair is a joint. </summary>
            ///
            /// <value> True if this pair is a joint, false if not. </value>
            public bool IsJoint => JointIndex != k_InvalidJointIndex;

            /// <summary>   Indicates whether this pair is a contact. </summary>
            ///
            /// <value> True if this pair is a contact, false if not. </value>
            public bool IsContact => !IsJoint;

            /// <summary>   Gets or sets the Solver type used for this pair. </summary>
            ///
            /// <value> The Solver type used for this pair. </value>
            public SolverType SolverType
            {
                get => (m_Data & k_DirectSolverBit) != 0 ? SolverType.Direct : SolverType.Iterative;
                set => m_Data = (m_Data & ~k_DirectSolverBit) | (value == SolverType.Direct ? k_DirectSolverBit : 0);
            }

            /// <summary>   Gets the coupling pair flag. </summary>
            ///
            /// <value> The coupling pair flag. </value>
            public bool IsCouplingEnabled => (m_Data & k_EnableCouplingBit) != 0;

            public void EnableCoupling()
            {
                m_Data |= k_EnableCouplingBit;
            }

            /// <summary>   Gets the invalid dispatch pair. </summary>
            ///
            /// <value> The invalid dispatch pair. </value>
            public static DispatchPair Invalid => new DispatchPair { m_BodyIndexA = ~0, m_BodyIndexB = ~0, m_Data = ~(uint)0 };

            /// <summary>   Gets or sets the body index a. </summary>
            ///
            /// <value> The body index a. </value>
            public int BodyIndexA
            {
                set => m_BodyIndexA = value;
                get => m_BodyIndexA;
            }

            /// <summary>   Gets the body index b. </summary>
            ///
            /// <value> The body index b. </value>
            public int BodyIndexB
            {
                set => m_BodyIndexB = value;
                get => m_BodyIndexB;
            }

            /// <summary>   Gets the zero-based index of the joint. </summary>
            ///
            /// <value> The joint index. </value>
            public int JointIndex
            {
                get => (int)(m_Data & k_InvalidJointIndex);
                internal set
                {
                    Assert.IsTrue(value < k_InvalidJointIndex);
                    m_Data = (m_Data & k_JointMask) | (uint)value;
                }
            }

            /// <summary>
            /// Gets a value indicating whether collision is enabled for this pair.
            /// If the pair is a joint, this indicates whether the joint allows collision between the bodies.
            /// If the pair is a collision, this indicates whether the collision is enabled.
            /// </summary>
            internal bool IsCollisionEnabled => (m_Data & k_EnableCollisionBit) != 0;

            /// <summary>
            /// Disables collision for this pair.
            /// </summary>
            internal void DisableCollision()
            {
                SafetyChecks.CheckAreEqualAndThrow(IsContact, true);
                m_Data &= ~k_EnableCollisionBit;
            }

            /// <summary>
            /// Sort key for sorting of DispatchPair sub-arrays with identical BodyIndexA.
            /// Packed version of BodyIndexB (leading) and rest of data.
            /// </summary>
            internal ulong BodyIndexASubArraySortKey => (ulong)m_BodyIndexB << 32 | m_Data;

            /// <summary>   Creates collision pair from a broad phase overlap result. </summary>
            ///
            /// <param name="overlapResult"> The overlap result. </param>
            ///
            /// <returns>   The new dispatch pair representing collision pair. </returns>
            public static DispatchPair CreateCollisionPair(Broadphase.OverlapResult overlapResult)
            {
                // Note: The "Enable Collision" flag is set to true deliberately, since initially, all contacts
                // are enabled. They might get disabled later during the pair sorting if none of the joints between the
                // corresponding body pair allows collision.
                // Furthermore, setting the "Enable Collision" and all bits related to the Joint (k_InvalidJointIndex)
                // ensures that joints always appear before contacts in the sorted dispatch pair array.
                // This serves two purposes:
                // 1) It guarantees that joints are always solved before the higher priority contacts in the iterative
                //    solver, yielding lower constraint error for contacts than for joints after every solver iteration.
                // 2) It is a requirement in the joints' "Enable Collision" feature, in which collisions between a given
                //    body pair are enabled if any of the joints between them allows it.
                return Create(overlapResult.BodyPair, (int)k_InvalidJointIndex, enableCollision: true, overlapResult.SolverType, couplingPair: false);
            }

            /// <summary>   Creates a joint. </summary>
            ///
            /// <param name="pair">             The body pair. </param>
            /// <param name="jointIndex">       Zero-based index of the joint. </param>
            /// <param name="enableCollision">  This joint enables collision between the given body pair. </param>
            /// <param name="solverType">       Solver type used for this joint. </param>
            ///
            /// <returns>   The new dispatch pair representing the joint. </returns>
            public static DispatchPair CreateJoint(BodyIndexPair pair, int jointIndex, bool enableCollision, SolverType solverType = SolverType.Iterative)
            {
                Assert.IsTrue(jointIndex < k_InvalidJointIndex);
                return Create(pair, jointIndex, enableCollision, solverType, couplingPair: false);
            }

            static DispatchPair Create(BodyIndexPair pair, int jointIndex, bool enableCollision, SolverType solverType, bool couplingPair)
            {
                int selectedA = math.min(pair.BodyIndexA, pair.BodyIndexB);
                int selectedB = math.max(pair.BodyIndexA, pair.BodyIndexB);
                return new DispatchPair
                {
                    BodyIndexA = selectedA,
                    BodyIndexB = selectedB,
                    m_Data =
                        (solverType == SolverType.Direct ? k_DirectSolverBit : 0) |
                        (couplingPair ? k_EnableCouplingBit : 0) |
                        (enableCollision ? k_EnableCollisionBit : 0) |
                        (uint)(jointIndex & k_InvalidJointIndex)
                };
            }

            public bool Equals(DispatchPair other)
            {
                return m_BodyIndexA == other.m_BodyIndexA && BodyIndexASubArraySortKey == other.BodyIndexASubArraySortKey;
            }

            public override int GetHashCode()
            {
                return HashCode.Combine(m_BodyIndexA.GetHashCode(), BodyIndexASubArraySortKey.GetHashCode());
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void SortDispatchPairs(NativeList<DispatchPair> dispatchPairs) => dispatchPairs.Sort(new DispatchPairComparer());

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void SortDispatchPairs(NativeArray<DispatchPair> dispatchPairs) => dispatchPairs.Sort(new DispatchPairComparer());

        [NoAlias]
        internal struct SolverSchedulerInfo : IDisposable
        {
            [NoAlias]
            public IterativeSolverSchedulerInfo IterativePairsIterativeScheduling;
            [NoAlias]
            public IterativeSolverSchedulerInfo CouplingPairsIterativeScheduling;
            [NoAlias]
            public IterativeSolverSchedulerInfo DirectPairsIterativeScheduling;
            [NoAlias]
            public DirectSolverSchedulerInfo DirectPairsDirectScheduling;

            [NoAlias]
            public NativeReference<int> NumIterativeWorkItems;

            public SolverSchedulerInfo(int numPhases, Allocator allocator = Allocator.TempJob)
            {
                IterativePairsIterativeScheduling = new IterativeSolverSchedulerInfo(numPhases, allocator);
                CouplingPairsIterativeScheduling = new IterativeSolverSchedulerInfo(numPhases, allocator);
                DirectPairsIterativeScheduling = new IterativeSolverSchedulerInfo(numPhases, allocator);
                DirectPairsDirectScheduling = DirectSolverSchedulerInfo.Create(allocator);
                NumIterativeWorkItems = new NativeReference<int>(0, allocator);
            }

            /// <summary>
            ///     Finalizes the iterative solver scheduler info.
            ///     <para/>
            ///     Sets <see cref="NumIterativeWorkItems">total iterative work item count</see> and
            ///     the <see cref="IterativeSolverSchedulerInfo.FirstWorkItemIndex">work item start
            ///     indices</see> for all iterative solvers.
            /// </summary>
            public void FinalizeIterativeWorkItemInfo()
            {
                var numWorkItems = 0;
                IterativePairsIterativeScheduling.FirstWorkItemIndex.Value = 0;
                numWorkItems += IterativePairsIterativeScheduling.NumWorkItems[0];
                CouplingPairsIterativeScheduling.FirstWorkItemIndex.Value = numWorkItems;
                numWorkItems += CouplingPairsIterativeScheduling.NumWorkItems[0];
                DirectPairsIterativeScheduling.FirstWorkItemIndex.Value = numWorkItems;

                NumIterativeWorkItems.Value = IterativePairsIterativeScheduling.NumWorkItems[0] +
                    CouplingPairsIterativeScheduling.NumWorkItems[0] +
                    DirectPairsIterativeScheduling.NumWorkItems[0];
            }

            public void Dispose()
            {
                IterativePairsIterativeScheduling.Dispose();
                CouplingPairsIterativeScheduling.Dispose();
                DirectPairsIterativeScheduling.Dispose();
                DirectPairsDirectScheduling.Dispose();
                NumIterativeWorkItems.Dispose();
            }

            public JobHandle ScheduleDisposeJob(JobHandle inputDeps)
            {
                return JobHandle.CombineDependencies(
                    IterativePairsIterativeScheduling.ScheduleDisposeJob(inputDeps),
                    CouplingPairsIterativeScheduling.ScheduleDisposeJob(inputDeps),
                    JobHandle.CombineDependencies(
                        DirectPairsIterativeScheduling.ScheduleDisposeJob(inputDeps),
                        DirectPairsDirectScheduling.ScheduleDisposeJob(inputDeps),
                        NumIterativeWorkItems.Dispose(inputDeps)));
            }
        }

        /// <summary>
        /// Island information for a dispatch pair in the direct solver
        /// </summary>
        [NoAlias]
        [DebuggerDisplay("DispatchPair Index: {UnphasedDispatchPairIndex}, Island Index {IslandIndex}")]
        internal struct DispatchPairIslandInfo
        {
            /// <summary> Index of the dispatch pair in the unphased (lexicographically sorted) array </summary>
            public int UnphasedDispatchPairIndex;
            /// <summary> Index of the island this dispatch pair belongs to </summary>
            public int IslandIndex;
        }

        /// <summary>
        /// Jacobian mapping information, mapping a dispatch pair in the direct solver to the
        /// location of its associated data in the Jacobian stream buffers.
        /// </summary>
        internal struct DispatchPairJacobianMapping
        {
            public UnsafeStream.ReaderState ReaderState;
            /// <summary> Work item index (buffer) where this dispatch pair's Jacobian data is located in the Jacobian stream </summary>
            public int WorkItemIndex;

            /// <summary> Indicates whether this mapping is valid </summary>
            public bool IsValid => WorkItemIndex >= 0;

            /// <summary> Invalid mapping, used for unmapped entries </summary>
            public static DispatchPairJacobianMapping Invalid => new DispatchPairJacobianMapping { WorkItemIndex = -1 };
        }

        [NoAlias]
        internal struct DirectSolverSchedulerInfo : IDisposable
        {
            /// <summary>
            /// Map from unphased direct pair index (used to identify pairs in the direct pair islands)
            /// to phased direct pair index (used to identify direct pairs in the phases of the iterative solver scheduling of
            /// the direct pairs. See <see cref="SolverSchedulerInfo.DirectPairsIterativeScheduling"/>).
            /// <para/> Note that the phased and unphased indices are both local to the direct pairs sub-array of the
            /// full dispatch pair array.
            /// <para/>
            /// With this map we can jump from the unphased dispatch pair indices with which the islands are
            /// organized (see <see cref="DispatchPairIslandInfo.UnphasedDispatchPairIndex"/>) to the (local) phased
            /// dispatch pair indices with which the direct pairs are organized in the global dispatch pair
            /// array (see <see cref="SolverSchedulerInfo.DirectPairsIterativeScheduling"/>).
            /// This map is among others used to allow locating the Jacobian data in the Jacobian stream buffers for
            /// a given unphased dispatch pair that is part of an island. The mapping from (local) phased dispatch pair
            /// index to said Jacobian data location is stored in <see cref="PhasedDispatchPairJacobianMappings"/> below
            /// and is produced in the <see cref="Solver.BuildDirectSolverJacobiansMapJob"/>.
            /// </summary>
            [NoAlias]
            public NativeList<int> UnphasedToPhasedDispatchPairMap;

            /// <summary>
            /// Mapping from the phased direct dispatch pair index (organized in phases
            /// in <see cref="SolverSchedulerInfo.DirectPairsIterativeScheduling"/>) to the Jacobian data location
            /// in the Jacobian stream buffers.
            /// </summary>
            [NoAlias]
            public NativeList<DispatchPairJacobianMapping> PhasedDispatchPairJacobianMappings;

            /// <summary>
            /// Island information for the direct pairs, grouped sequentially by islands, with
            /// "DispatchPairIslandInfoCounts[islandIndex]" many entries each, where islandIndex ranges from
            /// 0 to "DispatchPairIslandInfoCounts.Length - 1".
            /// </summary>
            [NoAlias]
            public NativeList<DispatchPairIslandInfo> DispatchPairIslandInfos;

            /// <summary> Indices into the DispatchPairIslandInfos array, indicating the first entry of each island. </summary>
            [NoAlias]
            public NativeList<int> FirstDispatchPairIslandInfoIndices;

            /// <summary> Number of dispatch pairs in each island, indexed by the island index. </summary>
            [NoAlias]
            public NativeList<int> DispatchPairIslandInfoCounts;

            /// <summary> Number of islands to be solved with the direct solver. </summary>
            public int NumIslands => DispatchPairIslandInfoCounts.Length;

            /// <summary>
            /// First direct dispatch pair index in the global dispatch pair array indicating the beginning of the
            /// direct dispatch pairs sub-array.
            /// </summary>
            [NoAlias]
            public NativeReference<int> FirstDispatchPairIndex;

            public static DirectSolverSchedulerInfo Create(Allocator allocator = Allocator.TempJob)
            {
                return new DirectSolverSchedulerInfo
                {
                    UnphasedToPhasedDispatchPairMap = new NativeList<int>(allocator),
                    PhasedDispatchPairJacobianMappings = new NativeList<DispatchPairJacobianMapping>(allocator),
                    DispatchPairIslandInfos = new NativeList<DispatchPairIslandInfo>(allocator),
                    FirstDispatchPairIslandInfoIndices = new NativeList<int>(128, allocator),
                    DispatchPairIslandInfoCounts = new NativeList<int>(128, allocator),
                    FirstDispatchPairIndex = new NativeReference<int>(0, allocator)
                };
            }

            public void ResizeMaps(int numDispatchPairs)
            {
                PhasedDispatchPairJacobianMappings.Clear();
                PhasedDispatchPairJacobianMappings.ResizeUninitialized(numDispatchPairs);

                UnphasedToPhasedDispatchPairMap.Clear();
                UnphasedToPhasedDispatchPairMap.ResizeUninitialized(numDispatchPairs);
            }

            public void Dispose()
            {
                UnphasedToPhasedDispatchPairMap.Dispose();
                PhasedDispatchPairJacobianMappings.Dispose();
                DispatchPairIslandInfos.Dispose();
                FirstDispatchPairIslandInfoIndices.Dispose();
                DispatchPairIslandInfoCounts.Dispose();
                FirstDispatchPairIndex.Dispose();
            }

            public JobHandle ScheduleDisposeJob(JobHandle inputDeps)
            {
                return JobHandle.CombineDependencies(UnphasedToPhasedDispatchPairMap.Dispose(inputDeps), PhasedDispatchPairJacobianMappings.Dispose(inputDeps),
                    JobHandle.CombineDependencies(DispatchPairIslandInfos.Dispose(inputDeps),
                        JobHandle.CombineDependencies(FirstDispatchPairIslandInfoIndices.Dispose(inputDeps),
                            DispatchPairIslandInfoCounts.Dispose(inputDeps), FirstDispatchPairIndex.Dispose(inputDeps))));
            }
        }

        /// <summary>
        /// A phased set of dispatch pairs used to schedule solver jobs and distribute work between them.
        /// </summary>
        [NoAlias]
        internal struct IterativeSolverSchedulerInfo : IDisposable
        {
            // A structure which describes the number of items in a single phase
            internal struct SolvePhaseInfo
            {
                internal int DispatchPairCount; // The total number of pairs in this phase
                internal int BatchSize; // The number of items per thread work item; at most, the number of pairs
                internal int NumWorkItems; // The amount of "subtasks" of size BatchSize in this phase
                internal int FirstWorkItemIndexOffset;  // Offset to first work item index of this phase. Calculated as
                                                        // the sum of NumWorkItems in all previous phases.
                                                        // Index for this work item in the global work item array is
                                                        // IterativeSolverSchedulerInfo.FirstWorkItemIndex + SolvePhaseInfo.FirstWorkItemIndexOffset.
                internal int FirstDispatchPairOffset;   // Offset to first dispatch pair in this phase within the sub-array
                                                        // of dispatch pairs for this scheduler info.
                                                        // Index for this pair in the global dispatch pair array is
                                                        // IterativeSolverSchedulerInfo.FirstDispatchPairIndex + SolvePhaseInfo.FirstDispatchPairOffset.
                internal bool ContainsDuplicateIndices; // Indicates that there are duplicate body indices across batches within this phase.
            }

            [NoAlias]
            internal NativeArray<SolvePhaseInfo> PhaseInfo;
            [NoAlias]
            internal NativeArray<int> NumActivePhases;
            [NoAlias]
            internal NativeReference<int> FirstWorkItemIndex;   // Index of first work item used for this scheduler info. Indicates where the
                                                                // solver data can be found in the corresponding streams (jacobian and contact).
                                                                // All work item indices specified in the SolvePhaseInfo of this scheduler info
                                                                // are relative to this index.
            [NoAlias]
            // Total number of work items in all phases for this scheduler info.
            internal NativeArray<int> NumWorkItems;

            [NoAlias]
            // Index of first dispatch pair for this scheduler info in the global dispatch pair array.
            internal NativeReference<int> FirstDispatchPairIndex;

            [NoAlias]
            // Total number of dispatch pairs in all phases for this scheduler info.
            internal NativeReference<int> NumDispatchPairs;

            // For a given work item index offset returns phase id.
            internal int FindPhaseId(int workItemIndexOffset)
            {
                int phaseId = 0;
                for (int i = NumActivePhases[0] - 1; i >= 0; i--)
                {
                    if (workItemIndexOffset >= PhaseInfo[i].FirstWorkItemIndexOffset)
                    {
                        phaseId = i;
                        break;
                    }
                }

                return phaseId;
            }

            // For a given work item index offset returns index into PhasedDispatchPairs and number of pairs to read.
            internal int GetWorkItemReadOffset(int workItemIndexOffset, out int pairReadCount)
            {
                SolvePhaseInfo phaseInfo = PhaseInfo[FindPhaseId(workItemIndexOffset)];

                int numItemsToRead = phaseInfo.BatchSize;
                int readStartOffset = phaseInfo.FirstDispatchPairOffset + (workItemIndexOffset - phaseInfo.FirstWorkItemIndexOffset) * phaseInfo.BatchSize;

                int lastWorkItemIndexOffset = phaseInfo.FirstWorkItemIndexOffset + phaseInfo.NumWorkItems - 1;
                bool isLastWorkItemInPhase = workItemIndexOffset == lastWorkItemIndexOffset;
                if (isLastWorkItemInPhase)
                {
                    int numPairsBeforeLastWorkItem = (phaseInfo.NumWorkItems - 1) * phaseInfo.BatchSize;
                    numItemsToRead = phaseInfo.DispatchPairCount - numPairsBeforeLastWorkItem;
                    readStartOffset = phaseInfo.FirstDispatchPairOffset + numPairsBeforeLastWorkItem;
                }

                pairReadCount = numItemsToRead;

                return readStartOffset;
            }

            public IterativeSolverSchedulerInfo(int numPhases, Allocator allocator = Allocator.TempJob)
            {
                PhaseInfo = new NativeArray<SolvePhaseInfo>(numPhases, allocator);
                NumActivePhases = new NativeArray<int>(1, allocator);
                FirstWorkItemIndex = new NativeReference<int>(0, allocator);
                NumWorkItems = new NativeArray<int>(1, allocator);
                FirstDispatchPairIndex = new NativeReference<int>(0, allocator);
                NumDispatchPairs = new NativeReference<int>(0, allocator);
            }

            public void Dispose()
            {
                if (PhaseInfo.IsCreated)
                {
                    PhaseInfo.Dispose();
                }

                if (NumActivePhases.IsCreated)
                {
                    NumActivePhases.Dispose();
                }

                if (FirstWorkItemIndex.IsCreated)
                {
                    FirstWorkItemIndex.Dispose();
                }

                if (NumWorkItems.IsCreated)
                {
                    NumWorkItems.Dispose();
                }

                if (FirstDispatchPairIndex.IsCreated)
                {
                    FirstDispatchPairIndex.Dispose();
                }

                if (NumDispatchPairs.IsCreated)
                {
                    NumDispatchPairs.Dispose();
                }
            }

            public JobHandle ScheduleDisposeJob(JobHandle inputDeps)
            {
                return JobHandle.CombineDependencies(new DisposeJob { PhaseInfo = PhaseInfo, NumActivePhases = NumActivePhases, NumWorkItems = NumWorkItems }.Schedule(inputDeps),
                    JobHandle.CombineDependencies(FirstWorkItemIndex.Dispose(inputDeps), NumDispatchPairs.Dispose(inputDeps), FirstDispatchPairIndex.Dispose(inputDeps)));
            }

            // A job to dispose the phase information
            [BurstCompile]
            struct DisposeJob : IJob
            {
                [DeallocateOnJobCompletion]
                public NativeArray<SolvePhaseInfo> PhaseInfo;

                [DeallocateOnJobCompletion]
                public NativeArray<int> NumActivePhases;

                [DeallocateOnJobCompletion]
                public NativeArray<int> NumWorkItems;

                public void Execute() {}
            }
        }

        struct DispatchPairComparer : IComparer<DispatchPair>
        {
            public int Compare(DispatchPair x, DispatchPair y)
            {
                return math.select(x.BodyIndexA.CompareTo(y.BodyIndexA),
                    x.BodyIndexASubArraySortKey.CompareTo(y.BodyIndexASubArraySortKey),
                    x.BodyIndexA == y.BodyIndexA);
            }
        }

        static void CreatePhasedDispatchPairsForSerialProcessing(int numDynamicBodies,
            NativeArray<DispatchPair> unsortedDispatchPairs, NativeArray<DispatchPair> dispatchPairs, int dispatchPairOffset,
            ref IterativeSolverSchedulerInfo solverSchedulerInfo, NativeArray<int> unphasedToPhasedDispatchPairMap = default)
        {
            NativeArray<IterativeSolverSchedulerInfo.SolvePhaseInfo> phaseInfo =
                new NativeArray<IterativeSolverSchedulerInfo.SolvePhaseInfo>(kMaxNumPhases, Allocator.Temp);
            CreateDispatchPairPhasesJob.CreateDispatchPairPhases(
                unsortedDispatchPairs, numDynamicBodies,
                dispatchPairs, out int numActivePhases, out int numWorkItems, ref phaseInfo, unphasedToPhasedDispatchPairMap);

            int totalDispatchPairs = 0;
            for (int i = 0; i < numActivePhases; i++)
            {
                totalDispatchPairs += phaseInfo[i].DispatchPairCount;
            }

            if (totalDispatchPairs > 0)
            {
                // combine all pairs into a single phase and work item
                solverSchedulerInfo.PhaseInfo[0] = new IterativeSolverSchedulerInfo.SolvePhaseInfo
                {
                    DispatchPairCount = totalDispatchPairs,
                    NumWorkItems = 1,
                    BatchSize = totalDispatchPairs,
                    FirstDispatchPairOffset = 0,
                    FirstWorkItemIndexOffset = 0
                };

                solverSchedulerInfo.NumActivePhases[0] = 1;
                solverSchedulerInfo.NumWorkItems[0] = 1;
            }
            else
            {
                solverSchedulerInfo.NumActivePhases[0] = 0;
                solverSchedulerInfo.NumWorkItems[0] = 0;
            }

            solverSchedulerInfo.FirstDispatchPairIndex.Value = dispatchPairOffset;
            solverSchedulerInfo.NumDispatchPairs.Value = totalDispatchPairs;
        }

        internal struct IslandSearchEdge
        {
            public DispatchPair Pair;
            public int PairIndex;
        }

        /// <summary>
        /// Finds islands amongst the direct solver pairs, each of which will be solved independently and potentially
        /// in parallel by the direct solver. The method uses a breadth-first search to find connected components in the graph
        /// formed by the direct solver pairs and adds them to the direct solver scheduler info.
        /// </summary>
        static void FindDirectSolverIslands(int numDynamicBodies,
            NativeArray<DispatchPair> directPairs,
            ref DirectSolverSchedulerInfo solverSchedulerInfo)
        {
            solverSchedulerInfo.DispatchPairIslandInfos.SetCapacity(directPairs.Length);

            // Find islands in the direct solver pairs by building a graph and finding connected components.

            // build graph
            var edges = new NativeParallelMultiHashMap<int, IslandSearchEdge>(directPairs.Length, Allocator.Temp);

            // add edges for each enabled direct solver pair:
            // 1) edges from body A to body B (knowing that body A is always dynamic)
            // 2) edges from body B to body A (if body B is dynamic)
            for (int i = 0; i < directPairs.Length; ++i)
            {
                var pair = directPairs[i];

                // We expect only valid and simulated pairs to be passed in here since we don't want to cause islands
                // to be connected across edges (constraints) which do not actually exist (are not simulated).
                SafetyChecks.CheckAreEqualAndThrow(true, pair.IsValid && (pair.IsJoint || pair.IsCollisionEnabled));

                var edge = new IslandSearchEdge
                {
                    Pair = pair,
                    PairIndex = i
                };

                edges.Add(pair.BodyIndexA, edge);

                if (pair.BodyIndexB < numDynamicBodies)
                {
                    // Exclude edges from static bodies so that we don't connect otherwise disjoint islands.
                    edges.Add(pair.BodyIndexB, edge);
                }
            }

            // Keep track of visited nodes (bodies)
            // @todo direct solver: could use a bloomfilter here, or add flag in RigidBody
            var visitedNodes = new NativeHashSet<int>(numDynamicBodies, Allocator.Temp);
            var visitedEdges = new NativeHashSet<int>(directPairs.Length, Allocator.Temp);
            int numIslandPairs = 0;
            int numIslandPairsTotal = 0;
            var numProcessedDirectPairs = 0;
            var queue = new NativeQueue<int>(Allocator.Temp);
            for (int i = 0; i < directPairs.Length; ++i)
            {
                var pair = directPairs[i];
                var pairIndex = i;
                if (visitedEdges.Contains(pairIndex))
                {
                    // already visited
                    continue;
                }

                // Sanity check: if edge was not yet visited, both of its nodes should also not have been visited yet.
                SafetyChecks.CheckAreEqualAndThrow(false, visitedNodes.Contains(pair.BodyIndexA) &&
                    (pair.BodyIndexB >= numDynamicBodies || visitedNodes.Contains(pair.BodyIndexB)));

                // breadth first search:
                // @todo direct solver (DOTS-10860): for bandwidth reduction, in the future, we need to do Cuthill-McKee or its reverse here.

                SafetyChecks.CheckAreEqualAndThrow(true, queue.IsEmpty());
                queue.Enqueue(pair.BodyIndexA);

                while (queue.Count > 0)
                {
                    int bodyIndex = queue.Dequeue();

                    if (visitedNodes.Contains(bodyIndex))
                    {
                        continue;
                    }

                    visitedNodes.Add(bodyIndex);

                    // visit all neighboring nodes (bodies) across edges (dispatch pairs)
                    if (edges.TryGetFirstValue(bodyIndex, out var connectedEdge, out var iterator))
                    {
                        do
                        {
                            if (visitedEdges.Contains(connectedEdge.PairIndex))
                            {
                                // already visited this edge
                                continue;
                            }

                            // add the edge to the island
                            var info = new DispatchPairIslandInfo()
                            {
                                UnphasedDispatchPairIndex = connectedEdge.PairIndex,
                                IslandIndex = solverSchedulerInfo.DispatchPairIslandInfoCounts.Length
                            };
                            solverSchedulerInfo.DispatchPairIslandInfos.Add(info);
                            ++numProcessedDirectPairs;
                            ++numIslandPairs;
                            visitedEdges.Add(connectedEdge.PairIndex);

                            var connectedPair = connectedEdge.Pair;
                            int otherBodyIndex = math.select(connectedPair.BodyIndexA, connectedPair.BodyIndexB,
                                connectedPair.BodyIndexA == bodyIndex);
                            if (otherBodyIndex < numDynamicBodies && // skip static bodies
                                !visitedNodes.Contains(otherBodyIndex))
                            {
                                queue.Enqueue(otherBodyIndex);
                            }
                        }
                        while (edges.TryGetNextValue(out connectedEdge, ref iterator));
                    }
                }

                solverSchedulerInfo.DispatchPairIslandInfoCounts.Add(numIslandPairs);
                solverSchedulerInfo.FirstDispatchPairIslandInfoIndices.Add(numIslandPairsTotal);
                numIslandPairsTotal += numIslandPairs;
                numIslandPairs = 0;
            }

            // Make sure we have as many assigned island pairs total as direct pairs.
            SafetyChecks.CheckAreEqualAndThrow(directPairs.Length, numProcessedDirectPairs);
        }

        /// <summary>
        /// Merge streams of body pairs and joints into a sorted array of dispatch pairs.
        /// </summary>
        internal static void CreateDispatchPairs(in SimulationStepInput stepInput,
            ref NativeStream dynamicVsDynamicBodyPairs, ref NativeStream staticVsDynamicBodyPairs,
            ref SolverSchedulerInfo solverSchedulerInfo, ref NativeList<DispatchPair> dispatchPairs)
        {
            int numDynamicBodies = stepInput.World.NumDynamicBodies;
            var tempPairs = new NativeList<DispatchPair>(Allocator.Temp);

            CreateUnsortedDispatchPairs(stepInput, dynamicVsDynamicBodyPairs, staticVsDynamicBodyPairs, tempPairs,
                out int numIterativePairs, out int numDirectPairs);

            int numIterativeCouplingPairs = 0;

            if (stepInput.DirectSolverSettings.ForceDirectSolver)
            {
                // The direct solver will be used for all constraints. Therefore, there are no coupling constraints,
                // which means, we don't need to set the direct solver body flags or the coupling flags.
                // Also, the solver type will have to be forced to be direct for all constraints.
                for (int i = 0; i < tempPairs.Length; ++i)
                {
                    var pair = tempPairs[i];
                    pair.SolverType = SolverType.Direct;
                    tempPairs[i] = pair;
                }

                numIterativePairs = 0;
                numDirectPairs = tempPairs.Length;
            }
            else if (numDirectPairs > 0)
            {
                var directSolverBodyFlags = new NativeArray<bool>(numDynamicBodies, Allocator.Temp,
                    // Note: important to clear memory for direct solver body flags, so that all are set to false initially.
                    options: NativeArrayOptions.ClearMemory);

                SetDirectSolverBodyFlags(stepInput, dynamicVsDynamicBodyPairs, staticVsDynamicBodyPairs,
                    directSolverBodyFlags);

                SetCouplingConstraintFlags(tempPairs, directSolverBodyFlags, out numIterativeCouplingPairs);
            }

            // Sort all pairs so that they are in lexicographical order of the body pairs. This will allow us to implement
            // the joints' "Enable Collisions" feature, and makes sure that they are in the right order for the iterative solver
            // with contact constraints being processed last for lower constraint error after each solver iteration.
            SortDispatchPairs(tempPairs);

            SetDisableCollisionFlags(tempPairs);

            dispatchPairs.ResizeUninitialized(tempPairs.Length);

            // Early out if no dispatch pairs
            if (tempPairs.Length > 0)
            {
                // Process the iterative, direct and coupling pairs separately and create one consecutive array of pairs
                // with the following layout:
                //     [ iterative pairs [iterative coupling pairs] ][ direct pairs ]
                //
                // - direct pairs: dispatch pairs to be solved with the direct solver.
                // - iterative pairs: dispatch pairs to be solved with the iterative solver.
                // - iterative coupling pairs: dispatch pairs to be solved with the iterative solver which share bodies
                //      with direct solver pairs, and thus require coupling. Note that these pairs appear at the end of
                //      the iterative pairs sub-array, leaving the purely iterative pairs (without coupling) at the
                //      beginning of the iterative pairs sub-array.
                //
                // All of these pair sub-arrays will be organized into phases of independent constraint subsets using
                // graph coloring for solving with the iterative solver, even the direct pairs.
                // Note that there will likely be disabled pairs following each constraint type due to the joints'
                // disable collision feature. These might need to be ignored in certain situations using DispatchPair.IsValid.

                var numPureIterativePairs = numIterativePairs - numIterativeCouplingPairs;
                if (numDirectPairs == 0)
                {
                    // no direct pairs
                    CreatePhasedDispatchPairsForSerialProcessing(numDynamicBodies, tempPairs.AsArray(), dispatchPairs.AsArray(),
                        dispatchPairOffset: 0, ref solverSchedulerInfo.IterativePairsIterativeScheduling);
                }
                else
                {
                    if (numIterativePairs > 0)
                    {
                        // prepare iterative pair array, separate from direct pairs, organized as
                        //  [ pure iterative pairs (without coupling) ] [iterative pairs with coupling ].
                        // The pure iterative pairs are filled in from the front, while the iterative pairs with
                        // coupling are filled in from the back.

                        var unphasedIterativePairs = new NativeArray<DispatchPair>(numIterativePairs, Allocator.Temp);
                        int pureIterativePairIndex = 0;
                        int iterativeCouplingPairIndex = numPureIterativePairs;
                        for (int i = 0; i < tempPairs.Length; ++i)
                        {
                            var pair = tempPairs[i];
                            if (pair.SolverType == SolverType.Iterative)
                            {
                                if (pair.IsCouplingEnabled)
                                {
                                    unphasedIterativePairs[iterativeCouplingPairIndex++] = pair;
                                }
                                else
                                {
                                    unphasedIterativePairs[pureIterativePairIndex++] = pair;
                                }
                            }
                        }

                        // Now "phase" the purely iterative and iterative coupling pairs separately using graph coloring.
                        // Each phasing requires as input the corresponding sub-array of "unphased" iterative pairs
                        // described above.

                        if (numPureIterativePairs > 0)
                        {
                            var unphasedPureIterativePairs =
                                unphasedIterativePairs.GetSubArray(0, numPureIterativePairs);
                            var pureIterativePairs =
                                dispatchPairs.AsArray().GetSubArray(0, numPureIterativePairs);

                            CreatePhasedDispatchPairsForSerialProcessing(numDynamicBodies, unphasedPureIterativePairs, pureIterativePairs,
                                dispatchPairOffset: 0, ref solverSchedulerInfo.IterativePairsIterativeScheduling);
                        }

                        // @todo direct solver (DOTS-10982): we keep these separate for now so that we can benefit from
                        // cases in which we want to sacrifice coupling quality in favor of performance, as we
                        // will be able to solve the direct islands and the uncoupled iterative constraints in parallel.
                        // In the future, we might want to remove this distinction.
                        if (numIterativeCouplingPairs > 0)
                        {
                            SafetyChecks.CheckAreEqualAndThrow(numIterativePairs, unphasedIterativePairs.Length);
                            var unphasedIterativeCouplingPairs =
                                unphasedIterativePairs.GetSubArray(numPureIterativePairs, numIterativeCouplingPairs);
                            var iterativeCouplingPairs =
                                dispatchPairs.AsArray().GetSubArray(numPureIterativePairs, numIterativeCouplingPairs);

                            CreatePhasedDispatchPairsForSerialProcessing(numDynamicBodies, unphasedIterativeCouplingPairs, iterativeCouplingPairs,
                                dispatchPairOffset: numPureIterativePairs, ref solverSchedulerInfo.CouplingPairsIterativeScheduling);
                        }
                    }

                    if (numDirectPairs > 0)
                    {
                        // Here, we produce two data sets:
                        // 1) a phased dispatch pair array for the direct pairs, just like for the iterative pairs above.
                        //  This is so that we can solve these also with the iterative solver efficiently. This array will be
                        //  concatenated with the iterative pairs array.
                        // 2) island information for the direct pairs.

                        // First, we need to identify all the direct pairs.
                        var directPairs = new NativeList<DispatchPair>(numDirectPairs, Allocator.Temp);
                        for (int i = 0; i < tempPairs.Length; ++i)
                        {
                            var pair = tempPairs[i];
                            if (pair.SolverType == SolverType.Direct &&
                                (pair.IsJoint || pair.IsCollisionEnabled)) // exclude disabled collision pairs
                            {
                                directPairs.Add(pair);
                            }
                        }

                        // update final direct pairs count
                        numDirectPairs = directPairs.Length;
                        if (numDirectPairs > 0)
                        {
                            // Prepare mapping storage:
                            solverSchedulerInfo.DirectPairsDirectScheduling.ResizeMaps(numDirectPairs);

                            // Phase direct pairs, and create unphased-to-phased map:

                            var directPairsOffset = numIterativePairs;
                            solverSchedulerInfo.DirectPairsDirectScheduling.FirstDispatchPairIndex.Value = directPairsOffset;

                            var phasedDirectPairs =
                                dispatchPairs.AsArray().GetSubArray(numIterativePairs, numDirectPairs);
                            CreatePhasedDispatchPairsForSerialProcessing(numDynamicBodies, directPairs.AsArray(), phasedDirectPairs,
                                dispatchPairOffset: directPairsOffset, ref solverSchedulerInfo.DirectPairsIterativeScheduling,
                                solverSchedulerInfo.DirectPairsDirectScheduling.UnphasedToPhasedDispatchPairMap.AsArray());

                            // Find islands:

                            // @todo direct solver (DOTS-10982): in the future, for efficiency, include coupling pairs
                            // here so that we can identify the islands that have coupling with iterative and those that don't.
                            // This allows us to solve the uncoupled direct solver islands in parallel to the iterative solve,
                            // and exclude the corresponding constraints from the iterative solve.
                            // Note: We will need to ignore invalid coupling dispatch pairs in this case (pairs that were
                            // disabled due to the joints' disable collision feature), since some might remain at the end
                            // of the coupling pairs array section.

                            FindDirectSolverIslands(numDynamicBodies, directPairs.AsArray(), ref solverSchedulerInfo.DirectPairsDirectScheduling);
                        }
                    }
                }
            }

            // Now that all the individual iterative solver scheduler infos have been computed, update their total work
            // item count and work item start indices in the global solver scheduler info.
            solverSchedulerInfo.FinalizeIterativeWorkItemInfo();
        }

        /// <summary>
        /// Schedule a set of jobs to merge streams of body pairs and joints into a sorted array of
        /// dispatch pairs.
        /// </summary>
        internal static SimulationJobHandles ScheduleCreatePhasedDispatchPairsJob(in SimulationStepInput stepInput,
            ref NativeStream dynamicVsDynamicBroadphasePairsStream, ref NativeStream staticVsDynamicBroadphasePairsStream,
            JobHandle inputDeps, ref NativeList<DispatchPair> dispatchPairs, out SolverSchedulerInfo solverSchedulerInfo,
            bool multiThreaded = true)
        {
            SimulationJobHandles returnHandles = default;

            if (!multiThreaded)
            {
                dispatchPairs = new NativeList<DispatchPair>(Allocator.TempJob);

                solverSchedulerInfo = new SolverSchedulerInfo(numPhases: 1);

                returnHandles.FinalExecutionHandle = new CreateDispatchPairsJob
                {
                    StepInput = stepInput,
                    SolverSchedulerInfo = solverSchedulerInfo,
                    DispatchPairs = dispatchPairs,
                    DynamicVsDynamicBroadphasePairsStream = dynamicVsDynamicBroadphasePairsStream,
                    StaticVsDynamicBroadphasePairsStream = staticVsDynamicBroadphasePairsStream,
                }.Schedule(inputDeps);

                // We must dispose pair streams here, since we don't have access to them
                // where we would ideally like to schedule their disposal
                returnHandles.FinalExecutionHandle = JobHandle.CombineDependencies(
                    dynamicVsDynamicBroadphasePairsStream.Dispose(returnHandles.FinalExecutionHandle),
                    staticVsDynamicBroadphasePairsStream.Dispose(returnHandles.FinalExecutionHandle));

                return returnHandles;
            }
            // else: (multiThreaded)

            // Parallel Scheduler:
            //
            // 1) In parallel, create unsorted pairs, with solver type fixed to a given type (direct if forced, iterative otherwise).
            //    Documentation for parallel implementation can be found in CreateUnsortedDispatchPairs().
            // 2) In the following order (unless direct is forced):
            //      2.1) In parallel, set solver type for all dispatch pairs (parallel for job, running over length of unsorted pairs list)
            //           Input: unsorted pairs list
            //           Output: numIterativePairs, numDirectPairs counters
            //      2.2) In parallel, set direct solver body flags for all bodies in the unsorted pairs. No race condition possible
            //           since default value for all flags in the array is false and all jobs set the flag to true if at all.
            //           Input: numDirectPairs counter from 2.1
            //           Note: Early out if numDirectPairs == 0.
            //      2.3) In parallel, set coupling constraint flags for all dispatch pairs, using the direct solver body flags.
            //           Input: numDirectPairs counter from 2.1
            //           Output: numIterativeCouplingPairs counter
            //           Note: Early out if numDirectPairs == 0.
            // 3) In parallel, lexicographically sort the unsorted pairs array, using radix sort (see ScheduleSortJob()).
            // 4) In parallel, set disable collision flags for all dispatch pairs, using the "Enable Collision" flag.
            //    This is done by parallelizing SetDisableCollisionFlags() as documented in the function.
            // 5) In parallel, perform the following operations:
            //      5.1) Phase the iterative pairs.
            //           Input: lexicographically sorted pairs list, numIterativePairs and numIterativeCouplingPairs counters
            //           Output: phased pairs sub-array and solver scheduler info for iterative pairs
            //      5.2) Phase the coupling pairs.
            //           Input: lexicographically sorted pairs list, numIterativePairs and numIterativeCouplingPairs counters
            //           Output: phased pairs sub-array and solver scheduler info for coupling pairs
            //      5.3) Phase the direct pairs. Before processing, resize the direct solver scheduler info maps (see DirectSolverSchedulerInfo.ResizeMaps()).
            //           Input: lexicographically sorted pairs list, numIterativePairs and numDirectPairs counters, and UnphasedToPhasedDispatchPairMap.
            //           Output: phased pairs sub-array and solver scheduler info for direct pairs, as well as the UnphasedToPhasedDispatchPairMap.
            //      5.4) Find direct solver islands.
            //           Input: lexicographically sorted pairs list, numIterativePairs and numDirectPairs counters
            //           Output: direct solver island info in the direct solver scheduler info
            //      5.5) With dependency on 5.1, 5.2 and 5.3, set total iterative work item count.
            //           See SolverSchedulerInfo.UpdateIterativeWorkItemInfo().

            var numWorkers = math.max(1, JobsUtility.JobWorkerCount);

            var unsortedPairs = new NativeList<DispatchPair>(Allocator.TempJob);
            var sortedPairs = new NativeList<DispatchPair>(Allocator.TempJob);
            var jointStreamPrefixSum = new NativeList<int>(numWorkers + 1, Allocator.TempJob);
            var dynamicVsDynamicStreamPrefixSum = new NativeList<int>(Allocator.TempJob);
            var staticVsDynamicStreamPrefixSum = new NativeList<int>(Allocator.TempJob);
            // @todo direct solver (DOTS-11060): use world allocator here
            var jointDispatchPairsStream = new NativeStream(numWorkers, Allocator.TempJob);
            // @todo direct solver (DOTS-11060): use world allocator here, and/or reuse memory some place
            var directSolverBodyFlags = new NativeArray<bool>(stepInput.World.NumDynamicBodies, Allocator.TempJob,
                // Note: important to clear memory for direct solver body flags, so that all are set to false initially.
                options: NativeArrayOptions.ClearMemory);
            // Note: use int for directSolverUsed flag instead of bool so that it can be used as work item count in
            // parallel-for-defer jobs in order to eliminate any job execution whatsoever when the flag is 0 (false).
            var directSolverUsed = new NativeReference<int>(0, Allocator.TempJob);
            var numJointsToProcessForDirectSolver = new NativeReference<int>(0, Allocator.TempJob);
            var numDynamicVsDynamicStreamsToProcessForDirectSolver = new NativeReference<int>(0, Allocator.TempJob);
            var numStaticVsDynamicStreamsToProcessForDirectSolver = new NativeReference<int>(0, Allocator.TempJob);
            var numPairsToProcessForCouplingDetection = new NativeReference<int>(0, Allocator.TempJob);
            var numWorkersForPairGrouping = new NativeReference<int>(0, Allocator.TempJob);

            using var disposeHandles = new NativeList<JobHandle>(32, Allocator.Temp);

            // Merge broadphase pairs and joint pairs into the unsorted array in parallel:

            JobHandle dynamicVsDynamicStreamPrefixSumHandle = new CreateStreamPrefixSumJob
            {
                Reader = dynamicVsDynamicBroadphasePairsStream.AsReader(),
                StreamPrefixSum = dynamicVsDynamicStreamPrefixSum
            }.Schedule(inputDeps);

            JobHandle staticVsDynamicStreamPrefixSumHandle = new CreateStreamPrefixSumJob
            {
                Reader = staticVsDynamicBroadphasePairsStream.AsReader(),
                StreamPrefixSum = staticVsDynamicStreamPrefixSum
            }.Schedule(inputDeps);

            JobHandle prefixSumHandle = JobHandle.CombineDependencies(dynamicVsDynamicStreamPrefixSumHandle, staticVsDynamicStreamPrefixSumHandle);

            // Allocate unsorted and sorted dispatch pair arrays
            JobHandle allocateDispatchPairs = new AllocateDispatchPairsJob
            {
                Joints = stepInput.World.DynamicsWorld.InternalJointsList.AsDeferredJobArray(),
                DynamicVsDynamicStreamPrefixSum = dynamicVsDynamicStreamPrefixSum,
                StaticVsDynamicStreamPrefixSum = staticVsDynamicStreamPrefixSum,
                UnsortedDispatchPairs = unsortedPairs,
                SortedDispatchPairs = sortedPairs
            }.Schedule(prefixSumHandle);

            // Here we launch three parallel chains of jobs for creation of the unsorted dispatch pair array,
            // organized as [dynamics vs. dynamic collision pairs][static vs. dynamic collision pairs][joint pairs].
            // These are:
            // 1) Creating dynamic vs. dynamic collision dispatch pairs: CreateCollisionDispatchPairsJob
            // 2) Creating static vs. dynamic collision dispatch pairs: CreateCollisionDispatchPairsJob
            // 3) Creating joint dispatch pairs: CreateJointDispatchPairsJob -> CreateStreamPrefixSumJob -> MergeJointDispatchPairsStreamJob
            // Finally, we shrink the dispatch pair array using the ShrinkDispatchPairListJob, allocated above assuming
            // all joints are valid, in case there are any invalid joints.

            JobHandle dynamicVsDynamicPairsHandle;
            JobHandle staticVsDynamicPairsHandle;
            JobHandle jointPairsHandle;
            // 1) Creating dynamic vs. dynamic collision dispatch pairs: CreateCollisionDispatchPairsJob
            dynamicVsDynamicPairsHandle = new CreateCollisionDispatchPairsJob
            {
                DispatchPairs = unsortedPairs.AsDeferredJobArray(),
                PrecedingStreamHistogram = default, // no preceding stream for dynamic vs. dynamic pairs in the dispatch pair array
                OverlapResultsStreamHistogram = dynamicVsDynamicStreamPrefixSum.AsDeferredJobArray(),
                OverlapResultsReader = dynamicVsDynamicBroadphasePairsStream.AsReader(),
            }.ScheduleUnsafe(dynamicVsDynamicBroadphasePairsStream, 1, allocateDispatchPairs);

            // 2) Creating static vs. dynamic collision dispatch pairs: CreateCollisionDispatchPairsJob
            staticVsDynamicPairsHandle = new CreateCollisionDispatchPairsJob
            {
                DispatchPairs = unsortedPairs.AsDeferredJobArray(),
                PrecedingStreamHistogram = dynamicVsDynamicStreamPrefixSum.AsDeferredJobArray(),
                OverlapResultsStreamHistogram = staticVsDynamicStreamPrefixSum.AsDeferredJobArray(),
                OverlapResultsReader = staticVsDynamicBroadphasePairsStream.AsReader(),
            }.ScheduleUnsafe(staticVsDynamicBroadphasePairsStream, 1, allocateDispatchPairs);

            // 3) Creating joint dispatch pairs:
            jointPairsHandle = new CreateJointDispatchPairsJob
            {
                Joints = stepInput.World.DynamicsWorld.InternalJointsList.AsDeferredJobArray(),
                JointDispatchPairWriter = jointDispatchPairsStream.AsWriter(),
                NumWorkers = numWorkers,
            }.ScheduleUnsafe(jointDispatchPairsStream, 1, inputDeps);

            jointPairsHandle = new CreateStreamPrefixSumJob
            {
                Reader = jointDispatchPairsStream.AsReader(),
                StreamPrefixSum = jointStreamPrefixSum
            }.Schedule(jointPairsHandle);

            jointPairsHandle = new MergeJointDispatchPairsStreamJob
            {
                DispatchPairs = unsortedPairs,
                JointDispatchPairReader = jointDispatchPairsStream.AsReader(),
                JointDispatchPairStreamHistogram = jointStreamPrefixSum.AsDeferredJobArray(),
                DynamicVsDynamicStreamHistogram = dynamicVsDynamicStreamPrefixSum.AsDeferredJobArray(),
                StaticVsDynamicStreamHistogram = staticVsDynamicStreamPrefixSum.AsDeferredJobArray()
            }.ScheduleUnsafe(jointDispatchPairsStream, 1, JobHandle.CombineDependencies(jointPairsHandle, allocateDispatchPairs));

            JobHandle dispatchPairsHandle = JobHandle.CombineDependencies(jointPairsHandle,  dynamicVsDynamicPairsHandle, staticVsDynamicPairsHandle);

            dispatchPairsHandle = new ShrinkDispatchPairListJob
            {
                UnsortedDispatchPairs = unsortedPairs,
                SortedDispatchPairs = sortedPairs,
                DynamicVsDynamicStreamPrefixSum = dynamicVsDynamicStreamPrefixSum.AsDeferredJobArray(),
                StaticVsDynamicStreamPrefixSum = staticVsDynamicStreamPrefixSum.AsDeferredJobArray(),
                JointStreamPrefixSum = jointStreamPrefixSum.AsDeferredJobArray()
            }.Schedule(dispatchPairsHandle);

            // dispose dependent resources once done with creation of unsorted pair array
            disposeHandles.Add(jointStreamPrefixSum.Dispose(dispatchPairsHandle));
            disposeHandles.Add(dynamicVsDynamicStreamPrefixSum.Dispose(dispatchPairsHandle));
            disposeHandles.Add(staticVsDynamicStreamPrefixSum.Dispose(dispatchPairsHandle));
            disposeHandles.Add(jointDispatchPairsStream.Dispose(dispatchPairsHandle));

            // if direct solver is forced on, mark all pairs as direct
            if (stepInput.DirectSolverSettings.ForceDirectSolver)
            {
                // The direct solver will be used for all constraints. Therefore, there are no coupling constraints, which means, we
                // don't need to set the direct solver body flags or the coupling flags.
                // Also, the solver type will have to be forced to be direct for all constraints.
                dispatchPairsHandle = new ForceDirectSolverOnAllPairsJob
                {
                    DispatchPairs = unsortedPairs,
                }.Schedule(unsortedPairs, 32, dispatchPairsHandle);

                // Make sure that the direct solver used flag is set in order to enable subsequent direct solver related phases
                directSolverUsed.Value = 1;
            }
            else
            {
                // In parallel, and if required, set direct solver body flags for later coupling detection.
                // Note: we only enable this phase if the direct solver is not forced on for everything. In this case,
                // there is no coupling and we don't need to prepare for the coupling detection consequently.

                JobHandle enableDirectSolverPairProcessingHandle = new EnableDirectSolverPairProcessing
                {
                    Joints = stepInput.World.DynamicsWorld.InternalJointsList.AsDeferredJobArray(),
                    DynamicVsDynamicBroadphaseStream = dynamicVsDynamicBroadphasePairsStream,
                    StaticVsDynamicBroadphaseStream = staticVsDynamicBroadphasePairsStream,
                    DirectSolverEnabledFlag = stepInput.World.DynamicsWorld.DirectSolverEnabledFlag,
                    NumJointsToProcess = numJointsToProcessForDirectSolver,
                    NumDynamicVsDynamicBroadphaseStreamsToProcess = numDynamicVsDynamicStreamsToProcessForDirectSolver,
                    NumStaticVsDynamicBroadphaseStreamsToProcess = numStaticVsDynamicStreamsToProcessForDirectSolver
                }.Schedule(inputDeps);

                JobHandle directSolverBodyFlagsJointsHandle = new SetDirectSolverBodyFlagsForJointsJob
                {
                    StepInput = stepInput,
                    DirectSolverBodyFlags = directSolverBodyFlags,
                    DirectSolverUsed = directSolverUsed
                }.ScheduleUnsafe(numJointsToProcessForDirectSolver, 16, enableDirectSolverPairProcessingHandle);

                JobHandle directSolverBodyFlagsDynamicVsDynamicHandle = new SetDirectSolverBodyFlagsForBroadphasePairsJob
                {
                    OverlapResultsReader = dynamicVsDynamicBroadphasePairsStream.AsReader(),
                    DirectSolverBodyFlags = directSolverBodyFlags,
                    DirectSolverUsed = directSolverUsed,
                    DirectSolverEnabledFlag = stepInput.World.DynamicsWorld.DirectSolverEnabledFlag,
                    StaticDynamicOverlaps = false
                }.ScheduleUnsafe(numDynamicVsDynamicStreamsToProcessForDirectSolver, 1, enableDirectSolverPairProcessingHandle);

                JobHandle directSolverBodyFlagsStaticVsDynamicHandle = new SetDirectSolverBodyFlagsForBroadphasePairsJob
                {
                    OverlapResultsReader = staticVsDynamicBroadphasePairsStream.AsReader(),
                    DirectSolverBodyFlags = directSolverBodyFlags,
                    DirectSolverUsed = directSolverUsed,
                    DirectSolverEnabledFlag = stepInput.World.DynamicsWorld.DirectSolverEnabledFlag,
                    StaticDynamicOverlaps = true
                }.ScheduleUnsafe(numStaticVsDynamicStreamsToProcessForDirectSolver, 1, enableDirectSolverPairProcessingHandle);

                dispatchPairsHandle = JobHandle.CombineDependencies(dispatchPairsHandle,
                    JobHandle.CombineDependencies(directSolverBodyFlagsJointsHandle,
                        directSolverBodyFlagsDynamicVsDynamicHandle, directSolverBodyFlagsStaticVsDynamicHandle));
            }

            // Now, dispose the broad phase pairs
            disposeHandles.Add(dynamicVsDynamicBroadphasePairsStream.Dispose(dispatchPairsHandle));
            disposeHandles.Add(staticVsDynamicBroadphasePairsStream.Dispose(dispatchPairsHandle));
            disposeHandles.Add(numJointsToProcessForDirectSolver.Dispose(dispatchPairsHandle));
            disposeHandles.Add(numDynamicVsDynamicStreamsToProcessForDirectSolver.Dispose(dispatchPairsHandle));
            disposeHandles.Add(numStaticVsDynamicStreamsToProcessForDirectSolver.Dispose(dispatchPairsHandle));

            // Enable coupling detection (and later pair grouping) if needed, i.e., if the direct solver is in use indicated
            // by the DirectSolverUsed flag. The EnableCouplingDetectionAndPairGroupingJob checks this flag and sets the number
            // of pairs to process for the coupling analysis to 0 or the total number of dispatch pairs accordingly.
            // Analogously, it sets the number of workers for the grouping process to 0 or the actual worker count.
            JobHandle couplingDetectionHandle = new EnableCouplingDetectionAndPairGroupingJob
            {
                StepInput = stepInput,
                DispatchPairs = unsortedPairs.AsDeferredJobArray(),
                NumPairsToProcessForCouplingDetection = numPairsToProcessForCouplingDetection,
                DirectSolverUsed = directSolverUsed,
                NumWorkersForPairGrouping = numWorkersForPairGrouping,
                NumWorkers = numWorkers
            }.Schedule(dispatchPairsHandle);

            // Perform coupling detection (unless direct solver is forced on for everything)
            if (!stepInput.DirectSolverSettings.ForceDirectSolver)
            {
                couplingDetectionHandle = new DetectCouplingConstraintsJob
                {
                    DispatchPairs = unsortedPairs.AsDeferredJobArray(),
                    DirectSolverBodyFlags = directSolverBodyFlags
                }.ScheduleUnsafe(numPairsToProcessForCouplingDetection, 16, couplingDetectionHandle);
            }

            disposeHandles.Add(numPairsToProcessForCouplingDetection.Dispose(couplingDetectionHandle));
            disposeHandles.Add(directSolverBodyFlags.Dispose(couplingDetectionHandle));

            dispatchPairsHandle = couplingDetectionHandle;

            // Sort dispatch pairs. Here, we have two cases:
            //  1)  Direct solver pairs are present. In this case, we need to ultimately re-organize all pairs
            //      into [purely iterative pairs][coupling pairs][direct pairs] order, with each section internally
            //      lexicographically sorted and then phased.
            //  2)  No direct solver pairs are present. In this case, we only have purely iterative pairs, and don't need
            //      the above re-organization.
            // However, in both cases, we need to have a first, global lexicographical sorting of all pairs in order to implement
            // the joints' "Enable Collisions" feature, which requires the corresponding pair ordering.
            //
            // Here, we start with the global lexicographical first sorting, followed by the "Enable Collisions" feature.
            // After that, we will do perform the optional pair re-organization from (1) and then the phasing.
            JobHandle sortHandle = ScheduleSortJob(stepInput.World.NumBodies, unsortedPairs, sortedPairs, dispatchPairsHandle);

            sortHandle = new SetDisableCollisionFlagsJob
            {
                DispatchPairs = sortedPairs.AsDeferredJobArray(),
                NumWorkers = numWorkers
            }.Schedule(numWorkers, 1, sortHandle);

            // Create phases for multi-threading in iterative solvers and islands for the direct solver.
            // 2 cases:
            // 1) No direct pairs: phase all pairs as iterative pairs
            // 2) Direct pairs: group pairs into purely iterative pairs, coupling pairs and direct pairs, and phase each
            //      category separately. Also, find direct solver islands.
            // Both cases are implemented by grouping pairs into the three required categories if required and overwriting
            // the sortedPairs array correspondingly. The sortedPairs array will then be organized as follows:
            //     [ iterative pairs [iterative coupling pairs] ][ direct pairs ]

            // Set output data structures:
            solverSchedulerInfo = new SolverSchedulerInfo(kMaxNumPhases);
            dispatchPairs = unsortedPairs;

            // Stage 1: Pair grouping into [ iterative pairs [iterative coupling pairs] ][ direct pairs ]
            // First we need to identify the pairs that are iterative, coupling and direct, and group them accordingly.
            // To this end we launch "worker count" jobs that each have associated stream buffers of 3 streams to fill,
            // one for each pair type "iterative, coupling and direct".
            // They are all responsible for a section of the dispatch pairs and will copy them into the corresponding
            // streams (scatter phase).
            // Once done, we will compute the offsets of the pairs in each stream buffer into the final array, i.e.,
            // the prefix sum of the series of pair counts of each stream's buffers.
            // Afterwards, the workers will copy the pairs from their associated stream buffers into the final array at
            // the appropriate offsets in parallel (gather phase).
            // Note that the resultant sub-arrays will still be lexicographically sorted by design given that they were already
            // lexicographically sorted previously. The lexicographical order is a pre-requisite for the quality and speed
            // of the iterative solver, ensuring that data of bodies involved in constraints is accessed in a cache friendly
            // manner during the solve, and that certain constraint types (e.g. contacts) are solved last for reduced constraint error.

            var iterativePairsStream = new NativeStream(numWorkers, Allocator.TempJob);
            var couplingPairsStream = new NativeStream(numWorkers, Allocator.TempJob);
            var directPairsStream = new NativeStream(numWorkers, Allocator.TempJob);
            var iterativePairsStreamPrefixSum = new NativeList<int>(numWorkers + 1, Allocator.TempJob);
            var couplingPairsStreamPrefixSum = new NativeList<int>(numWorkers + 1, Allocator.TempJob);
            var directPairsStreamPrefixSum = new NativeList<int>(numWorkers + 1, Allocator.TempJob);
            var numDirectPairs = new NativeReference<int>(0, Allocator.TempJob);
            var numIterativeCouplingPairs = new NativeReference<int>(0, Allocator.TempJob);

            // grouping:
            JobHandle groupPairsHandle = new ScatterDispatchPairsByTypeJob
            {
                DispatchPairs = sortedPairs.AsDeferredJobArray(),
                IterativePairsWriter = iterativePairsStream.AsWriter(),
                CouplingPairsWriter = couplingPairsStream.AsWriter(),
                DirectPairsWriter = directPairsStream.AsWriter(),
                NumWorkers = numWorkers
            }.ScheduleUnsafe(numWorkersForPairGrouping, 1, sortHandle);

            var iterativePairsPrefixSumHandle = new CreateStreamPrefixSumJobDefer
            {
                Reader = iterativePairsStream.AsReader(),
                StreamPrefixSum = iterativePairsStreamPrefixSum
            }.ScheduleUnsafe(directSolverUsed, 1, groupPairsHandle);

            var couplingPairsPrefixSumHandle = new CreateStreamPrefixSumJobDefer
            {
                Reader = couplingPairsStream.AsReader(),
                StreamPrefixSum = couplingPairsStreamPrefixSum
            }.ScheduleUnsafe(directSolverUsed, 1, groupPairsHandle);

            var directPairsPrefixSumHandle = new CreateStreamPrefixSumJobDefer
            {
                Reader = directPairsStream.AsReader(),
                StreamPrefixSum = directPairsStreamPrefixSum
            }.ScheduleUnsafe(directSolverUsed, 1, groupPairsHandle);

            groupPairsHandle = JobHandle.CombineDependencies(iterativePairsPrefixSumHandle,
                couplingPairsPrefixSumHandle, directPairsPrefixSumHandle);

            groupPairsHandle = new GatherDispatchPairsByTypeJob
            {
                DispatchPairs = sortedPairs.AsDeferredJobArray(),
                IterativePairsReader = iterativePairsStream.AsReader(),
                CouplingPairsReader = couplingPairsStream.AsReader(),
                DirectPairsReader = directPairsStream.AsReader(),
                IterativePairsStreamPrefixSum = iterativePairsStreamPrefixSum.AsDeferredJobArray(),
                CouplingPairsStreamPrefixSum = couplingPairsStreamPrefixSum.AsDeferredJobArray(),
                DirectPairsStreamPrefixSum = directPairsStreamPrefixSum.AsDeferredJobArray()
            }.ScheduleUnsafe(numWorkersForPairGrouping, 1, groupPairsHandle);

            disposeHandles.Add(numWorkersForPairGrouping.Dispose(groupPairsHandle));
            disposeHandles.Add(iterativePairsStream.Dispose(groupPairsHandle));
            disposeHandles.Add(couplingPairsStream.Dispose(groupPairsHandle));
            disposeHandles.Add(directPairsStream.Dispose(groupPairsHandle));

            // if required (direct solver is used), we now set the number of direct pairs and iterative coupling pairs for the phasing below
            groupPairsHandle = new SetPairTypeCountJob
            {
                DispatchPairs = sortedPairs,
                PhasedDispatchPairs = unsortedPairs,
                IterativePairsStreamPrefixSum = iterativePairsStreamPrefixSum.AsDeferredJobArray(),
                CouplingPairsStreamPrefixSum = couplingPairsStreamPrefixSum.AsDeferredJobArray(),
                DirectPairsStreamPrefixSum = directPairsStreamPrefixSum.AsDeferredJobArray(),
                NumDirectPairs = numDirectPairs,
                NumCouplingPairs = numIterativeCouplingPairs
            }.ScheduleUnsafe(directSolverUsed, 1, groupPairsHandle);

            disposeHandles.Add(iterativePairsStreamPrefixSum.Dispose(groupPairsHandle));
            disposeHandles.Add(couplingPairsStreamPrefixSum.Dispose(groupPairsHandle));
            disposeHandles.Add(directPairsStreamPrefixSum.Dispose(groupPairsHandle));

            // phasing:
            JobHandle phaseIterativePairsHandle = new CreateDispatchPairPhasesJob
            {
                ProcessedPairType = CreateDispatchPairPhasesJob.PairType.Iterative,
                DispatchPairs = sortedPairs.AsDeferredJobArray(),
                SolverSchedulerInfo = solverSchedulerInfo,
                NumDynamicBodies = stepInput.World.NumDynamicBodies,
                PhasedDispatchPairs = unsortedPairs.AsDeferredJobArray(),
                NumDirectPairs = numDirectPairs,
                NumCouplingPairs = numIterativeCouplingPairs
            }.Schedule(groupPairsHandle);

            JobHandle phaseCouplingPairsHandle = new CreateDispatchPairPhasesJobDefer
            {
                ProcessedPairType = CreateDispatchPairPhasesJob.PairType.Coupling,
                DispatchPairs = sortedPairs.AsDeferredJobArray(),
                SolverSchedulerInfo = solverSchedulerInfo,
                NumDynamicBodies = stepInput.World.NumDynamicBodies,
                PhasedDispatchPairs = unsortedPairs.AsDeferredJobArray(),
                NumDirectPairs = numDirectPairs,
                NumCouplingPairs = numIterativeCouplingPairs
            }.ScheduleUnsafe(directSolverUsed, 1, groupPairsHandle); // skip if direct solver not used

            JobHandle phaseDirectPairsHandle = new CreateDispatchPairPhasesJobDefer
            {
                ProcessedPairType = CreateDispatchPairPhasesJob.PairType.Direct,
                DispatchPairs = sortedPairs.AsDeferredJobArray(),
                SolverSchedulerInfo = solverSchedulerInfo,
                NumDynamicBodies = stepInput.World.NumDynamicBodies,
                PhasedDispatchPairs = unsortedPairs.AsDeferredJobArray(),
                NumDirectPairs = numDirectPairs,
                NumCouplingPairs = numIterativeCouplingPairs
            }.ScheduleUnsafe(directSolverUsed, 1, groupPairsHandle); // skip if direct solver not used

            // island search:
            JobHandle directSolverIslandsHandle = new FindDirectSolverIslandsJob
            {
                DispatchPairs = sortedPairs.AsDeferredJobArray(),
                DirectSolverSchedulerInfo = solverSchedulerInfo.DirectPairsDirectScheduling,
                NumDirectPairs = numDirectPairs,
                NumDynamicBodies = stepInput.World.NumDynamicBodies
            }.ScheduleUnsafe(directSolverUsed, 1, groupPairsHandle); // skip if direct solver not used

            var phasedDispatchPairsHandle = JobHandle.CombineDependencies(phaseIterativePairsHandle,
                phaseCouplingPairsHandle, phaseDirectPairsHandle);

            // Finalize iterative work item info
            phasedDispatchPairsHandle = new FinalizeIterativeWorkItemInfoJob
            {
                SolverSchedulerInfo = solverSchedulerInfo
            }.Schedule(phasedDispatchPairsHandle);

            var schedulerHandle = JobHandle.CombineDependencies(phasedDispatchPairsHandle, directSolverIslandsHandle);
            returnHandles.FinalExecutionHandle = schedulerHandle;

            // Finalize disposing
            disposeHandles.Add(directSolverUsed.Dispose(schedulerHandle));
            disposeHandles.Add(sortedPairs.Dispose(schedulerHandle));
            disposeHandles.Add(numDirectPairs.Dispose(schedulerHandle));
            disposeHandles.Add(numIterativeCouplingPairs.Dispose(schedulerHandle));

            returnHandles.FinalDisposeHandle = JobHandle.CombineDependencies(disposeHandles.AsArray());

            return returnHandles;
        }

        #region Helpers

        /// <summary>
        /// Helper function to schedule jobs to sort an array of dispatch pairs. The first single
        /// threaded job is a single pass Radix sort on the bits associated with DispatchPair.BodyIndexA,
        /// resulting in sub arrays with the same bodyA index. The second parallel job dispatches default
        /// sorts on each sub array.
        /// </summary>
        static JobHandle ScheduleSortJob(
            int numBodies,
            NativeList<DispatchPair> unsortedPairsIn,
            NativeList<DispatchPair> sortedPairsOut,
            JobHandle inputDependencies)
        {
#if SERIAL_RADIX_SORT   // Serial Radix Sort with Parallel Bucket Sort:
            var histogram = new NativeArray<int>(numBodies + 1, Allocator.TempJob);

            // Calculate number digits needed to encode all body indices,
            // which corresponds to the position of the most significant bit.
            int maxBodyIndex = numBodies - 1;

            // Perform single pass of single threaded radix sort.
            inputDependencies = new RadixSortPerBodyAJob
            {
                InputArray = unsortedPairsIn.AsDeferredJobArray(),
                OutputArray = sortedPairsOut.AsDeferredJobArray(),
                MaxIndex = maxBodyIndex,
                BodyIndexHistogram = histogram
            }.Schedule(inputDependencies);

            // Sort sub arrays with default sort.
            int numPerBatch = math.max(1, maxBodyIndex / 32);

            inputDependencies = new SortSubArraysJob
            {
                InOutArray = sortedPairsOut.AsDeferredJobArray(),
                NextElementIndex = histogram
            }.Schedule(histogram.Length, numPerBatch, inputDependencies);

            return inputDependencies;

#else       // Parallel Radix Sort with Parallel Bucket Sort:

            // In the first phase, we keep some workers available for other tasks that are still happening
            // from the previous stages in the pipeline, such as container disposal.
            int numAvailableWorkers = (int)(JobsUtility.JobWorkerCount * 0.2f);
            var numWorkersPhase1 = math.max(1, JobsUtility.JobWorkerCount - numAvailableWorkers);

            var histogramLength = numBodies + 1;
            var sortKeyHistogram = new NativeArray<int>(histogramLength, Allocator.TempJob);

            // Calculate number digits needed to encode all body indices,
            // which corresponds to the position of the most significant bit.
            int maxBodyIndex = numBodies - 1;

            // Perform radix sort in parallel using bodyA index as sort key:

            var sortHandle = new RadixSortHistogramJob
            {
                DispatchPairs = unsortedPairsIn.AsDeferredJobArray(),
                SortKeyHistogram = sortKeyHistogram,
                NumWorkers = numWorkersPhase1
            }.Schedule(numWorkersPhase1, 1, inputDependencies);

            var sortKeyPrefixSum = sortKeyHistogram;
            sortHandle = new RadixSortPrefixSumJob
            {
                SortKeyPrefixSum = sortKeyPrefixSum,
            }.Schedule(sortHandle);

            var numWorkersPhase2 = math.max(1, JobsUtility.JobWorkerCount);
            sortHandle = new RadixSortGatherJob
            {
                InputDispatchPairs = unsortedPairsIn.AsDeferredJobArray(),
                OutputDispatchPairs = sortedPairsOut.AsDeferredJobArray(),
                SortKeyPrefixSum = sortKeyPrefixSum,
                NumWorkers = numWorkersPhase2
            }.Schedule(numWorkersPhase2, 1, sortHandle);

            // Sort sub arrays by bodyB index and remaining dispatch pair values with default sort:

            int numPerBatch = math.max(1, maxBodyIndex / 32);

            // next element index array corresponds to the prefix sum of the last worker
            var nextElementIndex = sortKeyPrefixSum;
            sortHandle = new SortSubArraysJob
            {
                InOutArray = sortedPairsOut.AsDeferredJobArray(),
                NextElementIndex = nextElementIndex
            }.Schedule(nextElementIndex.Length, numPerBatch, sortHandle);

            return sortHandle;
#endif
        }

        #endregion

        #region Jobs

        [BurstCompile]
        struct FinalizeIterativeWorkItemInfoJob : IJob
        {
            [NativeDisableContainerSafetyRestriction]
            public SolverSchedulerInfo SolverSchedulerInfo;

            public void Execute()
            {
                SolverSchedulerInfo.FinalizeIterativeWorkItemInfo();
            }
        }

        [BurstCompile]
        struct ForceDirectSolverOnAllPairsJob : IJobParallelForDefer
        {
            [NativeDisableParallelForRestriction]
            public NativeList<DispatchPair> DispatchPairs;

            public void Execute(int index)
            {
                var pair = DispatchPairs[index];
                pair.SolverType = SolverType.Direct;
                DispatchPairs[index] = pair;
            }
        }

        [BurstCompile]
        struct EnableDirectSolverPairProcessing : IJob
        {
            [ReadOnly]
            public NativeArray<Joint> Joints;
            [ReadOnly]
            public NativeStream DynamicVsDynamicBroadphaseStream;
            [ReadOnly]
            public NativeStream StaticVsDynamicBroadphaseStream;
            [ReadOnly]
            public NativeReference<bool>.ReadOnly DirectSolverEnabledFlag;
            [WriteOnly]
            public NativeReference<int> NumJointsToProcess;
            [WriteOnly]
            public NativeReference<int> NumDynamicVsDynamicBroadphaseStreamsToProcess;
            [WriteOnly]
            public NativeReference<int> NumStaticVsDynamicBroadphaseStreamsToProcess;

            public void Execute()
            {
                if (DirectSolverEnabledFlag.Value == false)
                {
                    NumJointsToProcess.Value = 0;
                    NumDynamicVsDynamicBroadphaseStreamsToProcess.Value = 0;
                    NumStaticVsDynamicBroadphaseStreamsToProcess.Value = 0;
                    return;
                }
                // else:

                NumJointsToProcess.Value = Joints.Length;
                NumDynamicVsDynamicBroadphaseStreamsToProcess.Value = DynamicVsDynamicBroadphaseStream.ForEachCount;
                NumStaticVsDynamicBroadphaseStreamsToProcess.Value = StaticVsDynamicBroadphaseStream.ForEachCount;
            }
        }

        [BurstCompile]
        struct SetDirectSolverBodyFlagsForJointsJob : IJobParallelForDefer
        {
            [ReadOnly]
            public SimulationStepInput StepInput;

            [WriteOnly, NativeDisableContainerSafetyRestriction]
            public NativeArray<bool> DirectSolverBodyFlags;

            [WriteOnly, NativeDisableContainerSafetyRestriction]
            public NativeReference<int> DirectSolverUsed;

            public void Execute(int index)
            {
                var joint = StepInput.World.Joints[index];
                var numDynamicBodies = StepInput.World.NumDynamicBodies;
                if (joint.SolverType == SolverType.Direct)
                {
                    var bodyPair = joint.BodyPair;
                    var directSolverUsed = false;
                    if (bodyPair.IsValid)
                    {
                        if (bodyPair.BodyIndexA < numDynamicBodies)
                        {
                            DirectSolverBodyFlags[bodyPair.BodyIndexA] = true;
                            directSolverUsed = true;
                        }

                        if (bodyPair.BodyIndexB < numDynamicBodies)
                        {
                            DirectSolverBodyFlags[bodyPair.BodyIndexB] = true;
                            directSolverUsed = true;
                        }
                    }

                    if (directSolverUsed)
                    {
                        DirectSolverUsed.Value = 1;
                    }
                }
            }
        }

        [BurstCompile]
        struct SetDirectSolverBodyFlagsForBroadphasePairsJob : IJobParallelForDefer
        {
            public NativeStream.Reader OverlapResultsReader;

            [WriteOnly, NativeDisableContainerSafetyRestriction]
            public NativeArray<bool> DirectSolverBodyFlags;

            [WriteOnly, NativeDisableContainerSafetyRestriction]
            public NativeReference<int> DirectSolverUsed;

            [ReadOnly]
            public NativeReference<bool>.ReadOnly DirectSolverEnabledFlag;

            public bool StaticDynamicOverlaps;

            public void Execute(int index)
            {
                if (DirectSolverEnabledFlag.Value == false)
                {
                    // Early out if direct solver not enabled
                    return;
                }

                OverlapResultsReader.BeginForEachIndex(index);
                var rangeItemCount = OverlapResultsReader.RemainingItemCount;
                var directSolverUsed = false;

                // @todo direct solver: note that here we might flag the body as being solved with direct despite
                // not knowing yet whether this collision pair will actually lead to a contact being created, or it
                // becoming disabled due to the joint "Enable Collision" feature.
                // Can this cause issues? We might end up flagging iterative constraints as coupling constraints
                // despite this being not the case.

                if (StaticDynamicOverlaps) // only static / dynamic overlaps, i.e., one body is guaranteed to be static and the other dynamic.
                {
                    for (int j = 0; j < rangeItemCount; j++)
                    {
                        var overlapResult = OverlapResultsReader.Read<Broadphase.OverlapResult>();
                        if (overlapResult.SolverType == SolverType.Direct)
                        {
                            // Note: We only need to flag the single dynamic body here for direct solving, since static bodies
                            // can't move by design and are unaffected by the solver.
                            var dynamicBodyIndex = math.min(overlapResult.BodyPair.BodyIndexA, overlapResult.BodyPair.BodyIndexB);
                            DirectSolverBodyFlags[dynamicBodyIndex] = true;
                            directSolverUsed = true;
                        }
                    }
                }
                else // only dynamic / dynamic overlaps
                {
                    for (int j = 0; j < rangeItemCount; j++)
                    {
                        var overlapResult = OverlapResultsReader.Read<Broadphase.OverlapResult>();
                        if (overlapResult.SolverType == SolverType.Direct)
                        {
                            // Note: no need to check if bodies are dynamic, since the overlap necessarily contains
                            // two dynamic bodies.
                            DirectSolverBodyFlags[overlapResult.BodyPair.BodyIndexA] = true;
                            DirectSolverBodyFlags[overlapResult.BodyPair.BodyIndexB] = true;
                            directSolverUsed = true;
                        }
                    }
                }

                if (directSolverUsed)
                {
                    DirectSolverUsed.Value = 1;
                }

                OverlapResultsReader.EndForEachIndex();
            }
        }

        [BurstCompile]
        struct EnableCouplingDetectionAndPairGroupingJob : IJob
        {
            [ReadOnly]
            public SimulationStepInput StepInput;

            [ReadOnly]
            public NativeArray<DispatchPair> DispatchPairs;

            [WriteOnly]
            public NativeReference<int> NumPairsToProcessForCouplingDetection;

            [ReadOnly]
            public NativeReference<int>.ReadOnly DirectSolverUsed;

            [WriteOnly]
            public NativeReference<int> NumWorkersForPairGrouping;

            public int NumWorkers;

            public void Execute()
            {
                if (StepInput.DirectSolverSettings.ForceDirectSolver)
                {
                    // Direct solver forced on for everything. No coupling detection needed, but
                    // pair grouping required.
                    NumPairsToProcessForCouplingDetection.Value = 0;
                    NumWorkersForPairGrouping.Value = NumWorkers;
                    return;
                }
                // else:

                if (DirectSolverUsed.Value == 1)
                {
                    NumPairsToProcessForCouplingDetection.Value = DispatchPairs.Length;
                    NumWorkersForPairGrouping.Value = NumWorkers;
                }
                else
                {
                    NumPairsToProcessForCouplingDetection.Value = 0;
                    NumWorkersForPairGrouping.Value = 0;
                }
            }
        }

        [BurstCompile]
        struct DetectCouplingConstraintsJob : IJobParallelForDefer
        {
            [WriteOnly, NativeDisableContainerSafetyRestriction]
            public NativeArray<DispatchPair> DispatchPairs;

            [ReadOnly]
            public NativeArray<bool> DirectSolverBodyFlags;

            public void Execute(int index)
            {
                var pair = DispatchPairs[index];
                if (pair.SolverType == SolverType.Iterative)
                {
                    // Check if either body is incident to a direct solver pair
                    if ((pair.BodyIndexA < DirectSolverBodyFlags.Length && DirectSolverBodyFlags[pair.BodyIndexA]) ||
                        (pair.BodyIndexB < DirectSolverBodyFlags.Length && DirectSolverBodyFlags[pair.BodyIndexB]))
                    {
                        pair.EnableCoupling();
                        DispatchPairs[index] = pair;
                    }
                }
            }
        }

        [BurstCompile]
        struct SetDisableCollisionFlagsJob : IJobParallelFor
        {
            [NativeDisableParallelForRestriction]
            public NativeArray<DispatchPair> DispatchPairs;

            public int NumWorkers;

            public void Execute(int workerIndex)
            {
                if (DispatchPairs.Length == 0)
                {
                    return;
                }

                // We are parallelizing the joints' "Enable Collision" feature using a parallel-for job with a given number
                // of workers (e.g., one per job system worker) and starting each worker at different equally distributed
                // spots in the dispatch pair array. Starting at the beginning of their section, each worker first moves
                // forward in the array until the body pair indices change in order to find the beginning of the section it
                // is responsible for. Then, it starts the collision disabling operation.
                // The worker stops processing when it moves to the point beyond its associated section where again the
                // body pair index changes. This approach will make sure that each worker only processes sections of
                // dispatch pairs which begin and end with a new body pair, which is a core component in the way
                // the "Enable Collision" feature is implemented.

                var indicesPerWorker = (int)math.ceil(DispatchPairs.Length / (float)NumWorkers);
                var startIndex = workerIndex * indicesPerWorker;
                if (startIndex > DispatchPairs.Length - 1)
                {
                    // no work for this worker
                    return;
                }

                var endIndex = math.min(startIndex + indicesPerWorker, DispatchPairs.Length);

                // move to beginning of next valid body pair section in the sub-array for this worker
                if (startIndex != 0)
                {
                    var previousPair = DispatchPairs[startIndex - 1];
                    var startPair = DispatchPairs[startIndex];
                    while (startIndex < endIndex - 1 && startPair.BodyIndexA == previousPair.BodyIndexA && startPair.BodyIndexB == previousPair.BodyIndexB)
                    {
                        startPair = DispatchPairs[++startIndex];
                    }
                }

                // now, start processing the section for this worker
                int lastPairA = -1;
                int lastPairB = -1;

                bool contactsPermitted = true; // Will avoid creating a contact (=non-joint) pair if this is false

                for (int i = startIndex; i < DispatchPairs.Length; ++i)
                {
                    var pair = DispatchPairs[i];
                    int bodyIndexA = pair.BodyIndexA;
                    int bodyIndexB = pair.BodyIndexB;

                    bool indicesChanged = !(lastPairA == bodyIndexA && lastPairB == bodyIndexB);

                    if (i >= endIndex && indicesChanged)
                    {
                        // We reached the end of the section for this worker and an index change occurred.
                        // Stop!
                        break;
                    }

                    bool isJoint = pair.IsJoint;

                    if (indicesChanged || contactsPermitted || isJoint)
                    {
                        // If _any_ Joint between each Pair has Enable Collision set to true, then contacts will be permitted.
                        // Warning: For this to work, in a processed, continuous sequence of dispatch pairs with a given
                        // fixed body pair, joints must always appear before contacts for this implementation to work correctly.
                        // The DispatchPairs in the incoming pre-sorted dispatch pair array (dispatchPairs) is organized and sorted
                        // in the scheduler accordingly before this function is called.
                        bool thisPermitsContacts = isJoint && pair.IsCollisionEnabled;
                        contactsPermitted = (contactsPermitted && !indicesChanged) || thisPermitsContacts;

                        lastPairA = bodyIndexA;
                        lastPairB = bodyIndexB;
                    }
                    else
                    {
                        // Joints don't allow collision for this pair. Disable.
                        pair.DisableCollision();
                        DispatchPairs[i] = pair;
                    }
                }
            }
        }

        [BurstCompile]
        struct AllocateDispatchPairsJob : IJob
        {
            [ReadOnly]
            public NativeArray<Joint> Joints;

            [ReadOnly]
            public NativeList<int> DynamicVsDynamicStreamPrefixSum;

            [ReadOnly]
            public NativeList<int> StaticVsDynamicStreamPrefixSum;

            [WriteOnly]
            public NativeList<DispatchPair> UnsortedDispatchPairs;

            [WriteOnly]
            public NativeList<DispatchPair> SortedDispatchPairs;

            public void Execute()
            {
                var numDynamicVsDynamicOverlapResults = DynamicVsDynamicStreamPrefixSum[^ 1];
                var numStaticVsDynamicOverlapResults = StaticVsDynamicStreamPrefixSum[^ 1];

                var numDispatchPairsConservative =
                    numDynamicVsDynamicOverlapResults +
                    numStaticVsDynamicOverlapResults +
                    Joints.Length;

                if (numDispatchPairsConservative == 0)
                {
                    return;
                }

                UnsortedDispatchPairs.ResizeUninitialized(numDispatchPairsConservative);
                SortedDispatchPairs.ResizeUninitialized(numDispatchPairsConservative);
            }
        }

        [BurstCompile]
        struct ShrinkDispatchPairListJob : IJob
        {
            [WriteOnly]
            public NativeList<DispatchPair> UnsortedDispatchPairs;

            [WriteOnly]
            public NativeList<DispatchPair> SortedDispatchPairs;

            [ReadOnly]
            public NativeArray<int> JointStreamPrefixSum;

            [ReadOnly]
            public NativeArray<int> DynamicVsDynamicStreamPrefixSum;

            [ReadOnly]
            public NativeArray<int> StaticVsDynamicStreamPrefixSum;

            public void Execute()
            {
                var numDispatchPairs = JointStreamPrefixSum[^ 1]
                    + DynamicVsDynamicStreamPrefixSum[^ 1]
                    + StaticVsDynamicStreamPrefixSum[^ 1];

                UnsortedDispatchPairs.ResizeUninitialized(numDispatchPairs);
                SortedDispatchPairs.ResizeUninitialized(numDispatchPairs);
            }
        }


        [BurstCompile]
        struct CreateJointDispatchPairsJob : IJobParallelForDefer
        {
            [ReadOnly]
            public NativeArray<Joint> Joints;

            public NativeStream.Writer JointDispatchPairWriter;

            public int NumWorkers;

            public void Execute(int workerIndex)
            {
                if (Joints.Length == 0)
                {
                    return;
                }

                var indicesPerWorker = (int)math.ceil(Joints.Length / (float)NumWorkers);
                var startIndex = workerIndex * indicesPerWorker;
                if (startIndex > Joints.Length - 1)
                {
                    // no work for this worker
                    return;
                }

                var endIndex = math.min(startIndex + indicesPerWorker, Joints.Length);

                JointDispatchPairWriter.BeginForEachIndex(workerIndex);

                for (int i = startIndex; i < endIndex; i++)
                {
                    var joint = Joints[i];
                    if (joint.BodyPair.IsValid)
                    {
                        // Note: Since only valid joints are commited to the dispatch pair stream the joint indices will
                        // have some gaps, but that is alright, since the joint index is only used as a key for sorting
                        // of the dispatch pairs.
                        JointDispatchPairWriter.Write(DispatchPair.CreateJoint(joint.BodyPair, i, joint.EnableCollision, joint.SolverType));
                    }
                }

                JointDispatchPairWriter.EndForEachIndex();
            }
        }

        [BurstCompile]
        struct CreateStreamPrefixSumJob : IJob
        {
            public NativeStream.Reader Reader;

            public NativeList<int> StreamPrefixSum; // Note: can not be marked [WriteOnly] due to use of NativeList<T>.Length (via end expression "^ 1")

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void CreateStreamPrefixSum(NativeStream.Reader reader, NativeList<int> streamPrefixSum)
            {
                streamPrefixSum.ResizeUninitialized(reader.ForEachCount + 1); // +1 for total count entry at the end

                int totalCount = 0;
                for (int i = 0; i < reader.ForEachCount; i++)
                {
                    streamPrefixSum[i] = totalCount;
                    totalCount += reader.BeginForEachIndex(i);
                }

                // store total stream element count at the end of the histogram
                streamPrefixSum[^ 1] = totalCount;
            }

            public void Execute()
            {
                CreateStreamPrefixSum(Reader, StreamPrefixSum);
            }
        }

        /// <summary>
        /// Deferred version of CreateStreamPrefixSumJob used to skip execution if job is unnecessary.
        /// Only ever launched with a single worker.
        /// </summary>
        [BurstCompile]
        struct CreateStreamPrefixSumJobDefer : IJobParallelForDefer
        {
            public NativeStream.Reader Reader;

            [NativeDisableParallelForRestriction]
            public NativeList<int> StreamPrefixSum; // Note: can not be marked [WriteOnly] due to use of NativeList<T>.Length (via end expression "^ 1")

            public void Execute(int workerIndex)
            {
                // Note: workerIndex should always be 0, since we only derive from IJobParallelForDefer to skip execution
                // of this job if not required, i.e., it is either scheduled with a single worker or with no worker.
                SafetyChecks.CheckAreEqualAndThrow(0, workerIndex);

                CreateStreamPrefixSumJob.CreateStreamPrefixSum(Reader, StreamPrefixSum);
            }
        }

        [BurstCompile]
        struct MergeJointDispatchPairsStreamJob : IJobParallelForDefer
        {
            [WriteOnly, NativeDisableContainerSafetyRestriction]
            public NativeList<DispatchPair> DispatchPairs;

            public NativeStream.Reader JointDispatchPairReader;

            [ReadOnly]
            public NativeArray<int> JointDispatchPairStreamHistogram;
            [ReadOnly]
            public NativeArray<int> DynamicVsDynamicStreamHistogram;
            [ReadOnly]
            public NativeArray<int> StaticVsDynamicStreamHistogram;

            public void Execute(int index)
            {
                // calculate index for first dispatch pair in this worker, considering that the joint pairs are
                // located after the collision pairs in the dispatch pair array.
                var currentPairIndex = JointDispatchPairStreamHistogram[index]
                    + DynamicVsDynamicStreamHistogram[^ 1] + StaticVsDynamicStreamHistogram[^ 1];

                var jointPairs = JointDispatchPairReader.BeginForEachIndex(index);
                for (int j = 0; j < jointPairs; ++j)
                {
                    var jointPair = JointDispatchPairReader.Read<DispatchPair>();
                    DispatchPairs[currentPairIndex++] = jointPair;
                }
                JointDispatchPairReader.EndForEachIndex();
            }
        }

        [BurstCompile]
        struct CreateCollisionDispatchPairsJob : IJobParallelForDefer
        {
            [WriteOnly, NativeDisableContainerSafetyRestriction]
            public NativeArray<DispatchPair> DispatchPairs;

            [NativeDisableContainerSafetyRestriction] // to allow default value here
            public NativeArray<int> PrecedingStreamHistogram;

            [ReadOnly]
            public NativeArray<int> OverlapResultsStreamHistogram;

            public NativeStream.Reader OverlapResultsReader;

            public void Execute(int index)
            {
                // calculate index for first dispatch pair in this worker, considering that there might be a stream of
                // dispatch pairs in front with the number of pairs provided by the corresponding stream's
                // histogram (see PrecedingStreamHistogram).
                var currentPairIndex = OverlapResultsStreamHistogram[index];
                if (PrecedingStreamHistogram.IsCreated)
                {
                    currentPairIndex += PrecedingStreamHistogram[^ 1];
                }

                var overlapResults = OverlapResultsReader.BeginForEachIndex(index);
                for (int j = 0; j < overlapResults; ++j)
                {
                    var overlapResult = OverlapResultsReader.Read<Broadphase.OverlapResult>();

                    DispatchPairs[currentPairIndex++] = DispatchPair.CreateCollisionPair(overlapResult);
                }
                OverlapResultsReader.EndForEachIndex();
            }
        }

        /// <summary>
        /// Accumulates dispatch pairs for joints (joint pairs) and broadphase overlaps (collision pairs) into a single unsorted list.
        /// Note: the resultant list does not follow any particular pair order.
        /// </summary>
        internal static void CreateUnsortedDispatchPairs(in SimulationStepInput stepInput,
            NativeStream dynamicVsDynamicPairs, NativeStream staticVsDynamicPairs,
            NativeList<DispatchPair> unsortedDispatchPairs, out int numIterativePairs, out int numDirectPairs)
        {
            // count the number of valid joints:
            var joints = stepInput.World.Joints;

            int numValidJoints = 0;
            numIterativePairs = 0;
            numDirectPairs = 0;
            for (int i = 0; i < joints.Length; i++)
            {
                var joint = joints[i];
                if (joint.BodyPair.IsValid)
                {
                    ++numValidJoints;

                    if (joint.SolverType == SolverType.Iterative)
                    {
                        ++numIterativePairs;
                    }
                }
            }

            int numDispatchPairs =
                dynamicVsDynamicPairs.Count() +
                staticVsDynamicPairs.Count() +
                numValidJoints;

            if (numDispatchPairs == 0)
            {
                return;
            }

            // create the collision pairs:

            var dynamicVsDynamicPairReader = dynamicVsDynamicPairs.AsReader();
            var staticVsDynamicPairReader = staticVsDynamicPairs.AsReader();

            int totalPairCounter = 0;

            unsortedDispatchPairs.ResizeUninitialized(numDispatchPairs);
            NativeArray<DispatchPair> pairs = unsortedDispatchPairs.AsArray();

            for (int i = 0; i < dynamicVsDynamicPairReader.ForEachCount; i++)
            {
                dynamicVsDynamicPairReader.BeginForEachIndex(i);
                int rangeItemCount = dynamicVsDynamicPairReader.RemainingItemCount;
                for (int j = 0; j < rangeItemCount; j++)
                {
                    var overlapResult = dynamicVsDynamicPairReader.Read<Broadphase.OverlapResult>();

                    pairs[totalPairCounter++] = DispatchPair.CreateCollisionPair(overlapResult);

                    if (overlapResult.SolverType == SolverType.Iterative)
                    {
                        ++numIterativePairs;
                    }
                }
                dynamicVsDynamicPairReader.EndForEachIndex();
            }

            for (int i = 0; i < staticVsDynamicPairReader.ForEachCount; i++)
            {
                staticVsDynamicPairReader.BeginForEachIndex(i);
                int rangeItemCount = staticVsDynamicPairReader.RemainingItemCount;
                for (int j = 0; j < rangeItemCount; j++)
                {
                    var overlapResult = staticVsDynamicPairReader.Read<Broadphase.OverlapResult>();

                    pairs[totalPairCounter++] = DispatchPair.CreateCollisionPair(overlapResult);

                    if (overlapResult.SolverType == SolverType.Iterative)
                    {
                        ++numIterativePairs;
                    }
                }
                staticVsDynamicPairReader.EndForEachIndex();
            }

            // now create the joint pairs:

            for (int i = 0; i < joints.Length; i++)
            {
                var joint = joints[i];
                if (joint.BodyPair.IsValid)
                {
                    pairs[totalPairCounter++] =
                        DispatchPair.CreateJoint(joints[i].BodyPair, i, joints[i].EnableCollision, joints[i].SolverType);
                }
            }

            numDirectPairs = numDispatchPairs - numIterativePairs;

            SafetyChecks.CheckAreEqualAndThrow(true, totalPairCounter == pairs.Length);
            SafetyChecks.CheckAreEqualAndThrow(true, numDirectPairs + numIterativePairs == pairs.Length);
        }

        /// <summary>
        /// Sets the direct solver body flags for all dynamic bodies that are adjacent to at least one
        /// joint or collision pair that is solved with the direct solver.
        /// </summary>
        internal static void SetDirectSolverBodyFlags(in SimulationStepInput stepInput,
            NativeStream dynamicVsDynamicPairs, NativeStream staticVsDynamicPairs,
            NativeArray<bool> directSolverBodyFlags)
        {
            int numDynamicBodies = stepInput.World.NumDynamicBodies;
            var joints = stepInput.World.Joints;

            SafetyChecks.CheckAreEqualAndThrow(true, directSolverBodyFlags.Length == numDynamicBodies);

            // set the direct solver body flags for joints
            for (int i = 0; i < joints.Length; i++)
            {
                var joint = joints[i];
                if (joint.BodyPair.IsValid && joint.SolverType == SolverType.Direct)
                {
                    if (joint.BodyPair.BodyIndexA < numDynamicBodies)
                    {
                        directSolverBodyFlags[joint.BodyPair.BodyIndexA] = true;
                    }

                    if (joint.BodyPair.BodyIndexB < numDynamicBodies)
                    {
                        directSolverBodyFlags[joint.BodyPair.BodyIndexB] = true;
                    }
                }
            }

            // set the direct solver body flags for the collision pairs:

            var dynamicVsDynamicPairReader = dynamicVsDynamicPairs.AsReader();
            var staticVsDynamicPairReader = staticVsDynamicPairs.AsReader();

            for (int i = 0; i < dynamicVsDynamicPairReader.ForEachCount; i++)
            {
                dynamicVsDynamicPairReader.BeginForEachIndex(i);
                int rangeItemCount = dynamicVsDynamicPairReader.RemainingItemCount;
                for (int j = 0; j < rangeItemCount; j++)
                {
                    var overlapResult = dynamicVsDynamicPairReader.Read<Broadphase.OverlapResult>();
                    if (overlapResult.SolverType == SolverType.Direct)
                    {
                        // @todo direct solver: note that here we might flag the body as being solved with direct despite
                        // not knowing yet whether this collision pair will actually lead to a contact being created, or it
                        // becoming disabled due to the joint "Enable Collision" feature.
                        // Can this cause issues? We might end up flagging iterative constraints as coupling constraints
                        // despite this being not the case.

                        // Note: no need to check if bodies are dynamic, since this pair comes from the
                        // dynamic vs dynamic broadphase pairs stream.
                        directSolverBodyFlags[overlapResult.BodyPair.BodyIndexA] = true;
                        directSolverBodyFlags[overlapResult.BodyPair.BodyIndexB] = true;
                    }
                }

                dynamicVsDynamicPairReader.EndForEachIndex();
            }

            for (int i = 0; i < staticVsDynamicPairReader.ForEachCount; i++)
            {
                staticVsDynamicPairReader.BeginForEachIndex(i);
                int rangeItemCount = staticVsDynamicPairReader.RemainingItemCount;
                for (int j = 0; j < rangeItemCount; j++)
                {
                    var overlapResult = staticVsDynamicPairReader.Read<Broadphase.OverlapResult>();
                    if (overlapResult.SolverType == SolverType.Direct)
                    {
                        // @todo direct solver: note that here we might flag the body as being solved with direct despite
                        // not knowing yet whether this collision pair will actually lead to a contact being created, or it
                        // becoming disabled due to the joint "Enable Collision" feature.
                        // Can this cause issues? We might end up flagging iterative constraints as coupling constraints
                        // despite this being not the case.

                        // Note: We only need to flag the single dynamic body index here for direct solving, since static bodies
                        // can't move by design and are unaffected by the solver.
                        var dynamicBodyIndex = math.min(overlapResult.BodyPair.BodyIndexA, overlapResult.BodyPair.BodyIndexB);
                        directSolverBodyFlags[dynamicBodyIndex] = true;
                    }
                }

                staticVsDynamicPairReader.EndForEachIndex();
            }
        }

        /// <summary>
        /// Enables coupling in all iteratively solved pairs which are adjacent to a body that itself is adjacent
        /// to a pair that is solved with the direct solver.
        /// </summary>
        internal static void SetCouplingConstraintFlags(NativeList<DispatchPair> pairs,
            NativeArray<bool> directSolverBodyFlags, out int numIterativeCouplingPairs)
        {
            int numDynamicBodies = directSolverBodyFlags.Length;
            numIterativeCouplingPairs = 0;
            for (int i = 0; i < pairs.Length; ++i)
            {
                var pair = pairs[i];
                if (pair.SolverType == SolverType.Iterative &&
                    (
                        (pair.BodyIndexA < numDynamicBodies && directSolverBodyFlags[pair.BodyIndexA]) ||
                        (pair.BodyIndexB < numDynamicBodies && directSolverBodyFlags[pair.BodyIndexB])
                    )
                )
                {
                    pair.EnableCoupling();
                    ++numIterativeCouplingPairs;
                    pairs[i] = pair;
                }
            }
        }

        /// <summary>
        /// Goes through the provided lexicographically sorted dispatch pair array and disables collisions
        /// for all collision pairs for which all joints between the same body pair have the "Enable Collision"
        /// flag set to false.
        /// </summary>
        internal static void SetDisableCollisionFlags(NativeList<DispatchPair> pairs)
        {
            int lastPairA = -1;
            int lastPairB = -1;

            bool contactsPermitted = true; // Will avoid creating a contact (=non-joint) pair if this is false

            for (int i = 0; i < pairs.Length; i++)
            {
                var pair = pairs[i];
                int bodyIndexA = pair.BodyIndexA;
                int bodyIndexB = pair.BodyIndexB;

                bool indicesChanged = !(lastPairA == bodyIndexA && lastPairB == bodyIndexB);
                bool isJoint = pair.IsJoint;

                if (indicesChanged || contactsPermitted || isJoint)
                {
                    // If _any_ Joint between a given pair has Enable Collision set to true, then contacts will be permitted
                    // for this pair.
                    // Warning: For this to work, in a processed, continuous sequence of dispatch pairs with a given fixed
                    // body pair (sub-sequence of pairs with the same two bodies), joints must always appear before
                    // contacts for this implementation to work correctly.
                    // The DispatchPairs in the incoming pre-sorted dispatch pair array (dispatchPairs) is organized and
                    // sorted in the scheduler accordingly before this function is called (lexicographically sorted by
                    // first body A, then body B, then joints and then contacts).
                    bool thisPermitsContacts = isJoint && pair.IsCollisionEnabled;
                    contactsPermitted = (contactsPermitted && !indicesChanged) || thisPermitsContacts;

                    lastPairA = bodyIndexA;
                    lastPairB = bodyIndexB;
                }
                else // collision pair for which contacts are not permitted based on the "IsCollisionEnabled" flag in the preceding joint pairs.
                {
                    // Joints don't allow collision for this collision pair. Disable.
                    pair.DisableCollision();
                    pairs[i] = pair;
                }
            }
        }

        /// <summary>
        /// Combines body pairs and joint pairs into an unsorted array of dispatch pairs.
        /// </summary>
        [BurstCompile]
        struct CreateDispatchPairsJob : IJob
        {
            [ReadOnly] public SimulationStepInput StepInput;
            [ReadOnly] public NativeStream DynamicVsDynamicBroadphasePairsStream;
            [ReadOnly] public NativeStream StaticVsDynamicBroadphasePairsStream;
            public SolverSchedulerInfo SolverSchedulerInfo;
            public NativeList<DispatchPair> DispatchPairs;

            public void Execute()
            {
                CreateDispatchPairs(StepInput,
                    ref DynamicVsDynamicBroadphasePairsStream, ref StaticVsDynamicBroadphasePairsStream,
                    ref SolverSchedulerInfo, ref DispatchPairs);
            }
        }

        /// <summary>   Sorts an array of dispatch pairs by Body A index. </summary>
        [BurstCompile]
        internal struct RadixSortPerBodyAJob : IJob
        {
            [ReadOnly]
            public NativeArray<DispatchPair> InputArray;
            [NativeDisableParallelForRestriction]
            public NativeArray<DispatchPair> OutputArray;
            [NativeDisableParallelForRestriction]
            public NativeArray<int> BodyIndexHistogram;

            public int MaxIndex;

            public void Execute()
            {
                RadixSortPerBodyA(InputArray, OutputArray, BodyIndexHistogram, MaxIndex);
            }

            // Performs single pass of Radix sort on NativeArray<DispatchPair> based on BodyIndexA.
            public static void RadixSortPerBodyA(NativeArray<DispatchPair> inputArray, NativeArray<DispatchPair> outputArray,
                NativeArray<int> bodyIndexHistogram, int maxBodyIndex)
            {
                SafetyChecks.CheckAreEqualAndThrow(inputArray.Length, outputArray.Length);

                var length = inputArray.Length;

                // Count body indices
                for (int i = 0; i < length; i++)
                {
                    var bodyIndex = inputArray[i].BodyIndexA;
                    bodyIndexHistogram[bodyIndex]++;
                }

                // Calculate start index for each body index (prefix sum calculation)
                int prev = bodyIndexHistogram[0];
                bodyIndexHistogram[0] = 0;
                for (int i = 1; i <= maxBodyIndex; i++)
                {
                    int current = bodyIndexHistogram[i];
                    bodyIndexHistogram[i] = bodyIndexHistogram[i - 1] + prev;
                    prev = current;
                }

                // Copy elements into buckets based on body A index, that is, into contiguous target array sections
                // with the same body A index
                for (int i = 0; i < length; i++)
                {
                    var pair = inputArray[i];
                    var bodyIndex = pair.BodyIndexA;
                    int index = bodyIndexHistogram[bodyIndex]++;
                    outputArray[index] = pair;
                }
            }
        }

        [BurstCompile]
        struct RadixSortHistogramJob : IJobParallelFor
        {
            [ReadOnly, NativeDisableParallelForRestriction]
            public NativeArray<DispatchPair> DispatchPairs;

            [NativeDisableParallelForRestriction]
            public NativeArray<int> SortKeyHistogram;

            public int NumWorkers;

            public void Execute(int workerIndex)
            {
                var indicesPerWorker = (int)math.ceil(DispatchPairs.Length / (float)NumWorkers);
                var startIndex = workerIndex * indicesPerWorker;
                if (startIndex > DispatchPairs.Length - 1)
                {
                    // no work for this worker
                    return;
                }

                var endIndex = math.min(startIndex + indicesPerWorker, DispatchPairs.Length);
                var numIndices = endIndex - startIndex;
                var workerSubArray = DispatchPairs.GetSubArray(startIndex, numIndices);

                unsafe
                {
                    var histogramPtr = (int*)SortKeyHistogram.GetUnsafePtr();

                    // count keys in sub-array
                    for (int i = 0; i < numIndices; i++)
                    {
                        var bodyIndex = workerSubArray[i].BodyIndexA;
                        // @todo: in the future, to reduce the number of atomic operations and false sharing, and the
                        // resultant thread contention we can fill a per-job hash map with the count per body A index
                        // while going over the pair sub-array of this worker, and then do a single atomic add to the
                        // shared histogram per body A index at the end of this job's Execute function.
                        var counter = new UnsafeAtomicCounter32(histogramPtr + bodyIndex);
                        counter.Add(1);
                    }
                }
            }
        }

        [BurstCompile]
        struct RadixSortPrefixSumJob : IJob
        {
            [NativeDisableParallelForRestriction]
            public NativeArray<int> SortKeyPrefixSum;

            public void Execute()
            {
                var last = SortKeyPrefixSum[0];
                SortKeyPrefixSum[0] = 0;
                for (var i = 1; i < SortKeyPrefixSum.Length; ++i)
                {
                    var current = SortKeyPrefixSum[i];
                    SortKeyPrefixSum[i] = SortKeyPrefixSum[i - 1] + last;
                    last = current;
                }
            }
        }

        [BurstCompile]
        struct RadixSortGatherJob : IJobParallelFor
        {
            [ReadOnly, NativeDisableParallelForRestriction]
            public NativeArray<DispatchPair> InputDispatchPairs;

            [WriteOnly, NativeDisableParallelForRestriction]
            public NativeArray<DispatchPair> OutputDispatchPairs;

            [NativeDisableParallelForRestriction]
            public NativeArray<int> SortKeyPrefixSum;

            public int NumWorkers;

            public void Execute(int workerIndex)
            {
                var indicesPerWorker = (int)math.ceil(InputDispatchPairs.Length / (float)NumWorkers);
                var startIndex = workerIndex * indicesPerWorker;
                if (startIndex > InputDispatchPairs.Length - 1)
                {
                    // no work for this worker
                    return;
                }
                var endIndex = math.min(startIndex + indicesPerWorker, InputDispatchPairs.Length);
                var numIndices = endIndex - startIndex;
                var workerSubArray = InputDispatchPairs.GetSubArray(startIndex, numIndices);

                unsafe
                {
                    var prefixSumPtr = (int*)SortKeyPrefixSum.GetUnsafePtr();

                    // Copy elements into buckets based on bodyA index
                    for (int i = 0; i < numIndices; i++)
                    {
                        var pair = workerSubArray[i];
                        var bodyIndex = pair.BodyIndexA;
                        var counter = new UnsafeAtomicCounter32(prefixSumPtr + bodyIndex);
                        var index = counter.Add(1);
                        OutputDispatchPairs[index] = pair;
                    }
                }
            }
        }

        /// <summary>   Sorts slices of an array in parallel. </summary>
        [BurstCompile]
        internal struct SortSubArraysJob : IJobParallelFor
        {
            [NativeDisableParallelForRestriction]
            public NativeArray<DispatchPair> InOutArray;

            // Array used to locate the beginning of the next pair sub-array section with identical body A index.
            // Note: After completion of RadixSortPerBodyAJob, RadixSortPerBodyAJob.BodyIndexHistogram is the prefix sum
            // shifted by one element to the left, that is, RadixSortPerBodyAJob.BodyIndexHistogram[i] = array index of
            // the first pair with body A index == i + 1.
            [ReadOnly, NativeDisableParallelForRestriction, DeallocateOnJobCompletion]
            public NativeArray<int> NextElementIndex;

            public void Execute(int workItemIndex)
            {
                int startIndex = 0;
                if (workItemIndex > 0)
                {
                    startIndex = NextElementIndex[workItemIndex - 1];
                }

                if (startIndex < InOutArray.Length)
                {
                    int length = NextElementIndex[workItemIndex] - startIndex;
                    DefaultSortOfSubArrays(InOutArray, startIndex, length);
                }
            }

            struct BodyIndexASubArrayComparer : IComparer<DispatchPair>
            {
                public int Compare(DispatchPair x, DispatchPair y)
                {
                    return x.BodyIndexASubArraySortKey.CompareTo(y.BodyIndexASubArraySortKey);
                }
            }

            // Sorts sub array by BodyIndexB and remaining data (joint etc.) using default sort
            static void DefaultSortOfSubArrays(NativeArray<DispatchPair> inOutArray, int startIndex, int length)
            {
                // inOutArray[startIndex] to inOutArray[startIndex + length - 1] have the same bodyA index
                // so we can do a simple sorting.
                if (length > 2)
                {
                    var subArray = new NativeSlice<DispatchPair>(inOutArray, startIndex, length);
                    NativeSortExtension.Sort(subArray, new BodyIndexASubArrayComparer());
                }
                else if (length == 2)
                {
                    if (inOutArray[startIndex].BodyIndexASubArraySortKey > inOutArray[startIndex + 1].BodyIndexASubArraySortKey)
                    {
                        var temp = inOutArray[startIndex + 1];
                        inOutArray[startIndex + 1] = inOutArray[startIndex];
                        inOutArray[startIndex] = temp;
                    }
                }
            }
        }

        [BurstCompile]
        struct FindDirectSolverIslandsJob : IJobParallelForDefer
        {
            [ReadOnly]
            public NativeArray<DispatchPair> DispatchPairs;

            [NativeDisableParallelForRestriction]
            public DirectSolverSchedulerInfo DirectSolverSchedulerInfo;

            [ReadOnly]
            public NativeReference<int> NumDirectPairs;

            public int NumDynamicBodies;

            public void Execute(int workerIndex)
            {
                // Note: workerIndex should always be 0, since we only derive from IJobParallelForDefer to skip execution
                // of this job if not required, i.e., it is either scheduled with a single worker or with no worker.

                if (NumDirectPairs.Value == 0)
                {
                    // No direct solver pairs, nothing to do.
                    return;
                }

                var numPairs = DispatchPairs.Length;
                var numIterativePairs = numPairs - NumDirectPairs.Value;
                var directPairs = DispatchPairs.GetSubArray(numIterativePairs, NumDirectPairs.Value);

                DirectSolverSchedulerInfo.FirstDispatchPairIndex.Value = numIterativePairs;

                FindDirectSolverIslands(NumDynamicBodies, directPairs, ref DirectSolverSchedulerInfo);
            }
        }

        [BurstCompile]
        struct ScatterDispatchPairsByTypeJob : IJobParallelForDefer
        {
            [ReadOnly, NativeDisableParallelForRestriction]
            public NativeArray<DispatchPair> DispatchPairs;

            public NativeStream.Writer IterativePairsWriter;
            public NativeStream.Writer CouplingPairsWriter;
            public NativeStream.Writer DirectPairsWriter;

            public int NumWorkers;

            public void Execute(int workerIndex)
            {
                var indicesPerWorker = (int)math.ceil(DispatchPairs.Length / (float)NumWorkers);
                var startIndex = workerIndex * indicesPerWorker;
                if (startIndex > DispatchPairs.Length - 1)
                {
                    // no work for this worker
                    return;
                }

                var endIndex = math.min(startIndex + indicesPerWorker, DispatchPairs.Length);

                IterativePairsWriter.BeginForEachIndex(workerIndex);
                CouplingPairsWriter.BeginForEachIndex(workerIndex);
                DirectPairsWriter.BeginForEachIndex(workerIndex);

                for (int i = startIndex; i < endIndex; i++)
                {
                    var pair = DispatchPairs[i];
                    // exclude disabled collision pairs
                    if (!(pair.IsJoint || pair.IsCollisionEnabled))
                    {
                        continue;
                    }
                    // else:

                    switch (pair.SolverType)
                    {
                        case SolverType.Iterative:
                        {
                            if (pair.IsCouplingEnabled)
                            {
                                CouplingPairsWriter.Write(pair);
                            }
                            else
                            {
                                IterativePairsWriter.Write(pair);
                            }
                            break;
                        }
                        case SolverType.Direct:
                        {
                            DirectPairsWriter.Write(pair);
                            break;
                        }
                    }
                }

                IterativePairsWriter.EndForEachIndex();
                CouplingPairsWriter.EndForEachIndex();
                DirectPairsWriter.EndForEachIndex();
            }
        }

        [BurstCompile]
        struct GatherDispatchPairsByTypeJob : IJobParallelForDefer
        {
            [WriteOnly, NativeDisableContainerSafetyRestriction]
            public NativeArray<DispatchPair> DispatchPairs;

            public NativeStream.Reader IterativePairsReader;
            public NativeStream.Reader CouplingPairsReader;
            public NativeStream.Reader DirectPairsReader;

            [ReadOnly]
            public NativeArray<int> IterativePairsStreamPrefixSum;
            [ReadOnly]
            public NativeArray<int> CouplingPairsStreamPrefixSum;
            [ReadOnly]
            public NativeArray<int> DirectPairsStreamPrefixSum;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            void GatherPairs(int firstPairIndex, int workerIndex, NativeStream.Reader pairReader)
            {
                var currentPairIndex = firstPairIndex;
                var numPairs = pairReader.BeginForEachIndex(workerIndex);
                for (int j = 0; j < numPairs; ++j)
                {
                    var pair = pairReader.Read<DispatchPair>();
                    // exclude disabled collision pairs
                    if (pair.IsJoint || pair.IsCollisionEnabled)
                    {
                        DispatchPairs[currentPairIndex++] = pair;
                    }
                }
                pairReader.EndForEachIndex();
            }

            public void Execute(int workerIndex)
            {
                // gather purely iterative pairs:
                {
                    var firstPairIndex = IterativePairsStreamPrefixSum[workerIndex];
                    GatherPairs(firstPairIndex, workerIndex, IterativePairsReader);
                }

                var numPurelyIterativePairs = IterativePairsStreamPrefixSum[^ 1];

                // gather iterative coupling pairs:
                {
                    // calculate index for first dispatch pair in this worker, considering that the coupling pairs are
                    // located after the iterative pairs in the dispatch pair array.
                    var firstPairIndex = numPurelyIterativePairs + CouplingPairsStreamPrefixSum[workerIndex];
                    GatherPairs(firstPairIndex, workerIndex, CouplingPairsReader);
                }

                // gather direct pairs:
                {
                    // calculate index for first dispatch pair in this worker, considering that the direct pairs are
                    // located after the iterative and coupling pairs in the dispatch pair array.
                    var firstPairIndex = numPurelyIterativePairs + CouplingPairsStreamPrefixSum[^ 1] + DirectPairsStreamPrefixSum[workerIndex];
                    GatherPairs(firstPairIndex, workerIndex, DirectPairsReader);
                }
            }
        }

        [BurstCompile]
        struct SetPairTypeCountJob : IJobParallelForDefer // Uses parallel-for-defer to avoid job being processed if no work is needed (no direct pairs)
        {
            [NativeDisableContainerSafetyRestriction]
            public NativeList<DispatchPair> DispatchPairs;
            [NativeDisableContainerSafetyRestriction]
            public NativeList<DispatchPair> PhasedDispatchPairs;

            [ReadOnly]
            public NativeArray<int> IterativePairsStreamPrefixSum;
            [ReadOnly]
            public NativeArray<int> CouplingPairsStreamPrefixSum;
            [ReadOnly]
            public NativeArray<int> DirectPairsStreamPrefixSum;

            [WriteOnly, NativeDisableParallelForRestriction]
            public NativeReference<int> NumCouplingPairs;

            [WriteOnly, NativeDisableParallelForRestriction]
            public NativeReference<int> NumDirectPairs;

            public void Execute(int workerIndex)
            {
                // Note: will be launched with a single worker or no worker to prevent job launch if unnecessary
                SafetyChecks.CheckAreEqualAndThrow(0, workerIndex);

                var numPureIterativePairs = IterativePairsStreamPrefixSum[^ 1];
                var numCouplingPairs = CouplingPairsStreamPrefixSum[^ 1];
                var numDirectPairs = DirectPairsStreamPrefixSum[^ 1];

                // Note: number of pairs of each type present in the full dispatch pair array corresponds
                // to the last element of the corresponding prefix sum arrays, which are created if grouping is needed, that is,
                // if any direct solver pairs are present.
                NumCouplingPairs.Value = numCouplingPairs;
                NumDirectPairs.Value = numDirectPairs;

                // Set final dispatch pair array sizes
                var numPairsTotal = numPureIterativePairs + numCouplingPairs + numDirectPairs;
                DispatchPairs.ResizeUninitialized(numPairsTotal);
                PhasedDispatchPairs.ResizeUninitialized(numPairsTotal);
            }
        }

        /// <summary>   Creates phases based on sorted list of dispatch pairs. </summary>
        [BurstCompile]
        internal struct CreateDispatchPairPhasesJob : IJob
        {
            public enum PairType : byte
            {
                Iterative,
                Coupling,
                Direct
            }

            public PairType ProcessedPairType;

            [NativeDisableContainerSafetyRestriction]
            public NativeArray<DispatchPair> DispatchPairs;

            [NativeDisableContainerSafetyRestriction]
            public SolverSchedulerInfo SolverSchedulerInfo;

            [NativeDisableContainerSafetyRestriction]
            public NativeArray<DispatchPair> PhasedDispatchPairs;

            public NativeReference<int>.ReadOnly NumDirectPairs;
            public NativeReference<int>.ReadOnly NumCouplingPairs;

            public int NumDynamicBodies;

            // maximum number of dispatch pairs in a single batch, defining the maximum size of a work item
            internal const int kMaxBatchSize = 8;

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public static void ExecuteImpl(PairType processedPairType, NativeArray<DispatchPair> dispatchPairs,
                SolverSchedulerInfo solverSchedulerInfo, NativeArray<DispatchPair> phasedDispatchPairs,
                NativeReference<int>.ReadOnly numDirectPairs, NativeReference<int>.ReadOnly numCouplingPairs, int numDynamicBodies)
            {
                NativeArray<DispatchPair> dispatchPairSubArray = default;
                NativeArray<DispatchPair> phasedDispatchPairSubArray = default;
                IterativeSolverSchedulerInfo schedulerInfo = default;
                NativeArray<int> unphasedToPhasedDispatchPairMap = default;

                int startPairIndex = 0, numPairsToProcess = 0;
                var numPairs = dispatchPairs.Length;
                var numPurelyIterativePairs = numPairs - numDirectPairs.Value - numCouplingPairs.Value;

                switch (processedPairType)
                {
                    case PairType.Iterative:
                    {
                        startPairIndex = 0;
                        numPairsToProcess = numPurelyIterativePairs;
                        schedulerInfo = solverSchedulerInfo.IterativePairsIterativeScheduling;

                        break;
                    }
                    case PairType.Coupling:
                    {
                        startPairIndex = numPurelyIterativePairs;
                        numPairsToProcess = numCouplingPairs.Value;
                        schedulerInfo = solverSchedulerInfo.CouplingPairsIterativeScheduling;

                        break;
                    }
                    case PairType.Direct:
                    {
                        startPairIndex = numPurelyIterativePairs + numCouplingPairs.Value;
                        numPairsToProcess = numDirectPairs.Value;
                        schedulerInfo = solverSchedulerInfo.DirectPairsIterativeScheduling;

                        // Make sure we have sufficient space for the mapping between iterative and direct solver
                        solverSchedulerInfo.DirectPairsDirectScheduling.ResizeMaps(numDirectPairs.Value);

                        unphasedToPhasedDispatchPairMap = solverSchedulerInfo.DirectPairsDirectScheduling.UnphasedToPhasedDispatchPairMap.AsArray();
                        break;
                    }
                }

                if (numPairs == 0)
                {
                    // nothing to do.
                    return;
                }
                // else:

                dispatchPairSubArray = dispatchPairs.GetSubArray(startPairIndex, numPairsToProcess);
                phasedDispatchPairSubArray = phasedDispatchPairs.GetSubArray(startPairIndex, numPairsToProcess);

                CreateDispatchPairPhases(
                    dispatchPairSubArray, numDynamicBodies, phasedDispatchPairSubArray,
                    out int numActivePhases, out int numWorkItems, ref schedulerInfo.PhaseInfo, unphasedToPhasedDispatchPairMap);

                schedulerInfo.NumActivePhases[0] = numActivePhases;
                schedulerInfo.NumWorkItems[0] = numWorkItems;

                schedulerInfo.FirstDispatchPairIndex.Value = startPairIndex;
                schedulerInfo.NumDispatchPairs.Value = numPairsToProcess;
            }

            public void Execute()
            {
                ExecuteImpl(ProcessedPairType, DispatchPairs,
                    SolverSchedulerInfo, PhasedDispatchPairs, NumDirectPairs, NumCouplingPairs, NumDynamicBodies);
            }

            // @todo direct solver (DOTS-7639): optionally use this (old) version for faster processing in serial case if no direct solver in use
            internal static unsafe void CreateDispatchPairPhasesAndDisableCollision(
                NativeArray<DispatchPair> dispatchPairs, int numDynamicBodies,
                NativeArray<DispatchPair> phasedDispatchPairs, out int numActivePhases, out int numWorkItems,
                ref NativeArray<IterativeSolverSchedulerInfo.SolvePhaseInfo> phaseInfo)
            {
                const byte kUninitializedPair = byte.MaxValue;
                const byte kInvalidPhaseID = kUninitializedPair - 1;
                // make sure that we can fit all phase indices into a byte
                SafetyChecks.CheckAreEqualAndThrow(true, byte.MaxValue >= kMaxNumPhases);
                var phaseIdPerPair = new NativeArray<byte>(dispatchPairs.Length, Allocator.Temp, NativeArrayOptions.UninitializedMemory);
                for (int i = 0; i < dispatchPairs.Length; i++)
                {
                    phaseIdPerPair[i] = kUninitializedPair;
                }

                var rigidBodyMask = new NativeArray<ulong>(numDynamicBodies, Allocator.Temp);

                int lastPhaseIndex = kMaxNumPhases - 1;
                int* numPairsPerPhase = stackalloc int[kMaxNumPhases];
                // Note: we use the last phase for all pairs for which we could not find a phase for data parallel processing.
                // These bodies need to be processed sequentially.
                numPairsPerPhase[lastPhaseIndex] = 0;

                // Guaranteed not to be a real pair
                int lastPairA = 0;
                int lastPairB = 0;

                bool contactsPermitted = true; // Will avoid creating a contact (=non-joint) pair if this is false

                BatchInfo* batchInfos = stackalloc BatchInfo[lastPhaseIndex];
                for (int i = 0; i < lastPhaseIndex; i++)
                {
                    batchInfos[i].m_NumElements = 0;
                    batchInfos[i].m_PhaseMask = (ulong)1 << i;
                    batchInfos[i].m_NumBatchesProcessed = 0;
                }

                // Find phase for each dynamic-dynamic pair
                for (int i = 0; i < dispatchPairs.Length; i++)
                {
                    int bodyIndexA = dispatchPairs[i].BodyIndexA;
                    int bodyIndexB = dispatchPairs[i].BodyIndexB;

                    bool indicesChanged = !(lastPairA == bodyIndexA && lastPairB == bodyIndexB);
                    bool isJoint = dispatchPairs[i].IsJoint;

                    bool isBodyBStatic = bodyIndexB >= numDynamicBodies;

                    if (indicesChanged || contactsPermitted || isJoint)
                    {
                        // Skip dynamic-static pairs since we need to ensure that those are solved after all dynamic-dynamic pairs.
                        if (!isBodyBStatic)
                        {
                            int phaseIndex = FindFreePhaseDynamicDynamicPair(rigidBodyMask, bodyIndexA, bodyIndexB, lastPhaseIndex);
                            phaseIdPerPair[i] = (byte)phaseIndex;

                            if (phaseIndex != lastPhaseIndex)
                            {
                                batchInfos[phaseIndex].Add(rigidBodyMask, bodyIndexA, bodyIndexB);
                            }
                            else
                            {
                                // We ran out of phases. Put body in last phase.
                                rigidBodyMask[bodyIndexA] |= (ulong)1 << lastPhaseIndex;
                                numPairsPerPhase[lastPhaseIndex]++;
                            }
                        }

                        // If _any_ Joint between each Pair has Enable Collision set to true, then contacts will be
                        // permitted.
                        // Warning: For this to work, in a processed, continuous sequence of dispatch pairs with a given
                        // fixed body pair, joints must always appear before contacts for this implementation to work
                        // correctly. The DispatchPairs in the incoming pre-sorted dispatch pair array (dispatchPairs)
                        // is organized and sorted in the scheduler accordingly before this function is called.
                        bool thisPermitsContacts = isJoint && dispatchPairs[i].IsCollisionEnabled;
                        contactsPermitted = (contactsPermitted && !indicesChanged) || thisPermitsContacts;

                        lastPairA = bodyIndexA;
                        lastPairB = bodyIndexB;
                    }
                    else
                    {
                        phaseIdPerPair[i] = kInvalidPhaseID;
                    }
                }

                // Commit rigid bodies masks for all batches that aren't flushed.
                for (int i = 0; i < lastPhaseIndex; i++)
                {
                    batchInfos[i].CommitRigidBodyMasks(rigidBodyMask);
                }

                // Find phase for each dynamic-static pair.
                for (int pairIndex = 0; pairIndex < dispatchPairs.Length; pairIndex++)
                {
                    int bodyIndexB = dispatchPairs[pairIndex].BodyIndexB;

                    bool isBodyBStatic = bodyIndexB >= numDynamicBodies;
                    if (isBodyBStatic && phaseIdPerPair[pairIndex] == kUninitializedPair)
                    {
                        int bodyIndexA = dispatchPairs[pairIndex].BodyIndexA;

                        int phaseIndex = FindFreePhaseDynamicStaticPair(rigidBodyMask, bodyIndexA, lastPhaseIndex);
                        phaseIdPerPair[pairIndex] = (byte)phaseIndex;

                        rigidBodyMask[bodyIndexA] |= (ulong)1 << phaseIndex;
                        if (phaseIndex != lastPhaseIndex)
                        {
                            batchInfos[phaseIndex].m_NumElements++;
                        }
                        else
                        {
                            numPairsPerPhase[lastPhaseIndex]++;
                        }
                    }
                }

                for (int i = 0; i < lastPhaseIndex; i++)
                {
                    numPairsPerPhase[i] = batchInfos[i].m_NumBatchesProcessed * kMaxBatchSize + batchInfos[i].m_NumElements;
                }

                // Calculate phase start offset
                int* offsetInPhase = stackalloc int[kMaxNumPhases];
                offsetInPhase[0] = 0;
                for (int i = 1; i < kMaxNumPhases; i++)
                {
                    offsetInPhase[i] = offsetInPhase[i - 1] + numPairsPerPhase[i - 1];
                }

                // Populate PhasedDispatchPairsArray with dynamic-dynamic pairs
                for (int i = 0; i < dispatchPairs.Length; i++)
                {
                    int bodyIndexB = dispatchPairs[i].BodyIndexB;
                    bool isBodyBStatic = bodyIndexB >= numDynamicBodies;

                    if (!isBodyBStatic && phaseIdPerPair[i] != kInvalidPhaseID)
                    {
                        int phaseForPair = phaseIdPerPair[i];
                        int indexInArray = offsetInPhase[phaseForPair]++;
                        phasedDispatchPairs[indexInArray] = dispatchPairs[i];
                    }
                }

                // Populate PhasedDispatchPairsArray with dynamic-static pairs
                for (int i = 0; i < dispatchPairs.Length; i++)
                {
                    int bodyIndexB = dispatchPairs[i].BodyIndexB;
                    bool isBodyBStatic = bodyIndexB >= numDynamicBodies;
                    if (isBodyBStatic && phaseIdPerPair[i] != kInvalidPhaseID)
                    {
                        int phaseForPair = phaseIdPerPair[i];
                        int indexInArray = offsetInPhase[phaseForPair]++;
                        phasedDispatchPairs[indexInArray] = dispatchPairs[i];
                    }
                }

                // Populate SolvePhaseInfo for each solve phase
                int firstWorkItemIndex = 0;
                int numPairs = 0;
                numActivePhases = 0;
                for (int i = 0; i < kMaxNumPhases; i++)
                {
                    IterativeSolverSchedulerInfo.SolvePhaseInfo info;
                    info.DispatchPairCount = numPairsPerPhase[i];

                    if (info.DispatchPairCount == 0)
                    {
                        break;
                    }

                    info.BatchSize = math.min(kMaxBatchSize, info.DispatchPairCount);
                    info.NumWorkItems = (info.DispatchPairCount + info.BatchSize - 1) / info.BatchSize;
                    info.ContainsDuplicateIndices = i == lastPhaseIndex && info.NumWorkItems > 1;
                    info.FirstWorkItemIndexOffset = firstWorkItemIndex;
                    info.FirstDispatchPairOffset = numPairs;

                    firstWorkItemIndex += info.NumWorkItems;
                    numPairs += info.DispatchPairCount;

                    phaseInfo[i] = info;
                    numActivePhases++;
                }

                // Uncomment this code when testing scheduler
                //CheckIntegrity(phasedDispatchPairs, numDynamicBodies, ref phaseInfo);

                numWorkItems = firstWorkItemIndex;
            }

            internal static unsafe void CreateDispatchPairPhases(
                NativeArray<DispatchPair> dispatchPairs, int numDynamicBodies,
                NativeArray<DispatchPair> phasedDispatchPairs, out int numActivePhases, out int numWorkItems,
                ref NativeArray<IterativeSolverSchedulerInfo.SolvePhaseInfo> phaseInfo, NativeArray<int> unphasedToPhasedDispatchPairMap = default)
            {
                // make sure that we can fit all phase indices into a byte
                SafetyChecks.CheckAreEqualAndThrow(true, byte.MaxValue >= kMaxNumPhases);

                const byte kUninitializedPair = 0;
                var phaseIdPerPair = new NativeArray<byte>(dispatchPairs.Length, Allocator.Temp, NativeArrayOptions.ClearMemory);

                var rigidBodyMask = new NativeArray<ulong>(numDynamicBodies, Allocator.Temp);

                int lastPhaseIndex = kMaxNumPhases - 1;
                int* numPairsPerPhase = stackalloc int[kMaxNumPhases];
                // Note: we use the last phase for all pairs for which we could not find a phase for data parallel processing.
                // These bodies need to be processed sequentially.
                numPairsPerPhase[lastPhaseIndex] = 0;

                BatchInfo* batchInfos = stackalloc BatchInfo[lastPhaseIndex];
                for (int i = 0; i < lastPhaseIndex; i++)
                {
                    batchInfos[i].m_NumElements = 0;
                    batchInfos[i].m_PhaseMask = (ulong)1 << i;
                    batchInfos[i].m_NumBatchesProcessed = 0;
                }

                // Find phase for each dynamic-dynamic pair
                for (int i = 0; i < dispatchPairs.Length; i++)
                {
                    var dispatchPair = dispatchPairs[i];
                    int bodyIndexB = dispatchPair.BodyIndexB;
                    bool isBodyBStatic = bodyIndexB >= numDynamicBodies;

                    if (!isBodyBStatic && // skip dynamic-static pairs since we need to ensure that those are solved after all dynamic-dynamic pairs.
                        (dispatchPair.IsJoint || dispatchPair.IsCollisionEnabled)) // skip over disabled collision pairs
                    {
                        int bodyIndexA = dispatchPair.BodyIndexA;

                        int phaseIndex = FindFreePhaseDynamicDynamicPair(rigidBodyMask, bodyIndexA, bodyIndexB, lastPhaseIndex);
                        phaseIdPerPair[i] = (byte)(phaseIndex + 1); // +1 since kUninitializedPair == 0

                        if (phaseIndex != lastPhaseIndex)
                        {
                            batchInfos[phaseIndex].Add(rigidBodyMask, bodyIndexA, bodyIndexB);
                        }
                        else
                        {
                            // We ran out of phases. Put body in last phase.
                            rigidBodyMask[bodyIndexA] |= (ulong)1 << lastPhaseIndex;
                            numPairsPerPhase[lastPhaseIndex]++;
                        }
                    }
                }

                // Commit rigid bodies masks for all batches that aren't flushed.
                for (int i = 0; i < lastPhaseIndex; i++)
                {
                    batchInfos[i].CommitRigidBodyMasks(rigidBodyMask);
                }

                // Find phase for each dynamic-static pair.
                for (int pairIndex = 0; pairIndex < dispatchPairs.Length; pairIndex++)
                {
                    var dispatchPair = dispatchPairs[pairIndex];
                    int bodyIndexB = dispatchPair.BodyIndexB;

                    if (phaseIdPerPair[pairIndex] == kUninitializedPair // previously not processed. So, either static or collision pair with disabled collision (see below)
                        && (dispatchPair.IsJoint || dispatchPair.IsCollisionEnabled)) // skip over disabled collision pairs
                    {
                        SafetyChecks.CheckAreEqualAndThrow(true, bodyIndexB >= numDynamicBodies); // body must be static by design
                        int bodyIndexA = dispatchPair.BodyIndexA;

                        int phaseIndex = FindFreePhaseDynamicStaticPair(rigidBodyMask, bodyIndexA, lastPhaseIndex);
                        phaseIdPerPair[pairIndex] = (byte)(phaseIndex + 1); // +1 since kUninitializedPair == 0

                        rigidBodyMask[bodyIndexA] |= (ulong)1 << phaseIndex;
                        if (phaseIndex != lastPhaseIndex)
                        {
                            batchInfos[phaseIndex].m_NumElements++;
                        }
                        else
                        {
                            numPairsPerPhase[lastPhaseIndex]++;
                        }
                    }
                }

                for (int i = 0; i < lastPhaseIndex; i++)
                {
                    numPairsPerPhase[i] = batchInfos[i].m_NumBatchesProcessed * kMaxBatchSize + batchInfos[i].m_NumElements;
                }

                // Calculate phase start offset
                int* offsetInPhase = stackalloc int[kMaxNumPhases];
                offsetInPhase[0] = 0;
                for (int i = 1; i < kMaxNumPhases; i++)
                {
                    offsetInPhase[i] = offsetInPhase[i - 1] + numPairsPerPhase[i - 1];
                }

                var createMap = unphasedToPhasedDispatchPairMap.IsCreated;
                SafetyChecks.CheckAreEqualAndThrow(true, !createMap || unphasedToPhasedDispatchPairMap.Length == dispatchPairs.Length);

                // Populate PhasedDispatchPairsArray with dynamic-dynamic pairs
                for (int i = 0; i < dispatchPairs.Length; i++)
                {
                    int bodyIndexB = dispatchPairs[i].BodyIndexB;
                    bool isBodyBStatic = bodyIndexB >= numDynamicBodies;

                    if (!isBodyBStatic && phaseIdPerPair[i] != kUninitializedPair)
                    {
                        int phaseForPair = phaseIdPerPair[i] - 1; // -1 since kUninitializedPair == 0
                        int indexInArray = offsetInPhase[phaseForPair]++;
                        phasedDispatchPairs[indexInArray] = dispatchPairs[i];
                        if (createMap)
                        {
                            unphasedToPhasedDispatchPairMap[i] = indexInArray;
                        }
                    }
                    else if (createMap)
                    {
                        unphasedToPhasedDispatchPairMap[i] = -1; // mark as unused entry. No mapping since the pair was skipped.
                    }
                }

                // Populate PhasedDispatchPairsArray with dynamic-static pairs
                for (int i = 0; i < dispatchPairs.Length; i++)
                {
                    int bodyIndexB = dispatchPairs[i].BodyIndexB;
                    bool isBodyBStatic = bodyIndexB >= numDynamicBodies;

                    if (isBodyBStatic && phaseIdPerPair[i] != kUninitializedPair)
                    {
                        int phaseForPair = phaseIdPerPair[i] - 1; // -1 since kUninitializedPair == 0
                        int indexInArray = offsetInPhase[phaseForPair]++;
                        phasedDispatchPairs[indexInArray] = dispatchPairs[i];
                        if (createMap)
                        {
                            unphasedToPhasedDispatchPairMap[i] = indexInArray;
                        }
                    }
                }

                // Populate SolvePhaseInfo for each solve phase
                int firstWorkItemIndex = 0;
                int numPairs = 0;
                numActivePhases = 0;
                int numPhasedDispatchPairs = 0;
                for (int i = 0; i < kMaxNumPhases; i++)
                {
                    IterativeSolverSchedulerInfo.SolvePhaseInfo info;
                    info.DispatchPairCount = numPairsPerPhase[i];

                    if (info.DispatchPairCount == 0)
                    {
                        break;
                    }

                    numPhasedDispatchPairs += info.DispatchPairCount;

                    info.BatchSize = math.min(kMaxBatchSize, info.DispatchPairCount);
                    info.NumWorkItems = (info.DispatchPairCount + info.BatchSize - 1) / info.BatchSize;
                    info.ContainsDuplicateIndices = i == lastPhaseIndex && info.NumWorkItems > 1;
                    info.FirstWorkItemIndexOffset = firstWorkItemIndex;
                    info.FirstDispatchPairOffset = numPairs;

                    firstWorkItemIndex += info.NumWorkItems;
                    numPairs += info.DispatchPairCount;

                    phaseInfo[i] = info;
                    numActivePhases++;
                }

                // Invalidate the leftover pairs
                for (int i = numPhasedDispatchPairs; i < dispatchPairs.Length; i++)
                {
                    phasedDispatchPairs[i] = DispatchPair.Invalid;
                }

                // Uncomment this code when testing scheduler
                //CheckIntegrity(phasedDispatchPairs, numDynamicBodies, ref phaseInfo);

                numWorkItems = firstWorkItemIndex;
            }

            internal static void CheckIntegrity(NativeArray<DispatchPair> phasedDispatchPairs,
                int numDynamicBodies, ref NativeArray<IterativeSolverSchedulerInfo.SolvePhaseInfo> solverPhaseInfos)
            {
                int dispatchPairCount = 0;
                int expectedFirstWorkItemIndex = 0;
                int expectedFirstDispatchPairIndex = 0;
                for (int i = 0; i < solverPhaseInfos.Length; i++)
                {
                    IterativeSolverSchedulerInfo.SolvePhaseInfo info = solverPhaseInfos[i];

                    // make sure the info contains work load. Otherwise, it shouldn't be present.
                    Assert.IsTrue(info.DispatchPairCount >= 0);

                    // make sure the batch size is valid
                    Assert.IsTrue((info.BatchSize == 0 && info.DispatchPairCount == 0) || (info.BatchSize > 0 && info.BatchSize <= info.DispatchPairCount));

                    // make sure the number of work items in this phase is valid
                    int expectedWorkItemCount = info.BatchSize == 0 ? 0 : (int)((info.DispatchPairCount - 1) / info.BatchSize) + 1;
                    Assert.AreEqual(expectedWorkItemCount, info.NumWorkItems);

                    if (info.NumWorkItems > 0)
                    {
                        // make sure the first work item index is as expected
                        Assert.AreEqual(expectedFirstWorkItemIndex, info.FirstWorkItemIndexOffset);
                        // make sure the first dispatch pair index is as expected
                        Assert.AreEqual(expectedFirstDispatchPairIndex, info.FirstDispatchPairOffset);
                    }

                    // Verify for every phase that it can be processed in parallel. That is, it doesn't contain the same dynamic body twice.
                    // If it does, it will be indicated via the ContainsDuplicateIndices member.
                    // Here we make sure that either of this is the case.
                    dispatchPairCount += info.DispatchPairCount;

                    // Note: if there is only one work item, we don't need to check for duplicate dynamic bodies since
                    // the batch size is equal to the number of dispatch pairs in the phase. Therefore, we can by design
                    // not process the same dynamic body twice in parallel.
                    // In this case, we will not enter the loop below.

                    bool expectDuplicateEntries = info.ContainsDuplicateIndices;
                    int numDuplicatesFound = 0;

                    for (int j = 0; j < info.NumWorkItems - 1; j++)
                    {
                        int firstIndex = info.FirstDispatchPairOffset + j * info.BatchSize;
                        for (int k = j + 1; k < j + 1 + info.NumWorkItems; k++)
                        {
                            int secondIndex = info.FirstDispatchPairOffset + k * info.BatchSize;
                            for (int pairIndex1 = firstIndex;
                                 pairIndex1 < math.min(firstIndex + info.BatchSize, dispatchPairCount);
                                 pairIndex1++)
                            {
                                int aIndex1 = phasedDispatchPairs[pairIndex1].BodyIndexA;
                                int bIndex1 = phasedDispatchPairs[pairIndex1].BodyIndexB;
                                for (int pairIndex2 = secondIndex;
                                     pairIndex2 < math.min(secondIndex + info.BatchSize, dispatchPairCount);
                                     pairIndex2++)
                                {
                                    int aIndex2 = phasedDispatchPairs[pairIndex2].BodyIndexA;
                                    int bIndex2 = phasedDispatchPairs[pairIndex2].BodyIndexB;

                                    numDuplicatesFound += math.select(0, 1, aIndex1 == aIndex2 || aIndex1 == bIndex2);
                                    // Note: static bodies always appear at the 2nd place in the body pair (a,b). Because there is no danger of a data race
                                    // with static bodies given that they are immutable, it is allowed to have duplicate static bodies
                                    // across batches inside a phase. We therefore can (and must) safely ignore them in the duplicate search here.
                                    if (bIndex1 < numDynamicBodies || bIndex2 < numDynamicBodies)
                                    {
                                        numDuplicatesFound += math.select(0, 1, bIndex1 == bIndex2 || bIndex1 == aIndex2);
                                    }
                                }
                            }
                        }
                    }

                    Assert.IsTrue(!expectDuplicateEntries || numDuplicatesFound > 0);
                    expectedFirstDispatchPairIndex = dispatchPairCount;
                    expectedFirstWorkItemIndex += info.NumWorkItems;
                }
            }

            unsafe struct BatchInfo
            {
                internal void Add(NativeArray<ulong> rigidBodyMasks, int bodyIndexA, int bodyIndexB)
                {
                    int indexInBuffer = m_NumElements++ *2;

                    fixed(int* bodyIndices = m_BodyIndices)
                    {
                        bodyIndices[indexInBuffer++] = bodyIndexA;
                        bodyIndices[indexInBuffer] = bodyIndexB;

                        if (m_NumElements == kMaxBatchSize)
                        {
                            indexInBuffer = 0;
                            for (int i = 0; i < kMaxBatchSize; i++)
                            {
                                rigidBodyMasks[bodyIndices[indexInBuffer++]] |= m_PhaseMask;
                                rigidBodyMasks[bodyIndices[indexInBuffer++]] |= m_PhaseMask;
                            }

                            m_NumBatchesProcessed++;
                            m_NumElements = 0;
                        }
                    }
                }

                internal void CommitRigidBodyMasks(NativeArray<ulong> rigidBodyMasks)
                {
                    // Flush
                    int indexInBuffer = 0;
                    fixed(int* bodyIndices = m_BodyIndices)
                    {
                        for (int i = 0; i < m_NumElements; i++)
                        {
                            rigidBodyMasks[bodyIndices[indexInBuffer++]] |= m_PhaseMask;
                            rigidBodyMasks[bodyIndices[indexInBuffer++]] |= m_PhaseMask;
                        }
                    }
                }

                private fixed int m_BodyIndices[kMaxBatchSize * 2];

                internal int m_NumBatchesProcessed;
                internal ulong m_PhaseMask;
                internal int m_NumElements;
            }

            static int FindFreePhaseDynamicDynamicPair(NativeArray<ulong> rigidBodyMask, int bodyIndexA, int bodyIndexB,
                int lastPhaseIndex)
            {
                var mask = rigidBodyMask[bodyIndexA] | rigidBodyMask[bodyIndexB];
                int phaseIndex = -1;
                if (mask == ulong.MaxValue)
                {
                    phaseIndex = lastPhaseIndex;
                }
                else
                {
                    // Find index of first zero bit

                    int trailingOneBits = math.tzcnt(~mask);
                    phaseIndex = trailingOneBits;
                }

                SafetyChecks.CheckAreEqualAndThrow(true, phaseIndex >= 0 && phaseIndex <= lastPhaseIndex);
                return phaseIndex;
            }

            static int FindFreePhaseDynamicStaticPair(NativeArray<ulong> rigidBodyMask, int bodyIndexA,
                int lastPhaseIndex)
            {
                ulong mask = rigidBodyMask[bodyIndexA];

                int phaseIndex = -1;
                if (mask == ulong.MaxValue)
                {
                    phaseIndex = lastPhaseIndex;
                }
                else
                {
                    // Find the first zero bit after last one bit

                    const int kULongBitCount = sizeof(ulong) * 8;
                    int leadingZeroBits = math.lzcnt(mask);
                    phaseIndex = math.min(lastPhaseIndex, kULongBitCount - leadingZeroBits);
                }

                SafetyChecks.CheckAreEqualAndThrow(true, phaseIndex >= 0 && phaseIndex <= lastPhaseIndex);
                return phaseIndex;
            }
        }

        /// <summary>
        /// Deferred version of CreateDispatchPairPhasesJob used to skip execution if job is unnecessary.
        /// Only ever launched with a single worker.
        /// </summary>
        [BurstCompile]
        struct CreateDispatchPairPhasesJobDefer : IJobParallelForDefer
        {
            public CreateDispatchPairPhasesJob.PairType ProcessedPairType;

            [NativeDisableContainerSafetyRestriction]
            public NativeArray<DispatchPair> DispatchPairs;

            [NativeDisableContainerSafetyRestriction]
            public SolverSchedulerInfo SolverSchedulerInfo;

            [NativeDisableContainerSafetyRestriction]
            public NativeArray<DispatchPair> PhasedDispatchPairs;

            public NativeReference<int>.ReadOnly NumDirectPairs;
            public NativeReference<int>.ReadOnly NumCouplingPairs;

            public int NumDynamicBodies;

            public void Execute(int workerIndex)
            {
                // Note: workerIndex should always be 0, since we only derive from IJobParallelForDefer to skip execution
                // of this job if not required, i.e., it is either scheduled with a single worker or with no worker.
                SafetyChecks.CheckAreEqualAndThrow(0, workerIndex);

                CreateDispatchPairPhasesJob.ExecuteImpl(ProcessedPairType, DispatchPairs,
                    SolverSchedulerInfo, PhasedDispatchPairs, NumDirectPairs, NumCouplingPairs, NumDynamicBodies);
            }
        }

        #endregion
    }
}
