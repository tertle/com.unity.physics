using Unity.Physics.Systems;
using Unity.Collections;
using Unity.Entities;
using Unity.Jobs;
using Unity.Mathematics;
using UnityEngine;
using Unity.Burst;
using Unity.Transforms;

namespace Unity.Physics.Authoring
{
#if UNITY_EDITOR
    /// Job to iterate over all the bodies in a scene, for any
    /// which have a collider, calculate the bounding box and
    /// display it.
    [BurstCompile]
    public struct DisplayColliderAabbsJob : IJobParallelFor
    {
        [ReadOnly] private NativeArray<RigidBody> RigidBodies;

        public static JobHandle ScheduleJob(in NativeArray<RigidBody> rigidBodies, JobHandle inputDeps)
        {
            return new DisplayColliderAabbsJob
            {
                RigidBodies = rigidBodies
            }.Schedule(rigidBodies.Length, 16, inputDeps);
        }

        public void Execute(int i)
        {
            if (RigidBodies[i].Collider.IsCreated)
            {
                Aabb aabb = RigidBodies[i].CalculateAabb();
                float3 center = aabb.Center;
                PhysicsDebugDisplaySystem.Box(aabb.Extents, center, Quaternion.identity, DebugDisplay.ColorIndex.BrightRed);
            }
        }
    }

    [RequireMatchingQueriesForUpdate]
    [WorldSystemFilter(WorldSystemFilterFlags.Default)]
    [UpdateInGroup(typeof(PhysicsDebugDisplayGroup))]
    [BurstCompile]
    internal partial struct DisplayColliderAabbsSystem_Default : ISystem
    {
        private EntityQuery ColliderQuery;

        void OnCreate(ref SystemState state)
        {
            ColliderQuery = state.GetEntityQuery(ComponentType.ReadOnly<PhysicsCollider>(),
                ComponentType.ReadOnly<LocalToWorld>(),
                ComponentType.ReadOnly<LocalTransform>());

            state.RequireForUpdate(ColliderQuery);
            state.RequireForUpdate<PhysicsDebugDisplayData>();
        }

        [BurstCompile]
        void OnUpdate(ref SystemState state)
        {
            if (!SystemAPI.TryGetSingleton(out PhysicsDebugDisplayData debugDisplay) || debugDisplay.DrawColliderAabbs == 0)
                return;

            if (debugDisplay.DrawColliderAabbs == (int)PhysicsDebugDisplayAuthoring.DisplayMode.PreIntegration)
            {
                if (SystemAPI.TryGetSingleton(out PhysicsWorldSingleton physicsWorldSingleton))
                {
                    state.Dependency = DisplayColliderAabbsJob.ScheduleJob(physicsWorldSingleton.PhysicsWorld.Bodies, state.Dependency);
                }
            }
            else if (debugDisplay.DrawColliderAabbs == (int)PhysicsDebugDisplayAuthoring.DisplayMode.PostIntegration)
            {
                var rigidBodiesList = new NativeList<RigidBody>(Allocator.TempJob);
                DrawColliderUtility.GetRigidBodiesFromQuery(ref state, ref ColliderQuery, ref rigidBodiesList);

                if (rigidBodiesList.IsEmpty)
                {
                    rigidBodiesList.Dispose();
                    return;
                }

                var displayHandle = DisplayColliderAabbsJob.ScheduleJob(rigidBodiesList.AsArray(), state.Dependency);
                var disposeHandle = rigidBodiesList.Dispose(displayHandle);

                state.Dependency = disposeHandle;
            }
        }
    }

    /// Create a DisplayColliderAabbsJob
    [RequireMatchingQueriesForUpdate]
    [WorldSystemFilter(WorldSystemFilterFlags.Editor)]
    [UpdateInGroup(typeof(PhysicsDebugDisplayGroup_Editor))]
    [BurstCompile]
    internal partial struct DisplayColliderAabbsSystem_Editor : ISystem
    {
        private EntityQuery ColliderQuery;

        void OnCreate(ref SystemState state)
        {
            ColliderQuery = state.GetEntityQuery(ComponentType.ReadOnly<PhysicsCollider>(),
                ComponentType.ReadOnly<LocalToWorld>(),
                ComponentType.ReadOnly<LocalTransform>());

            state.RequireForUpdate(ColliderQuery);
            state.RequireForUpdate<PhysicsDebugDisplayData>();
        }

        [BurstCompile]
        void OnUpdate(ref SystemState state)
        {
            if (!SystemAPI.TryGetSingleton(out PhysicsDebugDisplayData debugDisplay) || debugDisplay.DrawColliderAabbs == 0)
                return;

            var rigidBodiesList = new NativeList<RigidBody>(Allocator.TempJob);
            DrawColliderUtility.GetRigidBodiesFromQuery(ref state, ref ColliderQuery, ref rigidBodiesList);

            if (rigidBodiesList.IsEmpty)
            {
                rigidBodiesList.Dispose();
                return;
            }

            var displayHandle = DisplayColliderAabbsJob.ScheduleJob(rigidBodiesList.AsArray(), state.Dependency);
            var disposeHandle = rigidBodiesList.Dispose(displayHandle);

            state.Dependency = disposeHandle;
        }
    }
#endif
}