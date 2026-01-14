using Unity.Burst;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Entities;
using Unity.Jobs;
using Unity.Mathematics;

namespace Unity.Physics.Authoring
{
#if UNITY_EDITOR || ENABLE_UNITY_PHYSICS_RUNTIME_DEBUG_DISPLAY

    /// Job which walks the broadphase tree and displays the
    /// bounding box of leaf nodes.
    [BurstCompile]
    struct DisplayBroadphaseJob : IJob
    {
        public DebugDraw DebugDraw;

        [ReadOnly]
        public NativeList<BoundingVolumeHierarchy.Node> StaticNodes;

        [ReadOnly]
        public NativeList<BoundingVolumeHierarchy.Node> DynamicNodes;

        void DrawLeavesRecursive(NativeArray<BoundingVolumeHierarchy.Node> nodes, Unity.DebugDisplay.ColorIndex color, int nodeIndex)
        {
            if (nodes[nodeIndex].IsLeaf)
            {
                bool4 leavesValid = nodes[nodeIndex].AreLeavesValid;
                for (int l = 0; l < 4; l++)
                {
                    if (leavesValid[l])
                    {
                        Aabb aabb = nodes[nodeIndex].Bounds.GetAabb(l);
                        float3 center = aabb.Center;
                        DebugDraw.Box(aabb.Extents, center, quaternion.identity, color);
                    }
                }

                return;
            }

            for (int i = 0; i < 4; i++)
            {
                if (nodes[nodeIndex].IsChildValid(i))
                {
                    DrawLeavesRecursive(nodes, color, nodes[nodeIndex].Data[i]);
                }
            }
        }

        public void Execute()
        {
            DrawLeavesRecursive(StaticNodes.AsArray(), Unity.DebugDisplay.ColorIndex.Yellow, 1);
            DrawLeavesRecursive(DynamicNodes.AsArray(), Unity.DebugDisplay.ColorIndex.Red, 1);
        }
    }

    // Creates DisplayBroadphaseJobs
    [RequireMatchingQueriesForUpdate]
    [UpdateInGroup(typeof(PhysicsDebugDisplayGroup))]
    [BurstCompile]
    partial struct DisplayBroadphaseAabbsSystem : ISystem
    {
        [BurstCompile]
        public void OnCreate(ref SystemState state)
        {
            state.RequireForUpdate<PhysicsDebugDisplayData>();
            state.RequireForUpdate<PhysicsWorldSingleton>();
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            if (!SystemAPI.TryGetSingleton(out PhysicsDebugDisplayData debugDisplay) || debugDisplay.DrawBroadphase == 0)
                return;

            if (!SystemAPI.TryGetSingleton(out DebugDraw draw))
                return;

            Broadphase broadphase = SystemAPI.GetSingleton<PhysicsWorldSingleton>().CollisionWorld.Broadphase;

            state.Dependency = new DisplayBroadphaseJob
            {
                StaticNodes = broadphase.StaticTree.Nodes,
                DynamicNodes = broadphase.DynamicTree.Nodes,
                DebugDraw = draw
            }.Schedule(state.Dependency);
        }
    }
#endif
}
