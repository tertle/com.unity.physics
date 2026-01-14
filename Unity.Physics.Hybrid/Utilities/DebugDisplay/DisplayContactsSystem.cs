using Unity.Burst;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Entities;
using Unity.Mathematics;
using Unity.Physics;
using Unity.Physics.Authoring;
using Unity.Physics.Systems;

namespace Unity.Physics.Authoring
{
#if UNITY_EDITOR

    // A system which draws all contact points produced by the physics step system
    [RequireMatchingQueriesForUpdate]
    [UpdateInGroup(typeof(PhysicsSimulationGroup))]
    [UpdateBefore(typeof(PhysicsCreateJacobiansGroup))]
    [UpdateAfter(typeof(PhysicsCreateContactsGroup))]
    [BurstCompile]
    internal partial struct DisplayContactsSystem : ISystem
    {
        public void OnCreate(ref SystemState state)
        {
            state.RequireForUpdate<PhysicsDebugDisplayData>();
            state.RequireForUpdate<PhysicsWorldSingleton>();
            state.RequireForUpdate<SimulationSingleton>();
        }

        [BurstCompile]
        public void OnUpdate(ref SystemState state)
        {
            if (!SystemAPI.TryGetSingleton(out PhysicsDebugDisplayData debugDisplay) || debugDisplay.DrawContacts == 0)
                return;

            if (!SystemAPI.TryGetSingleton(out DebugDraw draw))
                return;

            var world = SystemAPI.GetSingleton<PhysicsWorldSingleton>().PhysicsWorld;
            state.Dependency = new DisplayContactsJob
            {
                Draw = draw
            }.Schedule(SystemAPI.GetSingleton<SimulationSingleton>(), ref world, state.Dependency);
        }

        // Job which iterates over contacts from narrowphase and writes to display.
        [BurstCompile]
        private struct DisplayContactsJob : IContactsJob
        {
            [ReadOnly][NativeDisableUnsafePtrRestriction] public DebugDraw Draw;

            public void Execute(ref ModifiableContactHeader header, ref ModifiableContactPoint point)
            {
                float3 x0 = point.Position;
                float3 x1 = header.Normal * point.Distance;
                Draw.Arrow(x0, x1, Unity.DebugDisplay.ColorIndex.Green);
            }
        }
    }
#endif
}
