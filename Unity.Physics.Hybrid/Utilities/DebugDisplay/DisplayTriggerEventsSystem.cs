using Unity.Burst;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Entities;
using Unity.Physics;
using Unity.Physics.Authoring;

namespace Unity.Physics.Authoring
{
#if UNITY_EDITOR

    // A system which draws any trigger events produced by the physics step system
    [RequireMatchingQueriesForUpdate]
    [UpdateInGroup(typeof(PhysicsDebugDisplayGroup))]
    [BurstCompile]
    internal partial struct DisplayTriggerEventsSystem : ISystem
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
            if (!SystemAPI.TryGetSingleton(out PhysicsDebugDisplayData debugDisplay) || debugDisplay.DrawTriggerEvents == 0)
                return;

            if (!SystemAPI.TryGetSingleton(out DebugDraw draw))
                return;

            state.Dependency = new DisplayTriggerEventsJob()
            {
                Draw = draw,
                World = SystemAPI.GetSingleton<PhysicsWorldSingleton>().PhysicsWorld
            }.Schedule(SystemAPI.GetSingleton<SimulationSingleton>(), state.Dependency);
        }

        //Job which iterates over trigger events and writes display info to rendering buffers.
        [BurstCompile]
        private struct DisplayTriggerEventsJob : ITriggerEventsJob
        {
            [ReadOnly][NativeDisableUnsafePtrRestriction] public DebugDraw Draw;
            [ReadOnly] public PhysicsWorld World;

            public void Execute(TriggerEvent triggerEvent)
            {
                RigidBody bodyA = World.Bodies[triggerEvent.BodyIndexA];
                RigidBody bodyB = World.Bodies[triggerEvent.BodyIndexB];

                Aabb aabbA = bodyA.CalculateAabb();
                Aabb aabbB = bodyB.CalculateAabb();
                Draw.Line(aabbA.Center, aabbB.Center, Unity.DebugDisplay.ColorIndex.Yellow);
            }
        }
    }
#endif
}
