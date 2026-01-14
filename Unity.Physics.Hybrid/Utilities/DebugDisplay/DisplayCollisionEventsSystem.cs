using Unity.Burst;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Entities;
using Unity.Physics;
using Unity.Physics.Authoring;

namespace Unity.Physics.Authoring
{
#if UNITY_EDITOR

    // A system which draws any collision events produced by the physics step system
    [RequireMatchingQueriesForUpdate]
    [UpdateInGroup(typeof(PhysicsDebugDisplayGroup))]
    [BurstCompile]
    partial struct DisplayCollisionEventsSystem : ISystem
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
            if (!SystemAPI.TryGetSingleton(out PhysicsDebugDisplayData debugDisplay) || debugDisplay.DrawCollisionEvents == 0)
                return;

            if (!SystemAPI.TryGetSingleton(out DebugDraw draw))
                return;

            state.Dependency = new DisplayCollisionEventsJob
            {
                Draw = draw,
                World = SystemAPI.GetSingleton<PhysicsWorldSingleton>().PhysicsWorld
            }.Schedule(SystemAPI.GetSingleton<SimulationSingleton>(), state.Dependency);
        }

        // Job which iterates over collision events and writes display info to a PhysicsDebugDisplaySystem.
        [BurstCompile]
        struct DisplayCollisionEventsJob : ICollisionEventsJob
        {
            [ReadOnly][NativeDisableUnsafePtrRestriction] public DebugDraw Draw;
            [ReadOnly] public PhysicsWorld World;

            public void Execute(CollisionEvent collisionEvent)
            {
                CollisionEvent.Details details = collisionEvent.CalculateDetails(ref World);

                //Color code the impulse depending on the collision feature
                //vertex - blue
                //edge - cyan
                //face - magenta
                Unity.DebugDisplay.ColorIndex color;
                switch (details.EstimatedContactPointPositions.Length)
                {
                    case 1:
                        color = Unity.DebugDisplay.ColorIndex.Blue;
                        break;
                    case 2:
                        color = Unity.DebugDisplay.ColorIndex.Cyan;
                        break;
                    default:
                        color = Unity.DebugDisplay.ColorIndex.Magenta;
                        break;
                }

                var averageContactPosition = details.AverageContactPointPosition;
                Draw.Point(averageContactPosition, 0.01f, color);
                Draw.Arrow(averageContactPosition, collisionEvent.Normal * details.EstimatedImpulse, color);
            }
        }
    }
#endif
}
