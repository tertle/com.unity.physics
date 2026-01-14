using System;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.DebugDisplay;
using Unity.Mathematics;
using UnityEngine;
using Unity.Entities;
using Unity.Transforms;

namespace Unity.Physics.Authoring
{
#if UNITY_EDITOR || ENABLE_UNITY_PHYSICS_RUNTIME_DEBUG_DISPLAY

    /// <summary>
    /// A component system group that contains the physics debug display systems.
    /// </summary>
    [WorldSystemFilter(WorldSystemFilterFlags.Default)]
    [UpdateInGroup(typeof(LateSimulationSystemGroup))]
    public partial class PhysicsDebugDisplayGroup : ComponentSystemGroup
    {
    }

    /// <summary>
    /// A component system group that contains the physics debug display systems while in edit mode.
    /// </summary>
    [WorldSystemFilter(WorldSystemFilterFlags.Editor)]
    [UpdateAfter(typeof(TransformSystemGroup))]
    public partial class PhysicsDebugDisplayGroup_Editor : ComponentSystemGroup
    {
    }

    /// <summary>
    /// Debug draw singleton component with functions used for drawing debug display.
    /// <seealso cref="PhysicsDebugDisplaySystem"/>
    /// </summary>
    public struct DebugDraw : IComponentData
    {
        internal DrawData m_DrawData;

        /// <summary>
        /// Draws a point.
        /// </summary>
        /// <param name="x"> World space position. </param>
        /// <param name="size"> Extents. </param>
        /// <param name="color"> Color. </param>
        public void Point(float3 x, float size, ColorIndex color)
        {
            var lines = new Lines(3, m_DrawData);

            lines.Draw(x - new float3(size, 0, 0), x + new float3(size, 0, 0), color);
            lines.Draw(x - new float3(0, size, 0), x + new float3(0, size, 0), color);
            lines.Draw(x - new float3(0, 0, size), x + new float3(0, 0, size), color);
        }

        /// <summary>
        /// Draws a line between 2 points.
        /// </summary>
        /// <param name="x0"> Point 0 in world space. </param>
        /// <param name="x1"> Point 1 in world space. </param>
        /// <param name="color"> Color. </param>
        public void Line(float3 x0, float3 x1, Unity.DebugDisplay.ColorIndex color)
        {
            new Lines(1, m_DrawData).Draw(x0, x1, color);
        }

        /// <summary>
        /// Draws multiple lines between the provided pairs of points.
        /// </summary>
        /// <param name="lineVertices"> A pointer to a vertices array containing a sequence of point pairs. A line is drawn between every pair of points. </param>
        /// <param name="numVertices"> Number of vertices. </param>
        /// <param name="color"> Color. </param>
        public unsafe void Lines(float3* lineVertices, int numVertices, ColorIndex color)
        {
            var lines = new Lines(numVertices / 2, m_DrawData);
            for (int i = 0; i < numVertices; i += 2)
            {
                lines.Draw(lineVertices[i], lineVertices[i + 1], color);
            }
        }

        /// <summary>
        /// Draws multiple lines between the provided pairs of points.
        /// </summary>
        /// <param name="lineVertices"> A list of vertices containing a sequence of point pairs. A line is drawn between every pair of points. </param>
        /// <param name="color"> Color. </param>
        public void Lines(in NativeList<float3> lineVertices, ColorIndex color)
        {
            unsafe
            {
                Lines(lineVertices.GetUnsafeReadOnlyPtr(), lineVertices.Length, color);
            }
        }

        /// <summary>
        /// Draws multiple lines between the provided pairs of points.
        /// </summary>
        /// <param name="lineVertices"> An array of vertices containing a sequence of point pairs. A line is drawn between every pair of points. </param>
        /// <param name="color"> Color. </param>
        public void Lines(in NativeArray<float3> lineVertices, ColorIndex color)
        {
            unsafe
            {
                Lines((float3*)lineVertices.GetUnsafeReadOnlyPtr(), lineVertices.Length, color);
            }
        }

        /// <summary>
        /// Draws multiple triangles from the provided data arrays.
        /// </summary>
        /// <param name="vertices"> An array of vertices. </param>
        /// <param name="triangleIndices"> An array of triangle indices pointing into the vertices array. A triangle is drawn from every triplet of triangle indices. </param>
        /// <param name="color"> Color. </param>
        public void TriangleEdges(in NativeArray<float3> vertices, in NativeArray<int> triangleIndices, ColorIndex color)
        {
            var lines = new Lines(triangleIndices.Length, m_DrawData);
            for (int i = 0; i < triangleIndices.Length; i += 3)
            {
                lines.Draw(vertices[triangleIndices[i]], vertices[triangleIndices[i + 1]], color);
                lines.Draw(vertices[triangleIndices[i + 1]], vertices[triangleIndices[i + 2]], color);
                lines.Draw(vertices[triangleIndices[i + 2]], vertices[triangleIndices[i]], color);
            }
        }

        /// <summary>
        /// Draws an arrow.
        /// </summary>
        /// <param name="x"> World space position of the arrow base. </param>
        /// <param name="v"> Arrow direction with length. </param>
        /// <param name="color"> Color. </param>
        public void Arrow(float3 x, float3 v, ColorIndex color)
        {
            new Arrows(1, m_DrawData).Draw(x, v, color);
        }

        /// <summary>
        /// Draws a plane.
        /// </summary>
        /// <param name="x"> Point in world space. </param>
        /// <param name="v"> Normal. </param>
        /// <param name="color"> Color. </param>
        public void Plane(float3 x, float3 v, ColorIndex color)
        {
            new Planes(1, m_DrawData).Draw(x, v, color);
        }

        /// <summary>
        /// Draws an arc.
        /// </summary>
        /// <param name="center"> World space position of the arc center. </param>
        /// <param name="normal"> Arc normal. </param>
        /// <param name="arm"> Arc arm. </param>
        /// <param name="angle"> Arc angle. </param>
        /// <param name="color"> Color. </param>
        public void Arc(float3 center, float3 normal, float3 arm, float angle, ColorIndex color)
        {
            new Arcs(1, m_DrawData).Draw(center, normal, arm, angle, color);
        }

        /// <summary>
        /// Draws a cone.
        /// </summary>
        /// <param name="point"> Point in world space. </param>
        /// <param name="axis"> Cone axis. </param>
        /// <param name="angle"> Cone angle. </param>
        /// <param name="color"> Color. </param>
        public void Cone(float3 point, float3 axis, float angle, ColorIndex color)
        {
            new Cones(1, m_DrawData).Draw(point, axis, angle, color);
        }

        /// <summary>
        /// Draws a box.
        /// </summary>
        /// <param name="size"> Size of the box. </param>
        /// <param name="center"> Center of the box in world space. </param>
        /// <param name="orientation"> Orientation of the box in world space. </param>
        /// <param name="color"> Color. </param>
        public void Box(float3 size, float3 center, quaternion orientation, ColorIndex color)
        {
            new Boxes(1, m_DrawData).Draw(size, center, orientation, color);
        }

        /// <summary>
        /// Draws multiple triangles from the provided data arrays.
        /// </summary>
        /// <param name="vertices"> An array of vertices. </param>
        /// <param name="triangleIndices"> An array of triangle indices pointing into the vertices array. A triangle is drawn from every triplet of triangle indices. </param>
        /// <param name="color"> Color. </param>
        public void Triangles(in NativeArray<float3> vertices, in NativeArray<int> triangleIndices, ColorIndex color)
        {
            var triangles = new Triangles(triangleIndices.Length / 3, m_DrawData);
            for (int i = 0; i < triangleIndices.Length; i += 3)
            {
                var v0 = vertices[triangleIndices[i]];
                var v1 = vertices[triangleIndices[i + 1]];
                var v2 = vertices[triangleIndices[i + 2]];

                float3 normal = math.normalize(math.cross(v1 - v0, v2 - v0));
                triangles.Draw(v0, v1, v2, normal, color);
            }
        }

        /// <summary>
        /// Draws multiple triangles from the provided array of triplets of vertices.
        /// </summary>
        /// <param name="vertices"> An array containing a sequence of vertex triplets. A triangle is drawn from every triplet of vertices. </param>
        /// <param name="numVertices"> Number of vertices. </param>
        /// <param name="color"> Color. </param>
        public unsafe void Triangles(float3* vertices, int numVertices, ColorIndex color)
        {
            var triangles = new Triangles(numVertices / 3, m_DrawData);
            for (int i = 0; i < numVertices; i += 3)
            {
                var v0 = vertices[i];
                var v1 = vertices[i + 1];
                var v2 = vertices[i + 2];

                float3 normal = math.normalize(math.cross(v1 - v0, v2 - v0));
                triangles.Draw(v0, v1, v2, normal, color);
            }
        }

        /// <summary>
        /// Draws multiple triangles from the provided list of triplets of vertices.
        /// </summary>
        /// <param name="vertices"> A list containing a sequence of vertex triplets. A triangle is drawn from every triplet of vertices. </param>
        /// <param name="color"> Color. </param>
        public void Triangles(in NativeList<float3> vertices, ColorIndex color)
        {
            unsafe
            {
                Triangles(vertices.GetUnsafePtr(), vertices.Length, color);
            }
        }

        /// <summary>
        /// Draws multiple triangles from the provided array of triplets of vertices.
        /// </summary>
        /// <param name="vertices"> An array containing a sequence of vertex triplets. A triangle is drawn from every triplet of vertices. </param>
        /// <param name="color"> Color. </param>
        public void Triangles(in NativeArray<float3> vertices, ColorIndex color)
        {
            unsafe
            {
                Triangles((float3*)vertices.GetUnsafePtr(), vertices.Length, color);
            }
        }

        /// <summary>
        /// Draws a number in world space using 7-segment line display.
        /// </summary>
        /// <param name="value">The integer number to draw.</param>
        /// <param name="position">The world space position of the number (bottom-left).</param>
        /// <param name="scale">The scale for each digit segment.</param>
        /// <param name="spacing">The space between digits.</param>
        /// <param name="color">The color of the number lines.</param>
        public void Number(int value, float3 position, float scale, float spacing, ColorIndex color)
        {
            using var numbers = new DrawNumbers(16, m_DrawData); // 16 segments is safe for small numbers
            numbers.DrawNumber(value, position, spacing, scale, color);
        }

        /// <summary>
        /// Draws a string in world space using line segments for letters and symbols.
        /// </summary>
        /// <param name="text">The text message to draw (uppercase A-Z, digits, space, colon supported).</param>
        /// <param name="position">The world space position of the first character (bottom-left).</param>
        /// <param name="scale">The scale for each character segment.</param>
        /// <param name="spacing">The space between characters.</param>
        /// <param name="color">The color of the text lines.</param>
        public void Text(FixedString32Bytes text, float3 position, float scale, float spacing, ColorIndex color)
        {
            using var letters = new DrawLetters(text.Length, m_DrawData);
            letters.DrawText(text, position, spacing, scale, color);
        }
    }

    /// <summary>
    /// A system which is responsible for drawing physics debug display data.
    /// Create a singleton entity with <see cref="PhysicsDebugDisplayData"/> and select what you want to be drawn.<para/>
    ///
    /// If you want custom debug draw, you need to:<para/>
    /// 1) Create a system which updates before PhysicsDebugDisplaySystem.<br/>
    /// 2) In OnUpdate() of that system, call GetSingleton <see cref="PhysicsDebugDisplayData"/> (even if you are not using it, it is important to do so to properly chain dependencies).<br/>
    /// 3) Also in OnUpdate(), obtain the <see cref="DebugDraw"/> singleton component and use its methods (Line, Triangle, etc.) to draw your custom debug data.<br/>
    /// IMPORTANT: Drawing works only in the Editor.
    /// </summary>
    public abstract partial class PhysicsDebugDisplaySystem : SystemBase
    {
#if UNITY_EDITOR || ENABLE_UNITY_PHYSICS_RUNTIME_DEBUG_DISPLAY
        GameObject m_DrawComponentGameObject;
#endif

        DebugDisplay.Renderer m_Renderer;

        void Render()
        {
            if (m_Renderer != null)
            {
                CompleteDisplayDataDependencies();
                m_Renderer.Render();
            }
        }

        internal void Clear()
        {
            if (m_Renderer != null)
            {
                CompleteDisplayDataDependencies();
                m_Renderer.Clear();

                // set singleton draw data component again, in case its buffers were reallocated during the Clear() call
                var draw = new DebugDraw { m_DrawData = m_Renderer.DrawData };
                SystemAPI.SetSingleton(draw);
            }
        }

        class DrawComponent : MonoBehaviour
        {
            public PhysicsDebugDisplaySystem System;
            public void OnDrawGizmos()
            {
#if UNITY_EDITOR
                System?.Render();
#endif
            }

            void OnRenderObject()
            {
#if ENABLE_UNITY_PHYSICS_RUNTIME_DEBUG_DISPLAY
                System?.Render();
#endif
            }

            void OnDestroy()
            {
#if ENABLE_UNITY_PHYSICS_RUNTIME_DEBUG_DISPLAY
                // Clearing data in the player, or when changing between scenes.
                System?.Clear()
#endif
            }
        }

        protected override void OnCreate()
        {
            RequireForUpdate<PhysicsDebugDisplayData>();

#if UNITY_EDITOR || ENABLE_UNITY_PHYSICS_RUNTIME_DEBUG_DISPLAY
            m_Renderer = new DebugDisplay.Renderer();
            var draw = new DebugDraw { m_DrawData = m_Renderer.DrawData };
            EntityManager.CreateSingleton(draw);
#endif
        }

        protected override void OnDestroy()
        {
#if UNITY_EDITOR || ENABLE_UNITY_PHYSICS_RUNTIME_DEBUG_DISPLAY
            if (SystemAPI.HasSingleton<DebugDraw>())
            {
                EntityManager.DestroyEntity(SystemAPI.GetSingletonEntity<DebugDraw>());
            }

            m_Renderer.Dispose();
            m_Renderer = null;
#endif
        }

        protected override void OnStartRunning()
        {
#if UNITY_EDITOR || ENABLE_UNITY_PHYSICS_RUNTIME_DEBUG_DISPLAY
            if (m_DrawComponentGameObject == null)
            {
                m_DrawComponentGameObject = new GameObject("PhysicsDebugDisplaySystem")
                {
                    hideFlags = HideFlags.DontSave | HideFlags.HideInHierarchy
                };

                // Note: we are adding an additional child here so that we can hide the parent in the hierarchy
                // without hiding the child, which would prevent the OnDrawGizmos method on the DrawComponent in the child
                // from being called.
                var childGameObject = new GameObject("DrawComponent")
                {
                    hideFlags = HideFlags.DontSave
                };
                childGameObject.transform.parent = m_DrawComponentGameObject.transform;

                var drawComponent = childGameObject.AddComponent<DrawComponent>();

                drawComponent.System = this;
            }
#endif
        }

        static void DestroyGameObject(GameObject gameObject)
        {
            if (Application.isPlaying)
                UnityEngine.Object.Destroy(gameObject);
            else
                UnityEngine.Object.DestroyImmediate(gameObject);
        }

        protected override void OnStopRunning()
        {
#if UNITY_EDITOR || ENABLE_UNITY_PHYSICS_RUNTIME_DEBUG_DISPLAY
            if (m_DrawComponentGameObject != null)
            {
                var drawComponent = m_DrawComponentGameObject.GetComponentInChildren<DrawComponent>();
                if (drawComponent)
                {
                    drawComponent.System = null;
                }

                while (m_DrawComponentGameObject.transform.childCount > 0)
                {
                    var child = m_DrawComponentGameObject.transform.GetChild(0);
                    child.parent = null;
                    DestroyGameObject(child.gameObject);
                }

                DestroyGameObject(m_DrawComponentGameObject);

                m_DrawComponentGameObject = null;
            }
#endif
        }

        protected override void OnUpdate()
        {
        }

        /// <summary>
        ///  Completes dependencies for all systems in the physics debug display groups, which ensures that the debug display
        ///  data is fully produced by the corresponding debug display data systems before it is being prepared for rendering by this system.
        /// </summary>
        void CompleteDisplayDataDependencies()
        {
            var displayGroup = World.GetExistingSystemManaged<PhysicsDebugDisplayGroup>();
            if (displayGroup != null)
            {
                using var systemHandles = displayGroup.GetAllSystems();
                foreach (var handle in systemHandles)
                {
                    World.Unmanaged.ResolveSystemStateRef(handle).CompleteDependency();
                }
            }

            var editorDisplayGroup = World.GetExistingSystemManaged<PhysicsDebugDisplayGroup_Editor>();
            if (editorDisplayGroup != null)
            {
                using var systemHandles = editorDisplayGroup.GetAllSystems();
                foreach (var handle in systemHandles)
                {
                    World.Unmanaged.ResolveSystemStateRef(handle).CompleteDependency();
                }
            }
        }
    }

    /// <summary>
    /// Draws physics debug display data.
    /// </summary>
    [WorldSystemFilter(WorldSystemFilterFlags.Default)]
    [UpdateInGroup(typeof(PhysicsDebugDisplayGroup), OrderLast = true)]
    [RequireMatchingQueriesForUpdate]
    public partial class PhysicsDebugDisplaySystem_Default : PhysicsDebugDisplaySystem
    {}

    /// <summary>
    /// Draws physics debug display data while in edit mode.
    /// </summary>
    [WorldSystemFilter(WorldSystemFilterFlags.Editor)]
    [UpdateInGroup(typeof(PhysicsDebugDisplayGroup_Editor), OrderLast = true)]
    [RequireMatchingQueriesForUpdate]
    public partial class PhysicsDebugDisplaySystem_Editor : PhysicsDebugDisplaySystem
    {}

    #region Deprecated

    /// <summary>
    /// <para> Deprecated. Use PhysicsDebugDisplayGroup instead. </para>
    /// A component system group that contains the physics debug display systems.
    /// </summary>
    [DisableAutoCreation]
    [WorldSystemFilter(WorldSystemFilterFlags.Default)]
    [UpdateInGroup(typeof(LateSimulationSystemGroup))]
    [Obsolete("PhysicsDisplayDebugGroup has been deprecated (RemovedAfter 2023-05-04). Use PhysicsDebugDisplayGroup instead. (UnityUpgradable) -> PhysicsDebugDisplayGroup", true)]
    public partial class PhysicsDisplayDebugGroup : ComponentSystemGroup
    {
    }

    partial class PhysicsDebugDisplaySystem
    {
        /// <summary>
        /// Draws a point.
        /// <para> Deprecated. Use <see cref="DebugDraw.Point"/> instead. </para>
        /// </summary>
        /// <param name="x"> World space position. </param>
        /// <param name="size"> Extents. </param>
        /// <param name="color"> Color. </param>
        [Obsolete("PhysicsDebugDisplaySystem.Point has been deprecated (RemovedAfter 2024-06-11). Use DebugDraw.Point instead.", true)]
        public static void Point(float3 x, float size, ColorIndex color)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Draws a line between 2 points.
        /// <para> Deprecated. Use <see cref="DebugDraw.Line"/> instead. </para>
        /// </summary>
        /// <param name="x0"> Point 0 in world space. </param>
        /// <param name="x1"> Point 1 in world space. </param>
        /// <param name="color"> Color. </param>
        [Obsolete("PhysicsDebugDisplaySystem.Line has been deprecated (RemovedAfter 2024-06-11). Use DebugDraw.Line instead.", true)]
        public static void Line(float3 x0, float3 x1, Unity.DebugDisplay.ColorIndex color)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Draws multiple lines between the provided pairs of points.
        /// <para> Deprecated. Use <see cref="DebugDraw.Lines(float3*,int,ColorIndex)"/> instead. </para>
        /// </summary>
        /// <param name="lineVertices"> A pointer to a vertices array containing a sequence of point pairs. A line is drawn between every pair of points. </param>
        /// <param name="numVertices"> Number of vertices. </param>
        /// <param name="color"> Color. </param>
        [Obsolete("PhysicsDebugDisplaySystem.Lines has been deprecated (RemovedAfter 2024-06-11). Use DebugDraw.Lines instead.", true)]
        public static unsafe void Lines(float3* lineVertices, int numVertices, ColorIndex color)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Draws multiple lines between the provided pairs of points.
        /// <para> Deprecated. Use <see cref="DebugDraw.Lines(NativeList{float3},ColorIndex)"/> instead. </para>
        /// </summary>
        /// <param name="lineVertices"> A list of vertices containing a sequence of point pairs. A line is drawn between every pair of points. </param>
        /// <param name="color"> Color. </param>
        [Obsolete("PhysicsDebugDisplaySystem.Lines has been deprecated (RemovedAfter 2024-06-11). Use DebugDraw.Lines instead.", true)]
        public static void Lines(in NativeList<float3> lineVertices, ColorIndex color)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Draws multiple lines between the provided pairs of points.
        /// <para> Deprecated. Use <see cref="DebugDraw.Lines(NativeArray{float3},ColorIndex)"/> instead. </para>
        /// </summary>
        /// <param name="lineVertices"> An array of vertices containing a sequence of point pairs. A line is drawn between every pair of points. </param>
        /// <param name="color"> Color. </param>
        [Obsolete("PhysicsDebugDisplaySystem.Lines has been deprecated (RemovedAfter 2024-06-11). Use DebugDraw.Lines instead.", true)]
        public static void Lines(in NativeArray<float3> lineVertices, ColorIndex color)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Draws multiple triangles from the provided data arrays.
        /// <para> Deprecated. Use <see cref="DebugDraw.TriangleEdges"/> instead. </para>
        /// </summary>
        /// <param name="vertices"> An array of vertices. </param>
        /// <param name="triangleIndices"> An array of triangle indices pointing into the vertices array. A triangle is drawn from every triplet of triangle indices. </param>
        /// <param name="color"> Color. </param>
        [Obsolete("PhysicsDebugDisplaySystem.TriangleEdges has been deprecated (RemovedAfter 2024-06-11). Use DebugDraw.TriangleEdges instead.", true)]
        public static void TriangleEdges(in NativeArray<float3> vertices, in NativeArray<int> triangleIndices,
            ColorIndex color)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Draws an arrow.
        /// <para> Deprecated. Use <see cref="DebugDraw.Arrow"/> instead. </para>
        /// </summary>
        /// <param name="x"> World space position of the arrow base. </param>
        /// <param name="v"> Arrow direction with length. </param>
        /// <param name="color"> Color. </param>
        [Obsolete("PhysicsDebugDisplaySystem.Arrow has been deprecated (RemovedAfter 2024-06-11). Use DebugDraw.Arrow instead.", true)]
        public static void Arrow(float3 x, float3 v, ColorIndex color)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Draws a plane.
        /// <para> Deprecated. Use <see cref="DebugDraw.Plane"/> instead. </para>
        /// </summary>
        /// <param name="x"> Point in world space. </param>
        /// <param name="v"> Normal. </param>
        /// <param name="color"> Color. </param>
        [Obsolete("PhysicsDebugDisplaySystem.Plane has been deprecated (RemovedAfter 2024-06-11). Use DebugDraw.Plane instead.", true)]
        public static void Plane(float3 x, float3 v, ColorIndex color)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Draws an arc.
        /// <para> Deprecated. Use <see cref="DebugDraw.Arc"/> instead. </para>
        /// </summary>
        /// <param name="center"> World space position of the arc center. </param>
        /// <param name="normal"> Arc normal. </param>
        /// <param name="arm"> Arc arm. </param>
        /// <param name="angle"> Arc angle. </param>
        /// <param name="color"> Color. </param>
        [Obsolete("PhysicsDebugDisplaySystem.Arc has been deprecated (RemovedAfter 2024-06-11). Use DebugDraw.Arc instead.", true)]
        public static void Arc(float3 center, float3 normal, float3 arm, float angle,
            Unity.DebugDisplay.ColorIndex color)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Draws a cone.
        /// <para> Deprecated. Use <see cref="DebugDraw.Cone"/> instead. </para>
        /// </summary>
        /// <param name="point"> Point in world space. </param>
        /// <param name="axis"> Cone axis. </param>
        /// <param name="angle"> Cone angle. </param>
        /// <param name="color"> Color. </param>
        [Obsolete("PhysicsDebugDisplaySystem.Cone has been deprecated (RemovedAfter 2024-06-11). Use DebugDraw.Cone instead.", true)]
        public static void Cone(float3 point, float3 axis, float angle, ColorIndex color)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Draws a box.
        /// <para> Deprecated. Use <see cref="DebugDraw.Box"/> instead. </para>
        /// </summary>
        /// <param name="size"> Size of the box. </param>
        /// <param name="center"> Center of the box in world space. </param>
        /// <param name="orientation"> Orientation of the box in world space. </param>
        /// <param name="color"> Color. </param>
        [Obsolete("PhysicsDebugDisplaySystem.Box has been deprecated (RemovedAfter 2024-06-11). Use DebugDraw.Box instead.", true)]
        public static void Box(float3 size, float3 center, quaternion orientation, ColorIndex color)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Draws multiple triangles from the provided data arrays.
        /// <para> Deprecated. Use <see cref="DebugDraw.Triangles(NativeArray{float3}, NativeArray{int}, ColorIndex)"/> instead. </para>
        /// </summary>
        /// <param name="vertices"> An array of vertices. </param>
        /// <param name="triangleIndices"> An array of triangle indices pointing into the vertices array. A triangle is drawn from every triplet of triangle indices. </param>
        /// <param name="color"> Color. </param>
        [Obsolete("PhysicsDebugDisplaySystem.Triangles has been deprecated (RemovedAfter 2024-06-11). Use DebugDraw.Triangles instead.", true)]
        public static void Triangles(in NativeArray<float3> vertices, in NativeArray<int> triangleIndices, ColorIndex color)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Draws multiple triangles from the provided array of triplets of vertices.
        /// <para> Deprecated. Use <see cref="DebugDraw.Triangles(float3*, int, ColorIndex)"/> instead. </para>
        /// </summary>
        /// <param name="vertices"> An array containing a sequence of vertex triplets. A triangle is drawn from every triplet of vertices. </param>
        /// <param name="numVertices"> Number of vertices. </param>
        /// <param name="color"> Color. </param>
        [Obsolete("PhysicsDebugDisplaySystem.Triangles has been deprecated (RemovedAfter 2024-06-11). Use DebugDraw.Triangles instead.", true)]
        public static unsafe void Triangles(float3* vertices, int numVertices, ColorIndex color)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Draws multiple triangles from the provided list of triplets of vertices.
        /// <para> Deprecated. Use <see cref="DebugDraw.Triangles(NativeList{float3}, ColorIndex)"/> instead. </para>
        /// </summary>
        /// <param name="vertices"> A list containing a sequence of vertex triplets. A triangle is drawn from every triplet of vertices. </param>
        /// <param name="color"> Color. </param>
        [Obsolete("PhysicsDebugDisplaySystem.Triangles has been deprecated (RemovedAfter 2024-06-11). Use DebugDraw.Triangles instead.", true)]
        public static void Triangles(in NativeList<float3> vertices, ColorIndex color)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Draws multiple triangles from the provided array of triplets of vertices.
        /// <para> Deprecated. Use <see cref="DebugDraw.Triangles(NativeArray{float3}, ColorIndex)"/> instead. </para>
        /// </summary>
        /// <param name="vertices"> An array containing a sequence of vertex triplets. A triangle is drawn from every triplet of vertices. </param>
        /// <param name="color"> Color. </param>
        [Obsolete("PhysicsDebugDisplaySystem.Triangles has been deprecated (RemovedAfter 2024-06-11). Use DebugDraw.Triangles instead.", true)]
        public static void Triangles(in NativeArray<float3> vertices, ColorIndex color)
        {
            throw new NotImplementedException();
        }
    }
    #endregion

#endif
}
