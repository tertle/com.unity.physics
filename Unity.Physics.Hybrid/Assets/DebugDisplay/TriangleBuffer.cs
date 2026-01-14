using System;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Mathematics;

namespace Unity.DebugDisplay
{
    struct TriangleBuffer : IDisposable
    {
        NativeList<Instance> m_Buffer;
        NativeReference<Unit> m_BufferAllocations;

        internal struct Instance
        {
            internal float4 m_vertex0;
            internal float4 m_vertex1;
            internal float4 m_vertex2;
            internal float3 normal;
        }

        internal void Initialize(int size)
        {
            m_Buffer = new NativeList<Instance>(size, Allocator.Persistent);
            m_Buffer.Resize(size, NativeArrayOptions.UninitializedMemory);
            m_BufferAllocations = new NativeReference<Unit>(Allocator.Persistent);
        }

        internal void SetTriangle(float3 vertex0, float3 vertex1, float3 vertex2, float3 normal, Unity.DebugDisplay.ColorIndex colorIndex, int index)
        {
            m_Buffer[index] = new Instance
            {
                m_vertex0 = new float4(vertex0, colorIndex.value),
                m_vertex1 = new float4(vertex1, colorIndex.value),
                m_vertex2 = new float4(vertex2, colorIndex.value),
                normal = new float3(normal.x, normal.y, normal.z)
            };
        }

        internal int Size => m_Buffer.Length;
        internal int Filled => m_BufferAllocations.Value.Filled;
        internal bool ResizeRequired => m_BufferAllocations.Value.m_ResizeRequired;

        internal void ClearTriangle(int index)
        {
            m_Buffer[index] = new Instance {};
        }

        internal NativeArray<Instance> AsArray()
        {
            return m_Buffer.AsArray();
        }

        public void Dispose()
        {
            m_Buffer.Dispose();
            m_BufferAllocations.Dispose();
        }

        internal Unit AllocateAtomic(int count)
        {
            Physics.SafetyChecks.CheckAreEqualAndThrow(m_BufferAllocations.IsCreated, true);

            unsafe
            {
                return m_BufferAllocations.GetUnsafePtrWithoutChecks()->AllocateAtomic(count);
            }
        }

        internal void AllocateAll()
        {
            Physics.SafetyChecks.CheckAreEqualAndThrow(m_BufferAllocations.IsCreated, true);

            m_BufferAllocations.Value = new Unit(m_Buffer.Length);
        }
    }
}
