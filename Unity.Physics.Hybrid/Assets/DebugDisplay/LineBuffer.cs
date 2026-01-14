using System;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Mathematics;

namespace Unity.DebugDisplay
{
    struct LineBuffer : IDisposable
    {
        NativeList<Instance> m_Buffer;
        NativeReference<Unit> m_BufferAllocations;

        internal struct Instance
        {
            internal float4 m_Begin;
            internal float4 m_End;
        }

        internal void Initialize(int size)
        {
            m_Buffer = new NativeList<Instance>(size, Allocator.Persistent);
            m_Buffer.Resize(size, NativeArrayOptions.UninitializedMemory);
            m_BufferAllocations = new NativeReference<Unit>(Allocator.Persistent);
        }

        internal void SetLine(float3 begin, float3 end, ColorIndex colorIndex, int index)
        {
            m_Buffer[index] = new Instance
            {
                m_Begin = new float4(begin.x, begin.y, begin.z, colorIndex.value),
                m_End = new float4(end.x, end.y, end.z, colorIndex.value)
            };
        }

        internal int Size => m_Buffer.Length;
        internal int Filled => m_BufferAllocations.Value.Filled;
        internal bool ResizeRequired => m_BufferAllocations.Value.m_ResizeRequired;

        internal void ClearLine(int index)
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
