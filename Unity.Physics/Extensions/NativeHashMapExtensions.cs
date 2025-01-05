// TAKEN FROM BOVINELABS CORE
// https://gitlab.com/tertle/com.bovinelabs.core/-/blob/master/BovineLabs.Core/Extensions/NativeHashMapExtensions.cs

namespace Unity.Physics.Extensions
{
    using System;
    using System.Runtime.CompilerServices;
    using Unity.Assertions;
    using Unity.Collections;
    using Unity.Collections.LowLevel.Unsafe;

    public unsafe static class NativeHashMapExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void RecalculateBuckets<TKey, TValue>(this ref NativeHashMap<TKey, TValue> hashMap, int length)
            where TKey : unmanaged, IEquatable<TKey>
            where TValue : unmanaged
        {
            var data = hashMap.m_Data;

#if ENABLE_UNITY_COLLECTIONS_CHECKS
            AtomicSafetyHandle.CheckWriteAndBumpSecondaryVersion(hashMap.m_Safety);
            Assert.IsTrue(data->Capacity >= length);
#endif
            data->Count = length;
            data->AllocatedIndex = length;

            var buckets = data->Buckets;
            var nextPtrs = data->Next;
            var keys = data->Keys;

            var bucketCapacityMask = data->BucketCapacity - 1;

            for (var idx = 0; idx < length; idx++)
            {
                var bucket = (int)((uint)keys[idx].GetHashCode() & bucketCapacityMask);
                nextPtrs[idx] = buckets[bucket];
                buckets[bucket] = idx;
            }
        }
    }
}
