#if PHYSICS_ENABLE_PERF_TESTS
using NUnit.Framework;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Jobs;
using Unity.Mathematics;
using Unity.PerformanceTesting;
using Assert = UnityEngine.Assertions.Assert;
using Random = UnityEngine.Random;

namespace Unity.Physics.Tests.PerformanceTests
{
    class RadixSortTests
    {
        //@TODO: Make part of NativeArray API
        unsafe static NativeArray<U> ReinterpretCast<T, U>(NativeArray<T> array)
            where T : struct
            where U : struct
        {
            Assert.AreEqual(UnsafeUtility.SizeOf<T>(), UnsafeUtility.SizeOf<U>());

            var castedArray = NativeArrayUnsafeUtility.ConvertExistingDataToNativeArray<U>((byte*)array.GetUnsafePtr(), array.Length, Allocator.Invalid);
#if ENABLE_UNITY_COLLECTIONS_CHECKS
            NativeArrayUnsafeUtility.SetAtomicSafetyHandle(ref castedArray, NativeArrayUnsafeUtility.GetAtomicSafetyHandle(array));
#endif
            return castedArray;
        }

        public static void InitPairs(int minIndex, int maxIndex, int count, NativeArray<DispatchPairSequencer.DispatchPair> pairs)
        {
            Random.InitState(1234);

            for (var i = 0; i < pairs.Length; ++i)
            {
                var indexA = Random.Range(minIndex, maxIndex);
                var indexB = Random.Range(minIndex, maxIndex);

                if (indexB == indexA)
                {
                    if (indexB < maxIndex)
                    {
                        indexB++;
                    }
                }

                pairs[i] = DispatchPairSequencer.DispatchPair.CreateCollisionPair(new Broadphase.OverlapResult(indexA, indexB));
            }
        }

        [Test, Performance]
        [TestCase(1, TestName = "PerfRadixPassOnBodyA 1")]
        [TestCase(10, TestName = "PerfRadixPassOnBodyA 10")]
        [TestCase(100, TestName = "PerfRadixPassOnBodyA 100")]
        [TestCase(1000, TestName = "PerfRadixPassOnBodyA 1000")]
        [TestCase(10000, TestName = "PerfRadixPassOnBodyA 10 000")]
        [TestCase(100000, TestName = "PerfRadixPassOnBodyA 100 000")]
        public void PerfRadixPassOnBodyA(int count)
        {
            int maxBodyIndex = (int)math.pow(count, 0.7f);

            var pairs = new NativeArray<DispatchPairSequencer.DispatchPair>(count, Allocator.TempJob);
            var sortedPairs = new NativeArray<DispatchPairSequencer.DispatchPair>(count, Allocator.TempJob);
            var tempCount = new NativeArray<int>(maxBodyIndex + 1, Allocator.TempJob);

            InitPairs(1, maxBodyIndex, count, pairs);

            var job = new DispatchPairSequencer.RadixSortPerBodyAJob
            {
                InputArray = pairs,
                OutputArray = sortedPairs,
                BodyIndexHistogram = tempCount,
                MaxIndex = maxBodyIndex
            };

            Measure.Method(() =>
            {
                job.Run();
            })
                .MeasurementCount(1)
                .Run();

            for (int i = 0; i < count - 1; i++)
            {
                Assert.IsTrue((sortedPairs[i].BodyIndexA) <= (sortedPairs[i + 1].BodyIndexA),
                    $"Not sorted for index {i}, sortedPairs[i].BodyIndexA= {sortedPairs[i].BodyIndexA}," +
                    $"sortedPairs[i+1].BodyIndexA= {sortedPairs[i + 1].BodyIndexA}");
            }

            // Dispose all allocated data.
            pairs.Dispose();
            sortedPairs.Dispose();
            tempCount.Dispose();
        }

        [Test, Performance]
        [TestCase(1, TestName = "PerfDefaultSortOnSubarrays 1")]
        [TestCase(10, TestName = "PerfDefaultSortOnSubarrays 10")]
        [TestCase(100, TestName = "PerfDefaultSortOnSubarrays 100")]
        [TestCase(1000, TestName = "PerfDefaultSortOnSubarrays 1000")]
        [TestCase(10000, TestName = "PerfDefaultSortOnSubarrays 10 000")]
        [TestCase(100000, TestName = "PerfDefaultSortOnSubarrays 100 000")]
        public unsafe void PerfDefaultSortOnSubarrays(int count)
        {
            int maxBodyIndex = (int)math.pow(count, 0.7f);

            var pairs = new NativeArray<DispatchPairSequencer.DispatchPair>(count, Allocator.TempJob);
            var sortedPairs = new NativeArray<DispatchPairSequencer.DispatchPair>(count, Allocator.TempJob);

            InitPairs(1, maxBodyIndex, count, pairs);

            // Do a single pass of radix sort on bodyA only.
            var tempCount = new NativeArray<int>(maxBodyIndex + 1, Allocator.TempJob);
            DispatchPairSequencer.RadixSortPerBodyAJob.RadixSortPerBodyA(pairs, sortedPairs,
                tempCount, maxBodyIndex);

            var job = new DispatchPairSequencer.SortSubArraysJob
            {
                InOutArray = sortedPairs,
                NextElementIndex = tempCount
            };

            Measure.Method(() =>
            {
                job.Run(tempCount.Length);
            })
                .MeasurementCount(1)
                .Run();

            // Mask all bits NOT associated with the BodyA index

            for (int i = 0; i < count - 1; i++)
            {
                Assert.IsTrue((sortedPairs[i].BodyIndexA) < (sortedPairs[i + 1].BodyIndexA) ||
                    (sortedPairs[i].BodyIndexASubArraySortKey <= sortedPairs[i + 1].BodyIndexASubArraySortKey),
                    $"Not sorted for index {i}, sortedPairs[i] = {sortedPairs[i]}, sortedPairs[i+1] = {sortedPairs[i + 1]}");
            }

            // Dispose all allocated data.
            pairs.Dispose();
            sortedPairs.Dispose();
        }
    }
}
#endif
