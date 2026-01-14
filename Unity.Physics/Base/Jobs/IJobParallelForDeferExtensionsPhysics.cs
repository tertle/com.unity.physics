using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;

namespace Unity.Jobs
{
    static class IJobParallelForDeferExtensionsPhysics
    {
        public static JobHandle ScheduleUnsafeIndex0<T>(this T jobData, NativeArray<int> forEachCount, int innerloopBatchCount, JobHandle dependsOn = new JobHandle())
            where T : struct, IJobParallelForDefer
        {
            unsafe
            {
                return IJobParallelForDeferExtensions.Schedule(jobData, (int*)NativeArrayUnsafeUtility.GetUnsafeBufferPointerWithoutChecks(forEachCount), innerloopBatchCount, dependsOn);
            }
        }

        /// <summary>
        /// Schedules a deferred parallel-for job that uses the value of the given <see cref="NativeReference{T}"/> to determine the number of work items.
        /// </summary>
        public static JobHandle ScheduleUnsafe<T>(this T jobData, NativeReference<int> forEachCount, int innerloopBatchCount, JobHandle dependsOn = new JobHandle())
            where T : struct, IJobParallelForDefer
        {
            unsafe
            {
                return IJobParallelForDeferExtensions.Schedule(jobData, NativeReferenceUnsafeUtility.GetUnsafePtrWithoutChecks(forEachCount), innerloopBatchCount, dependsOn);
            }
        }

        /// <summary>
        /// Schedules a deferred parallel-for job that uses the <see cref="NativeStream.ForEachCount">buffers</see> in a <see cref="NativeStream"/> to determine the number of work items.
        /// </summary>
        public static JobHandle ScheduleUnsafe<T>(this T jobData, NativeStream stream, int innerloopBatchCount, JobHandle dependsOn = new JobHandle())
            where T : struct, IJobParallelForDefer
        {
            unsafe
            {
                return IJobParallelForDeferExtensions.Schedule(jobData, (int*)stream.GetUnsafeForEachCountPtr(), innerloopBatchCount, dependsOn);
            }
        }
    }
}
