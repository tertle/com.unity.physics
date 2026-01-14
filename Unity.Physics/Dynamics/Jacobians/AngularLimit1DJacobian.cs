using System.Runtime.CompilerServices;
using Unity.Burst;
using Unity.Collections;
using Unity.Mathematics;
using static Unity.Physics.Math;

namespace Unity.Physics
{
    // Solve data for a constraint that limits one degree of angular freedom
    [NoAlias]
    struct AngularLimit1DJacobian
    {
        // Limited axis in motion A space
        public float3 AxisInMotionA;

        // Index of the limited axis
        public int AxisIndex;

        // Relative angle limits
        public float MinAngle;
        public float MaxAngle;

        // Motion A orientation in world space
        public quaternion WorldFromMotionA;

        // Relative orientation of the motions before solving
        public quaternion MotionBFromA;

        // Rotation to joint space from motion space
        public quaternion MotionAFromJoint;
        public quaternion MotionBFromJoint;

        // Error before solving
        public float InitialError;

        // Fraction of the position error to correct per step
        public float Tau;

        // Fraction of the velocity error to correct per step
        public float Damping;

        // Build the Jacobian
        public void Build(
            MTransform aFromConstraint, MTransform bFromConstraint,
            MotionVelocity velocityA, MotionVelocity velocityB,
            MotionData motionA, MotionData motionB,
            Constraint constraint, float tau, float damping)
        {
            // Copy the constraint into the jacobian
            AxisIndex = constraint.ConstrainedAxis1D;
            AxisInMotionA = aFromConstraint.Rotation[AxisIndex];
            MinAngle = constraint.Min;
            MaxAngle = constraint.Max;
            Tau = tau;
            Damping = damping;

            MotionAFromJoint = new quaternion(aFromConstraint.Rotation);
            MotionBFromJoint = new quaternion(bFromConstraint.Rotation);

            Update(motionA, motionB);
        }

        public void Update(in MotionData motionA, in MotionData motionB)
        {
            MotionBFromA = math.mul(math.inverse(motionB.WorldFromMotion.rot), motionA.WorldFromMotion.rot);

            WorldFromMotionA = motionA.WorldFromMotion.rot;

            // Calculate the current error
            InitialError = CalculateError(MotionBFromA);
        }

        // Solve the Jacobian
        public void Solve(ref JacobianHeader jacHeader, ref MotionVelocity velocityA, ref MotionVelocity velocityB,
            Solver.StepInput stepInput, ref NativeStream.Writer impulseEventsWriter)
        {
            // Predict the relative orientation at the end of the step
            quaternion futureMotionBFromA = JacobianUtilities.IntegrateOrientationBFromA(MotionBFromA, velocityA.AngularVelocity, velocityB.AngularVelocity, stepInput.Timestep);

            // Calculate the effective mass
            float3 axisInMotionB = math.mul(futureMotionBFromA, -AxisInMotionA);
            float effectiveMass;
            {
                float invEffectiveMass = math.csum(AxisInMotionA * AxisInMotionA * velocityA.InverseInertia +
                    axisInMotionB * axisInMotionB * velocityB.InverseInertia);
                effectiveMass = math.select(1.0f / invEffectiveMass, 0.0f, invEffectiveMass == 0.0f);
            }

            // Calculate the error, adjust by tau and damping, and apply an impulse to correct it
            float futureError = CalculateError(futureMotionBFromA);
            float solveError = JacobianUtilities.CalculateCorrection(futureError, InitialError, Tau, Damping);
            float impulse = math.mul(effectiveMass, -solveError) * Solver.CalculateInvTimestep(stepInput.Timestep);
            velocityA.ApplyAngularImpulse(impulse * AxisInMotionA);
            velocityB.ApplyAngularImpulse(impulse * axisInMotionB);

            if ((jacHeader.Flags & JacobianFlags.EnableImpulseEvents) != 0)
            {
                HandleImpulseEvent(ref jacHeader, impulse, stepInput.ExportEventsInThisIteration,
                    ref impulseEventsWriter);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float CalculateAngle(quaternion motionBFromA)
        {
            // Calculate the relative joint frame rotation
            quaternion jointBFromA = math.mul(math.mul(math.inverse(MotionBFromJoint), motionBFromA), MotionAFromJoint);

            // Find the twist angle of the rotation.
            //
            // There is no one correct solution for the twist angle. Suppose the joint models a pair of bodies connected by
            // three gimbals, one of which is limited by this jacobian. There are multiple configurations of the gimbals that
            // give the bodies the same relative orientation, so it is impossible to determine the configuration from the
            // bodies' orientations alone, nor therefore the orientation of the limited gimbal.
            //
            // This code instead makes a reasonable guess, the twist angle of the swing-twist decomposition of the bodies'
            // relative orientation. It always works when the limited axis itself is unable to rotate freely, as in a limited
            // hinge. It works fairly well when the limited axis can only rotate a small amount, preferably less than 90
            // degrees. It works poorly at higher angles, especially near 180 degrees where it is not continuous. For systems
            // that require that kind of flexibility, the gimbals should be modeled as separate bodies.
            float angle = CalculateTwistAngle(jointBFromA, AxisIndex);

            // Angle is in [-2pi, 2pi].
            // For comparison against the limits, find k so that angle + 2k * pi is as close to [min, max] as possible.
            float centerAngle = (MinAngle + MaxAngle) / 2.0f;
            bool above = angle > (centerAngle + (float)math.PI);
            bool below = angle < (centerAngle - (float)math.PI);
            angle = math.select(angle, angle - 2.0f * (float)math.PI, above);
            angle = math.select(angle, angle + 2.0f * (float)math.PI, below);

            return angle;
        }

        // Helper function
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private float CalculateError(quaternion motionBFromA)
        {
            // Calculate the current angle
            var angle = CalculateAngle(motionBFromA);
            // Calculate the error from the limits
            var error = JacobianUtilities.CalculateError(angle, MinAngle, MaxAngle);
            //UnityEngine.Debug.Log($"AngularLimit1DJacobian.CalculateError: angle = {angle}, error = {error}");
            return error;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void HandleImpulseEvent(ref JacobianHeader jacHeader, float impulse, bool exportEvent, ref NativeStream.Writer impulseEventsWriter)
        {
            ref ImpulseEventSolverData impulseEventData = ref jacHeader.AccessImpulseEventSolverData();
            impulseEventData.AccumulatedImpulse[AxisIndex] += impulse;

            if (exportEvent && math.any(math.abs(impulseEventData.AccumulatedImpulse) > impulseEventData.MaxImpulse))
            {
                impulseEventsWriter.Write(new ImpulseEventData
                {
                    Type = ConstraintType.Angular,
                    Impulse = impulseEventData.AccumulatedImpulse,
                    JointEntity = impulseEventData.JointEntity,
                    BodyIndices = jacHeader.BodyPair
                });
            }
        }
    }
}
