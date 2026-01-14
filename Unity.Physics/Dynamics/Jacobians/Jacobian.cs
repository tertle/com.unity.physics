using System;
using System.Runtime.CompilerServices;
using Unity.Burst;
using Unity.Collections;
using Unity.Collections.LowLevel.Unsafe;
using Unity.Mathematics;
using UnityEngine.Assertions;

namespace Unity.Physics
{
    /// <summary>   Values that represent jacobian types. </summary>
    public enum JacobianType : byte
    {
        // Contact Jacobians
        /// <summary>   An enum constant representing the contact jacobian. </summary>
        Contact,
        /// <summary>   An enum constant representing the trigger jacobian. </summary>
        Trigger,

        // Joint Jacobians
        /// <summary>   An enum constant representing the linear limit joint jacobian. </summary>
        LinearLimit,
        /// <summary>   An enum constant representing the angular limit 1 d joint jacobian. </summary>
        AngularLimit1D,
        /// <summary>   An enum constant representing the angular limit 2D joint jacobian. </summary>
        AngularLimit2D,
        /// <summary>   An enum constant representing the angular limit 3D joint jacobian. </summary>
        AngularLimit3D,

        // Motor Jacobians
        /// <summary>   An enum constant representing the rotation motor jacobian. </summary>
        RotationMotor,
        /// <summary>   An enum constant representing the angular velocity motor jacobian. </summary>
        AngularVelocityMotor,
        /// <summary>   An enum constant representing the position motor jacobian. </summary>
        PositionMotor,
        /// <summary>   An enum constant representing the linear velocity motor jacobian. </summary>
        LinearVelocityMotor,
    }

    /// <summary>   Flags which enable optional Jacobian behaviors. </summary>
    [Flags]
    public enum JacobianFlags : byte
    {
        // These flags apply to all Jacobians
        /// <summary>   A binary constant representing the disabled flag. Applies to all jacobians. </summary>
        Disabled = 1 << 0,
        /// <summary>   A binary constant representing the enable mass factors flag. Applies to all jacobians.</summary>
        EnableMassFactors = 1 << 1,

        // These flags apply only to contact Jacobians:
        /// <summary> A binary constant representing whether both at least one of the two bodies in this contact is dynamic. </summary>
        IsContactDynamic = 1 << 3,
        /// <summary>
        /// A binary constant representing the detailed static mesh collision option which prevents ghost collisions
        /// with static meshes by further analyzing the colliders' trajectory at the contact point.
        /// </summary>
        EnableDetailedStaticMeshCollision = 1 << 4,
        /// <summary>   A binary constant representing the is trigger flag. Apples only to contact jacobian. </summary>
        IsTrigger = 1 << 5,
        /// <summary>   A binary constant representing the enable collision events flag. Apples only to contact jacobian. </summary>
        EnableCollisionEvents = 1 << 6,
        /// <summary>   A binary constant representing the enable surface velocity flag. Apples only to contact jacobian. </summary>
        EnableSurfaceVelocity = 1 << 7,

        // Applies only to joint Jacobians:
        /// <summary>   A binary constant representing the enable impulse events options. Apples only to joint jacobian. </summary>
        EnableImpulseEvents = 1 << 5,

        /// <summary>   A binary constant representing the user flag. Applies to all jacobians. </summary>
        UserFlag0 = 1 << 2,
    }

    /// <summary>
    /// Constraint block info, used for identifying a constraint within a <see cref="ConstraintBlock3">constraint block</see>,
    /// represented by a series of <see cref="JacobianHeader">Jacobians</see>.
    /// <para/>
    /// Provides the <see cref="Index">location</see> of the constraint in the block and the block's total
    /// <see cref="Length">constraint count</see>.
    /// The maximum number of constraints in a <see cref="ConstraintBlock3">constraint block</see> is 3.
    /// </summary>
    struct ConstraintBlockInfo
    {
        static readonly byte k_IndexMask = 0b_0000_1111;
        static readonly byte k_IndexBitCount = (byte)math.countbits((int)k_IndexMask);
        byte m_Data;

        /// <summary> The constraint's index in the <see cref="ConstraintBlock3">constraint block</see>. </summary>
        public int Index
        {
            get => m_Data & k_IndexMask;
            set => m_Data = (byte)(m_Data & ~k_IndexMask | (byte)value & k_IndexMask);
        }

        /// <summary> Length of the <see cref="ConstraintBlock3">constraint block</see> the constraint belongs to. </summary>
        public int Length
        {
            get => m_Data >> k_IndexBitCount;
            set => m_Data = (byte)(m_Data & k_IndexMask | (byte)value << k_IndexBitCount);
        }
    }

    // Jacobian header, first part of each Jacobian in the stream
    struct JacobianHeader
    {
        public BodyIndexPair BodyPair { get; internal set; }
        public JacobianType Type { get; internal set; }
        public JacobianFlags Flags { get; internal set; }

        internal ConstraintBlockInfo ConstraintBlockInfo;

        // Whether the Jacobian should be solved or not
        public bool Enabled
        {
            get => ((Flags & JacobianFlags.Disabled) == 0);
            set => Flags = value ? (Flags & ~JacobianFlags.Disabled) : (Flags | JacobianFlags.Disabled);
        }

        public bool IsAngular => Type >= JacobianType.AngularLimit1D && Type <= JacobianType.AngularVelocityMotor;

        public bool IsVelocityConstraint => Type == JacobianType.AngularVelocityMotor || Type == JacobianType.LinearVelocityMotor;

        // Whether the Jacobian contains manifold data for collision events or not
        public bool HasContactManifold => (Flags & JacobianFlags.EnableCollisionEvents) != 0;

        // Whether the Jacobian contains the necessary data to process a detailed static mesh contact or not
        public bool HasDetailedStaticMeshCollision => (Flags & JacobianFlags.EnableDetailedStaticMeshCollision) != 0;

        // Whether this contact contains at least one dynamic body
        public bool IsContactDynamic => (Flags & JacobianFlags.IsContactDynamic) != 0;

        // Collider keys for the collision events
        public ColliderKeyPair ColliderKeys
        {
            get => HasContactManifold? AccessColliderKeys() : ColliderKeyPair.Empty;
            set
            {
                if (HasContactManifold)
                    AccessColliderKeys() = value;
                else
                    SafetyChecks.ThrowNotSupportedException("Jacobian does not have collision events enabled");
            }
        }

        // Overrides for the mass properties of the pair of bodies
        public bool HasMassFactors => (Flags & JacobianFlags.EnableMassFactors) != 0;
        public MassFactors MassFactors
        {
            get => HasMassFactors? AccessMassFactors() : MassFactors.Default;
            set
            {
                if (HasMassFactors)
                    AccessMassFactors() = value;
                else
                    SafetyChecks.ThrowNotSupportedException("Jacobian does not have mass factors enabled");
            }
        }

        // The surface velocity to apply to contact points
        public bool HasSurfaceVelocity => (Flags & JacobianFlags.EnableSurfaceVelocity) != 0;
        public SurfaceVelocity SurfaceVelocity
        {
            get => HasSurfaceVelocity? AccessSurfaceVelocity() : new SurfaceVelocity();
            set
            {
                if (HasSurfaceVelocity)
                    AccessSurfaceVelocity() = value;
                else
                    SafetyChecks.ThrowNotSupportedException("Jacobian does not have surface velocity enabled");
            }
        }

        // Direct solver regularization parameters
        public struct DirectSolverRegularizationData
        {
            public float Epsilon;
            public float Gamma;
        }

        // Update the MotionData of the Jacobians when substepping. This is used when there are more than one substeps,
        // prior to solver iteration and is needed so that the Jacobian Solve methods have access to up-to-date motion data.
        public void UpdateContact(in MotionVelocity velocityA, in MotionVelocity velocityB, in Math.MTransform worldFromA,
            in Math.MTransform worldFromB, Solver.StepInput stepInput)
        {
            SafetyChecks.CheckAreEqualAndThrow(true, Type == JacobianType.Contact);
            if (Enabled)
            {
                AccessBaseJacobian<ContactJacobian>().Update(ref this, in velocityA, in velocityB,
                    in worldFromA, in worldFromB, stepInput);
            }
        }

        public void UpdateJoints(in MotionData motionDataA, in MotionData motionDataB)
        {
            if (Enabled)
            {
                switch (Type)
                {
                    case JacobianType.Contact:
                    case JacobianType.Trigger:
                        break;
                    case JacobianType.LinearLimit:
                        AccessBaseJacobian<LinearLimitJacobian>().Update(in motionDataA, in motionDataB);
                        break;
                    case JacobianType.AngularLimit1D:
                        AccessBaseJacobian<AngularLimit1DJacobian>().Update(in motionDataA, in motionDataB);
                        break;
                    case JacobianType.AngularLimit2D:
                        AccessBaseJacobian<AngularLimit2DJacobian>().Update(in motionDataA, in motionDataB);
                        break;
                    case JacobianType.AngularLimit3D:
                        AccessBaseJacobian<AngularLimit3DJacobian>().Update(in motionDataA, in motionDataB);
                        break;
                    case JacobianType.RotationMotor:
                        AccessBaseJacobian<RotationMotorJacobian>().Update(in motionDataA, in motionDataB);
                        break;
                    case JacobianType.AngularVelocityMotor:
                        AccessBaseJacobian<AngularVelocityMotorJacobian>().Update(in motionDataA, in motionDataB);
                        break;
                    case JacobianType.PositionMotor:
                        AccessBaseJacobian<PositionMotorJacobian>().Update(in motionDataA, in motionDataB);
                        break;
                    case JacobianType.LinearVelocityMotor:
                        AccessBaseJacobian<LinearVelocityMotorJacobian>().Update(in motionDataA, in motionDataB);
                        break;
                    default:
                        SafetyChecks.ThrowNotImplementedException();
                        return;
                }
            }
        }

        // Solve the Jacobian
        public void Solve([NoAlias] ref MotionVelocity velocityA, [NoAlias] ref MotionVelocity velocityB, Solver.StepInput stepInput,
            [NoAlias] ref NativeStream.Writer collisionEventsWriter, [NoAlias] ref NativeStream.Writer triggerEventsWriter,
            [NoAlias] ref NativeStream.Writer impulseEventsWriter, bool enableFrictionVelocitiesHeuristic,
            Solver.MotionStabilizationInput motionStabilizationSolverInputA, Solver.MotionStabilizationInput motionStabilizationSolverInputB)
        {
            if (Enabled)
            {
                switch (Type)
                {
                    case JacobianType.Contact:
                        AccessBaseJacobian<ContactJacobian>().Solve(ref this, ref velocityA, ref velocityB, stepInput, ref collisionEventsWriter,
                            enableFrictionVelocitiesHeuristic, motionStabilizationSolverInputA, motionStabilizationSolverInputB);
                        break;
                    case JacobianType.Trigger:
                        AccessBaseJacobian<TriggerJacobian>().Solve(ref this, ref velocityA, ref velocityB, stepInput, ref triggerEventsWriter);
                        break;
                    case JacobianType.LinearLimit:
                        AccessBaseJacobian<LinearLimitJacobian>().Solve(ref this, ref velocityA, ref velocityB, stepInput, ref impulseEventsWriter);
                        break;
                    case JacobianType.AngularLimit1D:
                        AccessBaseJacobian<AngularLimit1DJacobian>().Solve(ref this, ref velocityA, ref velocityB, stepInput, ref impulseEventsWriter);
                        break;
                    case JacobianType.AngularLimit2D:
                        AccessBaseJacobian<AngularLimit2DJacobian>().Solve(ref this, ref velocityA, ref velocityB, stepInput, ref impulseEventsWriter);
                        break;
                    case JacobianType.AngularLimit3D:
                        AccessBaseJacobian<AngularLimit3DJacobian>().Solve(ref this, ref velocityA, ref velocityB, stepInput, ref impulseEventsWriter);
                        break;
                    case JacobianType.RotationMotor:
                        AccessBaseJacobian<RotationMotorJacobian>().Solve(ref this, ref velocityA, ref velocityB, stepInput);
                        break;
                    case JacobianType.AngularVelocityMotor:
                        AccessBaseJacobian<AngularVelocityMotorJacobian>().Solve(ref this, ref velocityA, ref velocityB, stepInput);
                        break;
                    case JacobianType.PositionMotor:
                        AccessBaseJacobian<PositionMotorJacobian>().Solve(ref this, ref velocityA, ref velocityB, stepInput);
                        break;
                    case JacobianType.LinearVelocityMotor:
                        AccessBaseJacobian<LinearVelocityMotorJacobian>().Solve(ref this, ref velocityA, ref velocityB, stepInput);
                        break;
                    default:
                        SafetyChecks.ThrowNotImplementedException();
                        return;
                }
            }
        }

        #region Helpers

        public static bool IsNonMotorizedConstraint(JacobianType type)
        {
            return type >= JacobianType.LinearLimit && type <= JacobianType.RotationMotor;
        }

        public static int CalculateSize(JacobianType type, JacobianFlags flags, int numContactPoints = 0)
        {
            return UnsafeUtility.SizeOf<JacobianHeader>() +
                SizeOfBaseJacobian(type) + SizeOfModifierData(type, flags) +
                numContactPoints * UnsafeUtility.SizeOf<ContactJacAngAndVelToReachCp>() +
                SizeOfContactPointData(type, flags, numContactPoints);
        }

        public static int CalculateJointSize(JacobianType type, JacobianFlags flags, SolverType solverType)
        {
            var baseJointSize = CalculateSize(type, flags, numContactPoints: 0);
            return solverType == SolverType.Direct ?
                baseJointSize + UnsafeUtility.SizeOf<DirectSolverRegularizationData>() : // extra solver data for joints processed by direct solver
                baseJointSize;
        }

        private static int SizeOfColliderKeys(JacobianType type, JacobianFlags flags)
        {
            return (type == JacobianType.Contact && (flags & JacobianFlags.EnableCollisionEvents) != 0) ?
                UnsafeUtility.SizeOf<ColliderKeyPair>() : 0;
        }

        private static int SizeOfEntityPair(JacobianType type, JacobianFlags flags)
        {
            return (type == JacobianType.Contact && (flags & JacobianFlags.EnableCollisionEvents) != 0) ?
                UnsafeUtility.SizeOf<EntityPair>() : 0;
        }

        private static int SizeOfSurfaceVelocity(JacobianType type, JacobianFlags flags)
        {
            return (type == JacobianType.Contact && (flags & JacobianFlags.EnableSurfaceVelocity) != 0) ?
                UnsafeUtility.SizeOf<SurfaceVelocity>() : 0;
        }

        private static int SizeOfMassFactors(JacobianType type, JacobianFlags flags)
        {
            return (type == JacobianType.Contact && (flags & JacobianFlags.EnableMassFactors) != 0) ?
                UnsafeUtility.SizeOf<MassFactors>() : 0;
        }

        private static int SizeOfJacobianContactCollidersData(JacobianType type, JacobianFlags flags)
        {
            return (type == JacobianType.Contact && (flags & JacobianFlags.EnableDetailedStaticMeshCollision) != 0) ?
                UnsafeUtility.SizeOf<ContactJacobianPolygonData>() : 0;
        }

        private static int SizeOfModifierData(JacobianType type, JacobianFlags flags)
        {
            return SizeOfColliderKeys(type, flags) + SizeOfEntityPair(type, flags) + SizeOfSurfaceVelocity(type, flags) +
                SizeOfMassFactors(type, flags) + SizeOfImpulseEventSolverData(type, flags) + SizeOfJacobianContactCollidersData(type, flags);
        }

        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        private static int SizeOfImpulseEventSolverData(JacobianType type, JacobianFlags flags)
        {
            return (IsNonMotorizedConstraint(type) && (flags & JacobianFlags.EnableImpulseEvents) != 0) ?
                UnsafeUtility.SizeOf<ImpulseEventSolverData>() : 0;
        }

        private static int SizeOfContactPointData(JacobianType type, JacobianFlags flags, int numContactPoints = 0)
        {
            return (type == JacobianType.Contact && (flags & JacobianFlags.EnableCollisionEvents) != 0) ?
                numContactPoints * UnsafeUtility.SizeOf<ContactPoint>() : 0;
        }

        private static int SizeOfBaseJacobian(JacobianType type)
        {
            switch (type)
            {
                case JacobianType.Contact:
                    return UnsafeUtility.SizeOf<ContactJacobian>();
                case JacobianType.Trigger:
                    return UnsafeUtility.SizeOf<TriggerJacobian>();
                case JacobianType.LinearLimit:
                    return UnsafeUtility.SizeOf<LinearLimitJacobian>();
                case JacobianType.AngularLimit1D:
                    return UnsafeUtility.SizeOf<AngularLimit1DJacobian>();
                case JacobianType.AngularLimit2D:
                    return UnsafeUtility.SizeOf<AngularLimit2DJacobian>();
                case JacobianType.AngularLimit3D:
                    return UnsafeUtility.SizeOf<AngularLimit3DJacobian>();
                case JacobianType.RotationMotor:
                    return UnsafeUtility.SizeOf<RotationMotorJacobian>();
                case JacobianType.AngularVelocityMotor:
                    return UnsafeUtility.SizeOf<AngularVelocityMotorJacobian>();
                case JacobianType.PositionMotor:
                    return UnsafeUtility.SizeOf<PositionMotorJacobian>();
                case JacobianType.LinearVelocityMotor:
                    return UnsafeUtility.SizeOf<LinearVelocityMotorJacobian>();
                default:
                    SafetyChecks.ThrowNotImplementedException();
                    return default;
            }
        }

        // Access to "base" jacobian - a jacobian that comes after the header
        public unsafe ref T AccessBaseJacobian<T>() where T : struct
        {
            byte* ptr = (byte*)UnsafeUtility.AddressOf(ref this);
            ptr += UnsafeUtility.SizeOf<JacobianHeader>();
            return ref UnsafeUtility.AsRef<T>(ptr);
        }

        public unsafe ref ColliderKeyPair AccessColliderKeys()
        {
            Assert.IsTrue((Flags & JacobianFlags.EnableCollisionEvents) != 0);
            byte* ptr = (byte*)UnsafeUtility.AddressOf(ref this);
            ptr += UnsafeUtility.SizeOf<JacobianHeader>() + SizeOfBaseJacobian(Type);
            return ref UnsafeUtility.AsRef<ColliderKeyPair>(ptr);
        }

        public unsafe ref EntityPair AccessEntities()
        {
            Assert.IsTrue((Flags & JacobianFlags.EnableCollisionEvents) != 0);
            byte* ptr = (byte*)UnsafeUtility.AddressOf(ref this);
            ptr += UnsafeUtility.SizeOf<JacobianHeader>() + SizeOfBaseJacobian(Type) + SizeOfColliderKeys(Type, Flags);
            return ref UnsafeUtility.AsRef<EntityPair>(ptr);
        }

        public unsafe ref SurfaceVelocity AccessSurfaceVelocity()
        {
            Assert.IsTrue((Flags & JacobianFlags.EnableSurfaceVelocity) != 0);
            byte* ptr = (byte*)UnsafeUtility.AddressOf(ref this);
            ptr += UnsafeUtility.SizeOf<JacobianHeader>() + SizeOfBaseJacobian(Type) +
                SizeOfColliderKeys(Type, Flags) + SizeOfEntityPair(Type, Flags);
            return ref UnsafeUtility.AsRef<SurfaceVelocity>(ptr);
        }

        public unsafe ref MassFactors AccessMassFactors()
        {
            Assert.IsTrue((Flags & JacobianFlags.EnableMassFactors) != 0);
            byte* ptr = (byte*)UnsafeUtility.AddressOf(ref this);
            ptr += UnsafeUtility.SizeOf<JacobianHeader>() + SizeOfBaseJacobian(Type) +
                SizeOfColliderKeys(Type, Flags) + SizeOfEntityPair(Type, Flags) + SizeOfSurfaceVelocity(Type, Flags);
            return ref UnsafeUtility.AsRef<MassFactors>(ptr);
        }

        public unsafe ref ContactJacAngAndVelToReachCp AccessAngularJacobian(int pointIndex)
        {
            Assert.IsTrue(Type == JacobianType.Contact || Type == JacobianType.Trigger);
            byte* ptr = (byte*)UnsafeUtility.AddressOf(ref this);
            ptr += UnsafeUtility.SizeOf<JacobianHeader>() + SizeOfBaseJacobian(Type) + SizeOfModifierData(Type, Flags) +
                pointIndex * UnsafeUtility.SizeOf<ContactJacAngAndVelToReachCp>();
            return ref UnsafeUtility.AsRef<ContactJacAngAndVelToReachCp>(ptr);
        }

        public unsafe ref ContactPoint AccessContactPoint(int pointIndex)
        {
            Assert.IsTrue(Type == JacobianType.Contact);

            var baseJac = AccessBaseJacobian<ContactJacobian>();
            byte* ptr = (byte*)UnsafeUtility.AddressOf(ref this);
            ptr += UnsafeUtility.SizeOf<JacobianHeader>() + SizeOfBaseJacobian(Type) + SizeOfModifierData(Type, Flags) +
                baseJac.BaseJacobian.NumContacts * UnsafeUtility.SizeOf<ContactJacAngAndVelToReachCp>() +
                pointIndex * UnsafeUtility.SizeOf<ContactPoint>();
            return ref UnsafeUtility.AsRef<ContactPoint>(ptr);
        }

        public unsafe ref ImpulseEventSolverData AccessImpulseEventSolverData()
        {
            Assert.IsTrue(IsNonMotorizedConstraint(Type) && ((Flags & JacobianFlags.EnableImpulseEvents) != 0));

            byte* ptr = (byte*)UnsafeUtility.AddressOf(ref this);
            ptr += UnsafeUtility.SizeOf<JacobianHeader>() + SizeOfBaseJacobian(Type);
            return ref UnsafeUtility.AsRef<ImpulseEventSolverData>(ptr);
        }

        public unsafe ref ContactJacobianPolygonData AccessJacobianContactData()
        {
            Assert.IsTrue((Flags & JacobianFlags.EnableDetailedStaticMeshCollision) != 0);
            byte* ptr = (byte*)UnsafeUtility.AddressOf(ref this);
            ptr += UnsafeUtility.SizeOf<JacobianHeader>() + SizeOfBaseJacobian(Type) +
                SizeOfColliderKeys(Type, Flags) + SizeOfEntityPair(Type, Flags) + SizeOfSurfaceVelocity(Type, Flags) +
                SizeOfMassFactors(Type, Flags);
            return ref UnsafeUtility.AsRef<ContactJacobianPolygonData>(ptr);
        }

        public unsafe ref DirectSolverRegularizationData AccessDirectSolverRegularizationData()
        {
            byte* ptr = (byte*)UnsafeUtility.AddressOf(ref this);
            ptr += UnsafeUtility.SizeOf<JacobianHeader>() + SizeOfBaseJacobian(Type) + SizeOfModifierData(Type, Flags);
            return ref UnsafeUtility.AsRef<DirectSolverRegularizationData>(ptr);
        }

        #endregion
    }

    // Helper functions for working with Jacobians
    static class JacobianUtilities
    {
        // This is the inverse function to CalculateConstraintTauAndDamping
        // Given a final Tau and Damping you can get the original Spring Frequency and Damping Ratio.
        // See Unity.Physics.Constraint struct for discussion about default Spring Frequency and Damping Ratio.
        public static void CalculateSpringFrequencyAndDamping(float constraintTau, float constraintDamping,
            float timeStep, int iterations, out float springFrequency, out float dampingRatio)
        {
            int n = iterations;
            float h = timeStep;
            float hh = h * h;
            float a = 1.0f - constraintDamping;
            float aSum = 1.0f;
            for (int i = 1; i < n; i++)
            {
                aSum += math.pow(a, i);
            }

            float w = math.sqrt(constraintTau * aSum / math.pow(a, n)) / h;
            float ww = w * w;
            springFrequency = w / (2.0f * math.PI);
            dampingRatio = (math.pow(a, -n) - 1 - hh * ww) / (2.0f * h * w);
        }

        // This is the inverse function to CalculateSpringFrequencyAndDamping
        public static void CalculateConstraintTauAndDamping(float springFrequency, float dampingRatio, float timeStep,
            int iterations, out float constraintTau, out float constraintDamping)
        {
            /*
            In the following we derive the formulas for converting spring frequency and damping ratio to the solver constraint regularization parameters tau and damping,
            representing a normalized stiffness factor and damping factor, respectively.
            To this end, we compare the integration of spring-damper using implicit Euler integration with the time stepping formula for the constraint solver, and make both equivalent.

            1.  Implicit Euler integration of a spring-damper

                Constitutive equation of a spring-damper:
                    F = -kx - cx'
                with k = spring stiffness, c = damping coefficient, x = position, and x' = velocity.

                Backwards euler of the equations of motion a = x'' and v = x' with a = F/m where h = step length:

                    x2 = x1 + hv2
                    v2 = v1 + hx''
                       = v1 + hF/m
                       = v1 + h(-kx2 - cv2)/m
                       = v1 + h(-kx1 - hkv2 - cv2)/m
                       = 1 / (1 + h^2k/m + hc/m) * v1 - hk / (m + h^2k + hc) * x1

            2.  Gauss-Seidel iterations of a stiff constraint with Baumgarte stabilization parameters t and a, where
                t = tau, d = damping, and a = 1 - d.

                Example for four iterations:

                    v2 = av1 - (t / h)x1
                    v3 = av2 - (t / h)x1
                    v4 = av3 - (t / h)x1
                    v5 = av4 - (t / h)x1
                       = a^4v1 - (a^3 + a^2 + a + 1)(t / h)x1

                Given the recursive nature of the relationship above we can derive a closed-form expression for the new velocity with n iterations:
                    v_n = a * v_n-1 - (t / h) * x1
                        = a^n * v1 - (a^(n-1) + a^(n-2) + ... + a + 1)(t / h) * x1
                        = a^n * v1 - (\sum_{i=0}^{n-1} a^i)(t / h) * x1
                        = a^n * v1 - ((1 - a^n) / (1 - a))(t / h) * x1                      (1)

                Note that above we replaced the geometric series from 1 to n-1 with the closed form expression (1 - a^n) / (1 - a). This is valid for
                a != 1.0. If a == 1.0, the following closed form expression needs to be used instead:

                  \sum_{i=0}^{n-1} a^i) = n

                In this case, the equation above simplifies to:

                v_n = a^n * v1 - (\sum_{i=0}^{n-1} a^i)(t / h) * x1
                    = a^n * v1 - n(t / h) * x1

                For now we will ignore this special case. We will see if a can become 1 and under which conditions, once we have found an expression for a in the following step.

            3.1 Via coefficient matching, we can map the stiffness and damping parameters in the spring-damper to the tau and damping parameters in the stiff constraint.
                For n iterations, we have the following equations:

                    a^n = 1 / (1 + h^2k / m + hc / m), and                                  (2)
                    ((1 - a^n) / (1 - a))(t / h) = hk / (m + h^2k + hc)                     (3)

                where k is the spring constant, c is the damping constant, m is the mass, h is the time step, and a and t are the
                damping and tau parameters of the stiff constraint, respectively.

                We can solve (2) and (3) for a and t in terms of k, c, m, and h as follows.

                First, solve equation (2) for a:

                    a = (1 / (1 + h^2k / m + hc / m))^(1/n)                                 (4)
                <=> d = 1 - a
                      = 1 - (1 / (1 + h^2k / m + hc / m))^(1/n)                             (5)

                Then plug a into equation (3) to solve for t:

                         ((1 - a^n) / (1 - a))(t / h) = hk / (m + h^2k + hc)
                    <=>  ((1 - 1 / (1 + h^2k / m + hc / m)) / (1 - a))(t / h) = hk / (m + h^2k + hc)
                    <=>  ((1 - 1 / (1 + h^2k / m + hc / m)) / (1 - (1 / (1 + h^2k / m + hc / m))^(1/n)))(t / h) = hk / (m + h^2k + hc)
                    <=> t = h^2k / (m + h^2k + hc) * (1 - (1 / (1 + h^2k / m + hc / m))^(1/n)) / ((1 - 1 / (1 + h^2k / m + hc / m))

               We can simplify this further as follows:

                    t = h^2k / (m + h^2k + hc) * (1 - (1 / (1 + h^2k / m + hc / m))^(1/n)) * (1 + h^2k / m + hc / m) / ((1 + h^2k / m + hc / m) - 1)
                    t = h^2k / (m + h^2k + hc) * (1 - (1 / (1 + h^2k / m + hc / m))^(1/n)) * (1 + h^2k / m + hc / m) / (h^2k / m + hc / m)
                    t = h^2k / (m + h^2k + hc) * (1 - (1 / (1 + h^2k / m + hc / m))^(1/n)) * (m + h^2k + hc) / (h^2k + hc)
                    t = h^2k / (h^2k + hc) * (1 - (1 / (1 + h^2k / m + hc / m))^(1/n)) * (m + h^2k + hc) / (m + h^2k + hc)
                    t = h^2k / (h^2k + hc) * (1 - (1 / (1 + h^2k / m + hc / m))^(1/n))

               This yields the final expression for t:

                    t = h^2k / (h^2k + hc) * (1 - (1 / (1 + h^2k / m + hc / m))^(1/n))
                      = h^2k / (h^2k + hc) * d                                              (6)

            3.2 Coming back to our requirement from above that a != 1, let's examine in what situation a can become 1:

                    a = (1 / (1 + h^2k / m + hc / m))^(1/n) = 1

                We can see that a can only be 1 iff (if and only if) the term h^2k / m + hc / m equals 0.

                Given that k and c are both positive values and both m and h are strictly positive, this can only be the case if both k and h are 0, in which case our spring-damper
                will simply not apply any force, meaning, the constraint will not be active. We can deal with this case by simply setting the constraint regularization parameters
                t and d (= 1 - a) to 0 in this case. This will result in the constraint being inactive, which is what we want.

            3.4 Parametrization using Spring Frequency and Damping Ratio:

                Given spring frequency f, damping ratio z and effective mass m, we have the following relationships:
                    w = f * 2 * pi
                    k = m * w^2 <=> k/m = w^2
                    c = z * 2 * w * m <=> c/m = z * 2 * w
                where w denotes the angular spring frequency, k denotes the spring stiffness coefficient and c denotes the damping coefficient.

                We can use the relationships above to convert the expressions (5) and (6) for d and t to the following expressions in terms of spring frequency and damping ratio:
                    d = 1 - (1 / (1 + h^2 * m * w^2 / m + h * z * 2 * w * m / m))^(1/n)
                      = 1 - (1 / (1 + h^2 * w^2 + h * z * 2 * w))^(1/n)                     (7)

                In (6), substitute k for m * w^2 and c for z * 2 * w * m to get:
                    t = h^2k / (h^2k + hc) * d
                      = h^2 * m * w^2 / (h^2 * m * w^2 + h * z * 2 * w * m) * d

                Eliminate m to obtain the final expression:
                    t = h^2 * w^2 / (h^2 * w^2 + h * z * 2 * w) * d                         (8)

                This allows us to parametrize our constraint using the spring frequency and damping ratio of an equivalent spring-damper system.
            */

            // Compute damping factor d from spring frequency f, damping ratio z, time step h, and number of iterations n using equation (7) above.
            // With d in hand, compute stiffness factor tau from spring frequency f, damping coefficient c, time step h, number of iterations n and damping factor d using equation (8) above.

            // f: spring frequency, w: angular spring frequency, z: damping ratio
            float f = springFrequency;
            float z = dampingRatio;
            float h = timeStep;
            float w = f * 2 * math.PI; // convert frequency to angular frequency, i.e., oscillations/sec to radians/sec
            float hw = h * w;
            float hhww = hw * hw; // = h^2 * w^2
            float denom = hhww + hw * z * 2;
            float exp1 = 1 / (1 + denom);
            float exp2 = math.pow(exp1, 1f / iterations);

            constraintDamping = 1 - exp2;
            if (denom < math.EPSILON)
            {
                constraintTau = 0.0f;
                return;
            }
            constraintTau = hhww / denom * constraintDamping;
        }

        /// <summary>
        /// Computes regularization terms epsilon and gamma based on stiffness, damping and time step using
        /// Eq. 59 in S. Andrews et al., SIGGRAPH'22 Course: Contact and Friction Simulation for Computer Graphics.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ComputeDirectSolverViscoelasticRegularizationTerms(out float epsilon, out float gamma, in float stiffness,
            in float damping, in float timeStep)
        {
            if (stiffness + damping > 0)
            {
                epsilon = math.rcp(timeStep * timeStep * stiffness + timeStep * damping);
                gamma = (timeStep * stiffness) / ((timeStep * stiffness) + damping);
            }
            else
            {
                epsilon = math.EPSILON;
                gamma = 1.0f;
            }
        }

        /// <summary>
        /// Computes regularization terms for purely elastic constitutive model, i.e., a spring-damper without any damping.
        /// Same as calling <see cref="ComputeDirectSolverViscoelasticRegularizationTerms"/> with damping set to 0.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ComputeDirectSolverElasticRegularizationTerms(out float epsilon, out float gamma, in float stiffness, in float timeStep)
        {
            epsilon = math.select(math.EPSILON, math.rcp(timeStep * timeStep * stiffness), stiffness > 0);
            gamma = 1.0f;
        }

        /// <summary>
        /// Computes regularization term epsilon based on motor slip (inverse damping) and time step using
        /// Eq. 59 in S. Andrews et al., SIGGRAPH'22 Course: Contact and Friction Simulation for Computer Graphics.
        /// </summary>
        /// <param name="epsilon"></param>
        /// <param name="slip"></param>
        /// <param name="timeStep"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ComputeDirectSolverViscousRegularizationTerm(out float epsilon, in float slip, in float timeStep)
        {
            // epsilon for zero stiffness, and considering that damping = 1/slip:
            //      epsilon = 1 / (timestep * damping) = 1 / (timestep * (1/slip)) = (1 / timestep) * slip = slip / timestep
            epsilon = slip / timeStep;
        }

        public static JacobianHeader.DirectSolverRegularizationData CalculateDirectSolverRegularizationData(in JacobianHeader header,
            in Constraint constraint, in MotionVelocity velocityA, in MotionVelocity velocityB,
            in Solver.DirectSolverSettings directSolverSettings, in float timeStep)
        {
            var constrainedMass = header.IsAngular ? GetConstrainedBodyInertia(velocityA, velocityB) : GetConstrainedBodyMass(velocityA, velocityB);
            var stiffnessCoefficient = CalculateSpringConstantFromSpringFrequency(constraint.SpringFrequency, constrainedMass);
            var dampingCoefficient = CalculateDampingCoefficient(stiffnessCoefficient, constraint.DampingRatio, constrainedMass);

            float epsilon;
            float gamma = 1.0f; // Note: without any regularization, gamma is 1 in the formulation.

            var velocityConstraint = header.IsVelocityConstraint;
            if (velocityConstraint)
            {
                // velocity constraint: use viscous constitutive model (pure damper)
                var jointSlip = math.select(0, math.rcp(dampingCoefficient), dampingCoefficient > Math.Constants.Eps);
                var slip = math.max(jointSlip, directSolverSettings.MinimumMotorSlip);
                ComputeDirectSolverViscousRegularizationTerm(out epsilon, slip, timeStep);
            }
            else
            {
                var cappedStiffnessCoefficient = math.min(stiffnessCoefficient, directSolverSettings.MaximumJointStiffness);
                var cappedDampingCoefficient = math.min(dampingCoefficient, directSolverSettings.MaximumJointDamping);

                // position constraint: use visco-elastic constitutive model (spring/damper)
                ComputeDirectSolverViscoelasticRegularizationTerms(out epsilon, out gamma, cappedStiffnessCoefficient, cappedDampingCoefficient, timeStep);
            }

            return new JacobianHeader.DirectSolverRegularizationData
            {
                Epsilon = epsilon,
                Gamma = gamma,
            };
        }

        // Calculates the spring frequency from the spring constant and mass for a simple mass-spring system.
        //
        // Useful for built-in to Unity Physics spring parameter conversion.
        // - built-in joints use a spring constant parameterization
        // - Unity Physics joints use a spring frequency parameterization
        public static float CalculateSpringFrequencyFromSpringConstant(float springConstant, float mass = 1.0f)
        {
            if (springConstant < Math.Constants.Eps) return 0.0f;

            // f = (1 / 2pi) * sqrt(k / m)
            return math.sqrt(springConstant / mass) * Math.Constants.OneOverTau;
        }

        // Calculates the damping ratio from the spring constant, damping coefficient and mass for a simple
        // mass-spring-damper system.
        //
        // Useful for built-in to Unity Physics spring-damper parameter conversion.
        //
        // Note that the spring constant is required as input. For cases where a spring constant isn't available, e.g.,
        // purely viscous motors, the provided damping coefficient is returned here as an approximation.
        public static float CalculateDampingRatio(float springConstant, float dampingCoefficient, float mass = 1.0f)
        {
            if (dampingCoefficient < Math.Constants.Eps) return 0.0f;

            var tmp = springConstant * mass;
            if (tmp < Math.Constants.Eps)
            {
                // Can not compute damping ratio. Just use damping coefficient as an approximation.
                return dampingCoefficient;
            }

            // Calculation: damping ratio = damping coefficient / (2 * sqrt(k * m))
            return dampingCoefficient / (2 * math.sqrt(tmp)); // damping coefficient / critical damping coefficient
        }

        public static float CalculateSpringConstantFromSpringFrequency(float springFrequency, float mass = 1.0f)
        {
            if (springFrequency < Math.Constants.Eps) return 0.0f;

            // f = (1 / 2pi) * sqrt(k / m)
            // k = m * (f * 2pi)^2
            var fTwoPi = springFrequency * Math.Constants.Tau;
            return mass * fTwoPi * fTwoPi;
        }

        public static float CalculateDampingCoefficient(float springConstant, float dampingRatio, float mass = 1.0f)
        {
            if (dampingRatio < Math.Constants.Eps) return 0.0f;

            var tmp = springConstant * mass;
            if (tmp < Math.Constants.Eps)
            {
                // Can not compute damping coefficient. Just use damping ratio as an approximation.
                return dampingRatio;
            }

            //      damping ratio = damping coefficient / (2 * sqrt(k * m))
            // <=>  damping coefficient = damping ratio * (2 * sqrt(k * m))
            return dampingRatio * 2 * math.sqrt(tmp); // = damping ratio * critical damping coefficient
        }

        // Returns x - clamp(x, min, max)
        public static float CalculateError(float x, float min, float max)
        {
            float error = math.max(x - max, 0.0f);
            error = math.min(x - min, error);
            return error;
        }

        // Returns the amount of error for the solver to correct, where initialError is the pre-integration error and predictedError is the expected post-integration error
        public static float CalculateCorrection(float predictedError, float initialError, float tau, float damping)
        {
            return (predictedError - initialError) * damping + initialError * tau;
        }

        // Integrate the relative orientation of a pair of bodies, faster and less memory than storing both bodies' orientations and integrating them separately
        public static quaternion IntegrateOrientationBFromA(quaternion bFromA, float3 angularVelocityA,
            float3 angularVelocityB, float timestep)
        {
            quaternion dqA = Integrator.IntegrateAngularVelocity(angularVelocityA, timestep);
            quaternion dqB = Integrator.IntegrateAngularVelocity(angularVelocityB, timestep);
            return math.normalize(math.mul(math.mul(math.inverse(dqB), bFromA), dqA));
        }

        // Calculate the inverse effective mass of a linear jacobian
        public static float CalculateInvEffectiveMassDiag(
            float3 angA, float3 invInertiaA, float invMassA,
            float3 angB, float3 invInertiaB, float invMassB)
        {
            float3 angularPart = angA * angA * invInertiaA + angB * angB * invInertiaB;
            float linearPart = invMassA + invMassB;
            return (angularPart.x + angularPart.y) + (angularPart.z + linearPart);
        }

        // Calculate the inverse effective mass for a pair of jacobians with perpendicular linear parts
        public static float CalculateInvEffectiveMassOffDiag(
            float3 angA0, float3 angA1, float3 invInertiaA,
            float3 angB0, float3 angB1, float3 invInertiaB)
        {
            return math.csum(angA0 * angA1 * invInertiaA + angB0 * angB1 * invInertiaB);
        }

        // Inverts a symmetric 3x3 matrix with diag = (0, 0), (1, 1), (2, 2), offDiag = (0, 1), (0, 2), (1, 2) = (1, 0), (2, 0), (2, 1)
        public static bool InvertSymmetricMatrix(float3 diag, float3 offDiag, out float3 invDiag, out float3 invOffDiag)
        {
            float3 offDiagSq = offDiag.zyx * offDiag.zyx;
            float determinant = (Math.HorizontalMul(diag) + 2.0f * Math.HorizontalMul(offDiag) - math.csum(offDiagSq * diag));
            bool determinantOk = (determinant != 0);
            float invDeterminant = math.select(0.0f, 1.0f / determinant, determinantOk);
            invDiag = (diag.yxx * diag.zzy - offDiagSq) * invDeterminant;
            invOffDiag = (offDiag.yxx * offDiag.zzy - diag.zyx * offDiag) * invDeterminant;
            return determinantOk;
        }

        // Builds a symmetric 3x3 matrix from diag = (0, 0), (1, 1), (2, 2), offDiag = (0, 1), (0, 2), (1, 2) = (1, 0), (2, 0), (2, 1)
        public static float3x3 BuildSymmetricMatrix(float3 diag, float3 offDiag)
        {
            return new float3x3(
                new float3(diag.x, offDiag.x, offDiag.y),
                new float3(offDiag.x, diag.y, offDiag.z),
                new float3(offDiag.y, offDiag.z, diag.z)
            );
        }

        // Makes sure that the provided matrix is symmetric by averaging its off-diagonal elements.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float3x3 BuildSymmetricMatrix(in float3x3 mat)
        {
            var mat01 = (mat.c0.y + mat.c1.x) * 0.5f;
            var mat02 = (mat.c0.z + mat.c2.x) * 0.5f;
            var mat12 = (mat.c1.z + mat.c2.y) * 0.5f;

            return new(
                new float3(mat.c0.x, mat01, mat02),
                new float3(mat01, mat.c1.y, mat12),
                new float3(mat02, mat12, mat.c2.z));
        }

        /// <summary>
        /// Compute how much of an impulse can be applied, based on the impulses accumulated over several iterations, to
        /// a motor based on some threshold. This method can be used for positive or negative checks. For 1D corrections.
        /// </summary>
        /// <param name="impulse"> The current calculated impulse that can be applied </param>
        /// <param name="accumulatedImpulse"> The impulse that has been accumulated so far. This is passed by reference
        /// and as an input argument does not include the current impulse. On completion, this will be the accumulated
        /// impulse. This impulse will not exceed the define maximum impulse threshold. </param>
        /// <param name="maxImpulseOfMotor"> A magnitude representing the maximum accumulated impulse that can be
        /// applied. Value should not be negative </param>
        /// <returns>  The impulse to be applied by a motor. </returns>
        internal static float CapImpulse(float impulse, ref float accumulatedImpulse, float maxImpulseOfMotor)
        {
            SafetyChecks.CheckWithinThresholdAndThrow(accumulatedImpulse, maxImpulseOfMotor, "Accumulated Impulse");

            float newAccImpulse = accumulatedImpulse + impulse;
            if (newAccImpulse < -maxImpulseOfMotor)
            {
                // we want an impulse with which we have -maxImpulse = accumulatedImpulse + impulse
                impulse = -maxImpulseOfMotor - accumulatedImpulse;
                accumulatedImpulse = -maxImpulseOfMotor;
            }
            else if (newAccImpulse > maxImpulseOfMotor)
            {
                // we want an impulse with which we have maxImpulse = accumulatedImpulse + impulse
                impulse = maxImpulseOfMotor - accumulatedImpulse;
                accumulatedImpulse = maxImpulseOfMotor;
            }
            else
            {
                // impulse is within range
                accumulatedImpulse += impulse;
            }

            return impulse;
        }

        /// <summary>
        /// Compute how much of an impulse can be applied, based on the impulses accumulated over several iterations, to
        /// a motor based on some threshold. This method can be used for positive or negative checks. For 3D corrections.
        /// </summary>
        /// <param name="impulse"> The current calculated impulse that can be applied </param>
        /// <param name="accumulatedImpulse"> A parameter passed by reference. On input it is the impulse that has been
        /// accumulated so far. This value does not include the current impulse. On completion, it is the accumulated
        /// impulse. This impulse will not exceed the define maximum impulse threshold. </param>
        /// <param name="maxImpulseOfMotor"> A magnitude representing the maximum accumulated impulse that can be
        /// applied. Value should not be negative </param>
        /// <returns>  The impulse to be applied by a motor. </returns>
        internal static float3 CapImpulse(float3 impulse, ref float3 accumulatedImpulse, float maxImpulseOfMotor)
        {
            // Test Case A: Adding impulse to accumulation does not exceed threshold
            if (math.length(accumulatedImpulse + impulse) < maxImpulseOfMotor)
            {
                accumulatedImpulse += impulse;
                return impulse;
            }

            // Test Case D: Accumulation has reached threshold already
            if (maxImpulseOfMotor - math.length(accumulatedImpulse) <= math.EPSILON)
            {
                accumulatedImpulse = maxImpulseOfMotor * math.normalizesafe(accumulatedImpulse); //set to max instead of input > corrective in case given wrong accumulation
                return 0.0f;
            }

            // Cases when threshold is exceeded:
            // Test Case C: No impulses accumulated and the impulse is larger than threshold:
            if ((math.length(accumulatedImpulse) < math.EPSILON) && (math.length(impulse) - maxImpulseOfMotor > math.EPSILON))
            {
                impulse = maxImpulseOfMotor * math.normalizesafe(impulse); //cap impulse at threshold
            }
            else // Test Case B: accumulation + impulse will push over threshold, so apply the remaining impulse balance
            {
                impulse = (maxImpulseOfMotor * math.normalizesafe(accumulatedImpulse)) - accumulatedImpulse;
            }

            accumulatedImpulse += impulse;

            return impulse;
        }

        // isAxisInA should match how axisInA/axisInB is used in the Motor Baking
        // if the authoring axis is an axis relative to bodyA then set isAxisInA = T
        // if the authoring axis is an axis relative to bodyB, then set isAxisInA = F
        internal static BodyFrame CalculateDefaultBodyFramesForConnectedBody(RigidTransform worldFromA, RigidTransform worldFromB,
            float3 positionBodyA, float3 axis, out BodyFrame jointFrameB, bool isAxisInA)
        {
            float3 positionBodyB, perpendicularAxisInA, perpendicularAxisInB;
            float3 axisInA, axisInB;

            RigidTransform bFromA = math.mul(math.inverse(worldFromB), worldFromA);

            if (isAxisInA) // during authoring, axis of motor is relative to bodyA, used for Angular Motors
            {
                axisInA = axis;
                axisInB = math.mul(bFromA.rot, axisInA); //motor axis in Connected Entity space
            }
            else // during authoring, axis of motor is specified relative to bodyB, used for Linear Motors
            {
                RigidTransform aFromB = math.mul(math.inverse(worldFromA), worldFromB);
                axisInA = math.mul(aFromB.rot, axis); //motor axis relative to bodyA
                axisInB = axis;  //motor axis in Connected Entity space
            }

            //position of motored body relative to Connected Entity in world space
            positionBodyB = math.transform(bFromA, positionBodyA);

            // Always calculate the perpendicular axes
            Math.CalculatePerpendicularNormalized(axisInA, out perpendicularAxisInA, out _);
            perpendicularAxisInB = math.mul(bFromA.rot, perpendicularAxisInA); //perp motor axis in Connected Entity space

            var jointFrameA = new BodyFrame
            {
                Axis = axisInA,
                PerpendicularAxis = perpendicularAxisInA,
                Position = positionBodyA
            };
            jointFrameB = new BodyFrame
            {
                Axis = axisInB,
                PerpendicularAxis = perpendicularAxisInB,
                Position = positionBodyB
            };

            return jointFrameA;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float GetConstrainedBodyMass(in MotionVelocity motionVelocityA, in MotionVelocity motionVelocityB)
        {
            float mass = 0;

            if (!motionVelocityA.IsKinematic)
            {
                mass += math.rcp(motionVelocityA.InverseMass);
            }

            if (!motionVelocityB.IsKinematic)
            {
                mass += math.rcp(motionVelocityB.InverseMass);
            }

            return mass > 0 ? mass : 1.0f;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static float GetConstrainedBodyInertia(in MotionVelocity motionVelocityA, in MotionVelocity motionVelocityB)
        {
            float approxInertia = 0;
            if (!motionVelocityA.IsKinematic)
            {
                var inertia = math.rcp(motionVelocityA.InverseInertia);
                approxInertia += 1f / 3f * math.csum(inertia);
            }

            if (!motionVelocityB.IsKinematic)
            {
                var inertia = math.rcp(motionVelocityB.InverseInertia);
                approxInertia += 1f / 3f * math.csum(inertia);
            }

            return approxInertia > 0 ? approxInertia : 1.0f;
        }
    }

    struct IslandJacobianIterator
    {
        UnsafeStream.Reader m_Reader;
        DispatchPairSequencer.DirectSolverSchedulerInfo m_DirectSolverSchedulerInfo;
        DispatchPairSequencer.DispatchPairJacobianMapping m_JacobianMapping;
        int m_NextDispatchPairIslandInfoIndex;
        int m_LastDispatchPairIslandInfoIndex;
        bool m_NextDispatchPair;
#if ENABLE_UNITY_COLLECTIONS_CHECKS || UNITY_DOTS_DEBUG
        int m_CurrentConstraintBlockLength;
        int m_CurrentConstraintIndex;
#endif

        public IslandJacobianIterator(in NativeStream.Reader jacobianStreamReader,
            in DispatchPairSequencer.DirectSolverSchedulerInfo directSolverSchedulerInfo, int islandIndex)
        {
            m_Reader = jacobianStreamReader.GetUnsafeReader();
            m_DirectSolverSchedulerInfo = directSolverSchedulerInfo;
            m_NextDispatchPairIslandInfoIndex = directSolverSchedulerInfo.FirstDispatchPairIslandInfoIndices[islandIndex];
            m_LastDispatchPairIslandInfoIndex = m_NextDispatchPairIslandInfoIndex
                + directSolverSchedulerInfo.DispatchPairIslandInfoCounts[islandIndex] - 1;
            m_NextDispatchPair = true;
#if ENABLE_UNITY_COLLECTIONS_CHECKS || UNITY_DOTS_DEBUG
            m_CurrentConstraintBlockLength = -1;
            m_CurrentConstraintIndex = 0;
#endif
            m_JacobianMapping = default;
            m_JacobianMapping = MoveToNextValidDispatchPair();
        }

        public bool HasJacobiansLeft()
        {
            return m_JacobianMapping.IsValid;
        }

        DispatchPairSequencer.DispatchPairJacobianMapping MoveToNextValidDispatchPair()
        {
            var jacobianMapping = DispatchPairSequencer.DispatchPairJacobianMapping.Invalid;

            if (m_NextDispatchPairIslandInfoIndex > m_LastDispatchPairIslandInfoIndex)
            {
                return jacobianMapping;
            }
            // else:

            // find the next valid mapping
            do
            {
                // Get phased dispatch pair index from unphased dispatch pair index, and move to next index
                int unphasedDispatchPairIndex = m_DirectSolverSchedulerInfo.DispatchPairIslandInfos[m_NextDispatchPairIslandInfoIndex++].UnphasedDispatchPairIndex;
                int phasedDispatchPairIndex = m_DirectSolverSchedulerInfo.UnphasedToPhasedDispatchPairMap[unphasedDispatchPairIndex];

                // Get Jacobian mapping for current pair
                jacobianMapping = m_DirectSolverSchedulerInfo.PhasedDispatchPairJacobianMappings[phasedDispatchPairIndex];
            }
            while (!jacobianMapping.IsValid && m_NextDispatchPairIslandInfoIndex <= m_LastDispatchPairIslandInfoIndex);

            return jacobianMapping;
        }

        public ref JacobianHeader ReadJacobianHeader()
        {
            SafetyChecks.CheckAreEqualAndThrow(true, m_JacobianMapping.IsValid);

            unsafe
            {
                // We will start reading Jacobians for the next dispatch pair.
                if (m_NextDispatchPair)
                {
                    // Move reader to correct stream buffer state for reading of the corresponding Jacobian block
                    m_Reader.State = m_JacobianMapping.ReaderState;

                    m_NextDispatchPair = false;
                }

                // Get Jacobian size and read Jacobian
                int readSize = m_Reader.Read<int>();
                ref var jacobian = ref UnsafeUtility.AsRef<JacobianHeader>(m_Reader.ReadUnsafePtr(readSize));

                // Check if we reached the end of the constraint block. If yes, move the dispatch pair info pointer forward.
                if (jacobian.ConstraintBlockInfo.Index == jacobian.ConstraintBlockInfo.Length - 1)
                {
                    m_JacobianMapping = MoveToNextValidDispatchPair();
                    m_NextDispatchPair = true;
                }

#if ENABLE_UNITY_COLLECTIONS_CHECKS || UNITY_DOTS_DEBUG
                if (m_CurrentConstraintBlockLength == -1)
                {
                    // Initialize the current constraint block length and constraint index
                    m_CurrentConstraintBlockLength = jacobian.ConstraintBlockInfo.Length;
                    m_CurrentConstraintIndex = 0;
                }

                // Make sure the current constraint block info is as expected
                Assert.IsTrue(jacobian.ConstraintBlockInfo.Index < jacobian.ConstraintBlockInfo.Length);
                Assert.AreEqual(m_CurrentConstraintBlockLength, jacobian.ConstraintBlockInfo.Length);
                Assert.AreEqual(m_CurrentConstraintIndex, jacobian.ConstraintBlockInfo.Index);
                ++m_CurrentConstraintIndex;

                // We have reached the end of the current constraint block. Reinitialize counters.
                if (m_CurrentConstraintIndex == jacobian.ConstraintBlockInfo.Length)
                {
                    m_CurrentConstraintBlockLength = -1;
                }
#endif
                return ref jacobian;
            }
        }
    }

    // Iterator (and modifier) for jacobians
    unsafe struct JacobianIterator
    {
        NativeStream.Reader m_Reader;

        public JacobianIterator(NativeStream.Reader jacobianStreamReader, int workItemIndex)
        {
            m_Reader = jacobianStreamReader;
            m_Reader.BeginForEachIndex(workItemIndex);
        }

        public bool HasJacobiansLeft()
        {
            return m_Reader.RemainingItemCount > 0;
        }

        public ref JacobianHeader ReadJacobianHeader()
        {
            int readSize = Read<int>();
            return ref UnsafeUtility.AsRef<JacobianHeader>(Read(readSize));
        }

        public ref JacobianHeader ReadJacobianHeader(out int jacobianByteCountInStream)
        {
            int readSize = Read<int>();
            jacobianByteCountInStream = sizeof(int) + readSize;
            return ref UnsafeUtility.AsRef<JacobianHeader>(Read(readSize));
        }

        public UnsafeStream.ReaderState ReaderState => m_Reader.GetUnsafeReader().State;

        private byte* Read(int size)
        {
            byte* dataPtr = m_Reader.ReadUnsafePtr(size);

            return dataPtr;
        }

        private ref T Read<T>() where T : struct
        {
            int size = UnsafeUtility.SizeOf<T>();
            return ref UnsafeUtility.AsRef<T>(Read(size));
        }
    }
}
