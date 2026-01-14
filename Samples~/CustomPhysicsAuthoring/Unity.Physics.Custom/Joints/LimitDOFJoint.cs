using Unity.Collections;
using Unity.Mathematics;
using UnityEngine;

namespace Unity.Physics.Authoring
{
    // This Joint allows you to lock one or more of the 6 degrees of freedom of a constrained body.
    // This is achieved by combining the appropriate lower level 'constraint atoms' to form the higher level Joint.
    // In this case Linear and Angular constraint atoms are combined.
    // One use-case for this Joint could be to restrict a 3d simulation to a 2d plane.
    public class LimitDOFJoint : BaseJoint
    {
        public bool3 LockLinearAxes;
        public bool3 LockAngularAxes;

        public PhysicsJoint CreateLimitDOFJoint(RigidTransform offset)
        {
            var constraints = new FixedList512Bytes<Constraint>();
            if (math.any(LockLinearAxes))
            {
                constraints.Add(new Constraint
                {
                    ConstrainedAxes = LockLinearAxes,
                    Type = ConstraintType.Linear,
                    Min = 0,
                    Max = 0,
                    SpringFrequency = Constraint.DefaultSpringFrequency,
                    DampingRatio = Constraint.DefaultDampingRatio,
                    MaxImpulse = MaxImpulse,
                });
            }
            if (math.any(LockAngularAxes))
            {
                constraints.Add(new Constraint
                {
                    ConstrainedAxes = LockAngularAxes,
                    Type = ConstraintType.Angular,
                    Min = 0,
                    Max = 0,
                    SpringFrequency = Constraint.DefaultSpringFrequency,
                    DampingRatio = Constraint.DefaultDampingRatio,
                    MaxImpulse = MaxImpulse,
                });
            }

            var joint = new PhysicsJoint
            {
                BodyAFromJoint = BodyFrame.Identity,
                BodyBFromJoint = offset
            };
            joint.SetConstraints(constraints);

            return joint;
        }
    }

    class LimitDOFJointBaker : JointBaker<LimitDOFJoint>
    {
        public override void Bake(LimitDOFJoint authoring)
        {
            if (!math.any(authoring.LockLinearAxes) && !math.any(authoring.LockAngularAxes))
                return;

            RigidTransform bFromA = math.mul(math.inverse(authoring.worldFromB), authoring.worldFromA);
            PhysicsJoint physicsJoint = authoring.CreateLimitDOFJoint(bFromA);

            var constraintBodyPair = GetConstrainedBodyPair(authoring);

            uint worldIndex = GetWorldIndexFromBaseJoint(authoring);
            CreateJointEntity(worldIndex, constraintBodyPair, authoring.SolverType, physicsJoint);
        }
    }
}
