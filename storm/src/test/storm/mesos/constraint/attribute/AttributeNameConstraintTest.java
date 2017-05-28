/**
 * AttributeNameConstraintTest.java
 *
 * @since 2017/05/28
 * <p>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint.attribute;

import org.apache.mesos.Protos.Attribute;
import org.junit.Test;
import storm.mesos.TestUtils;
import storm.mesos.constraint.Constraint;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author fuji-151a
 */
public class AttributeNameConstraintTest {

    @Test
    public void isAccepted() throws Exception {
        Attribute attr = TestUtils.buildTextAttribute("host", "test");
        Constraint<Attribute> constraintAttribute = new AttributeNameConstraint("host");
        assertTrue(constraintAttribute.isAccepted(attr));
    }

    @Test
    public void isNotAccepted() throws Exception {
        Attribute attr = TestUtils.buildTextAttribute("ip", "test");
        Constraint<Attribute> constraintAttribute = new AttributeNameConstraint("host");
        assertFalse(constraintAttribute.isAccepted(attr));
    }
}
