/**
 * TextAttrEqConstraintTest.java
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 *
 * @author fuji-151a
 */
public class TextAttrEqConstraintTest {

    @Test
    public void isAccepted() throws Exception {
        Map<String, String> cons = new HashMap<>();
        cons.put("type", "host");
        Map<String, Map> conf = new HashMap<>();
        conf.put("mesos.constraint", cons);
        List<Constraint<Attribute>> constraintList = new TextAttrEqConstraint.Builder().build(conf);
        Attribute attr = TestUtils.buildTextAttribute("type", "host");
        for (Constraint<Attribute> constraint : constraintList) {
            assertTrue(constraint.isAccepted(attr));
        }
    }

    @Test
    public void isNotAccepted() throws Exception {
        Map<String, String> cons = new HashMap<>();
        cons.put("type", "host");
        Map<String, Map> conf = new HashMap<>();
        conf.put("mesos.constraint", cons);
        List<Constraint<Attribute>> constraintList = new TextAttrEqConstraint.Builder().build(conf);
        Attribute attr = TestUtils.buildTextAttribute("type", "hostA");
        for (Constraint<Attribute> constraint : constraintList) {
            assertFalse(constraint.isAccepted(attr));
        }
    }
}