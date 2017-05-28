/**
 * TestMultiConstraintBuilder.java
 *
 * @since 2017/05/28
 * <p>
 * Copyright 2017 fuji-151a
 * All Rights Reserved.
 */
package storm.mesos.constraint;

import com.google.common.collect.Lists;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Offer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 * @author fuji-151a
 */
public class TestMultiConstraintBuilder implements MultiConstraintBuilder<Offer> {

    private final List<Constraint<Offer>> constraintList;

    public TestMultiConstraintBuilder() {
        constraintList = new ArrayList<>();
    }

    public TestMultiConstraintBuilder(Constraint<Offer>... constraint) {
        this.constraintList = Arrays.asList(constraint);
    }

    @Override
    public List<Constraint<Offer>> build(Map conf) {
        List<Constraint<Offer>> returnList = new ArrayList<>();
        returnList.addAll(constraintList);
        return returnList;
    }
}
