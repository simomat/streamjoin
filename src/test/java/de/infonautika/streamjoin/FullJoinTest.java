package de.infonautika.streamjoin;

import de.infonautika.streamjoin.repo.Department;
import de.infonautika.streamjoin.repo.Employee;
import de.infonautika.streamjoin.repo.Tuple;
import org.junit.Test;

import java.util.stream.Stream;

import static de.infonautika.streamjoin.FullJoin.fullJoin;
import static de.infonautika.streamjoin.StreamMatcher.isEmptyStream;
import static de.infonautika.streamjoin.StreamMatcher.isStreamOfTuples;
import static de.infonautika.streamjoin.repo.TestRepository.*;
import static de.infonautika.streamjoin.repo.Tuple.tuple;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class FullJoinTest {
    @Test
    public void withCombiner() {
        Stream<Tuple<Department, Employee>> joined =
                fullJoin(departmentsWithId())
                .withKey(Department::getId)
                .on(employeesWithDepartment())
                .withKey(Employee::getDepartmentId)
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isStreamOfTuples(
                // Matched both
                tuple(sales, rafferty),
                tuple(salesTwo, rafferty),
                tuple(engineering, jones),
                tuple(engineering, heisenberg),
                tuple(clerical, robinson),
                tuple(clerical, smith),
                // Not matched on right
                tuple(marketing, null),
                // Not matched on left
                tuple(null, scruffy)));
    }

    @Test
    public void withMatcher() {
        Stream<Tuple<Department, Employee>> joined =
                fullJoin(Stream.of(sales, salesTwo, clerical))
                .withKey(Department::getName)
                .on(Stream.of(smith, rafferty))
                .withKey(Employee::getName)
                .matching((d, e) -> d.charAt(0) == e.charAt(0))
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isStreamOfTuples(
                // Matched both
                tuple(sales, smith),
                tuple(salesTwo, smith),
                // Not matched on right
                tuple(clerical, null),
                // Not matched on left
                tuple(null, rafferty)));
    }

    @Test
    public void treatsNullKeyOnLeftAsNonMatching() {
        assertNull(storage.getId());
        assertNotNull(smith.getDepartmentId());

        Stream<Tuple<Department, Employee>> joined =
                fullJoin(Stream.of(storage))
                .withKey(Department::getId)
                .on(Stream.of(smith))
                .withKey(Employee::getDepartmentId)
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isStreamOfTuples(
            // Not matched to anything
            tuple(storage, null),
            // Also not matched to anything
            tuple(null, smith)));
    }

    @Test
    public void treatsNullKeyOnRightAsNonMatching() {
        assertNotNull(clerical.getId());
        assertNull(williams.getDepartmentId());

        Stream<Tuple<Department, Employee>> joined =
                fullJoin(Stream.of(clerical))
                .withKey(Department::getId)
                .on(Stream.of(williams))
                .withKey(Employee::getDepartmentId)
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isStreamOfTuples(
            // Not matched to anything
            tuple(clerical, null),
            // Also not matched to anything
            tuple(null, williams)));
    }

    @Test
    public void emptyBothYieldsEmpty() {
        Stream<Tuple<Department, Employee>> joined =
                fullJoin(Stream.<Department>empty())
                .withKey(Department::getId)
                .on(Stream.<Employee>empty())
                .withKey(Employee::getDepartmentId)
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isEmptyStream());
    }

    @Test
    public void emptyLeftYieldsStreamOfRight() {
        Stream<Tuple<Department, Employee>> joined =
                fullJoin(Stream.<Department>empty())
                .withKey(Department::getId)
                .on(employeesWithDepartment())
                .withKey(Employee::getDepartmentId)
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isStreamOfTuples(employeesWithDepartment().map(Tuple::tupleB)));
    }

    @Test
    public void emptyRightYieldsStreamOfLeft() {
        Stream<Tuple<Department, Employee>> joined =
                fullJoin(departmentsWithId())
                .withKey(Department::getId)
                .on(Stream.<Employee>empty())
                .withKey(Employee::getDepartmentId)
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isStreamOfTuples(departmentsWithId().map(Tuple::tupleA)));
    }
}
