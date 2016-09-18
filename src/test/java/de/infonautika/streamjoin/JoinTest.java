package de.infonautika.streamjoin;

import de.infonautika.streamjoin.repo.Department;
import de.infonautika.streamjoin.repo.Employee;
import de.infonautika.streamjoin.repo.Tuple;
import org.junit.Test;

import java.util.List;

import static de.infonautika.streamjoin.repo.TestRepository.*;
import static de.infonautika.streamjoin.repo.Tuple.tuple;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;

public class JoinTest {

    @Test
    public void innerJoinWithGrouper() throws Exception {
        List<Tuple<Department, List<Employee>>> joined = Join
                .join(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .group((d, es) -> tuple(d, es.collect(toList())))
                .collect(toList());

        assertThat(joined, hasItem(tuple(engineering, asList(jones, heisenberg))));
    }

    @Test
    public void innerJoinWithCombiner() throws Exception {
        List<Tuple<Department, Employee>> joined = Join
                .join(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .combine(Tuple::tuple)
                .collect(toList());

        assertThat(joined, hasItem(tuple(engineering, jones)));
        assertThat(joined, hasItem(tuple(engineering, heisenberg)));
    }

    @Test
    public void leftOuterJoinWithCombiner() throws Exception {
        List<Tuple<Department, Employee>> joined = Join
                .leftOuter(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .combine(Tuple::tuple)
                .collect(toList());

        assertThat(joined, hasItem(tuple(engineering, jones)));
        assertThat(joined, hasItem(tuple(engineering, heisenberg)));
    }

    @Test
    public void leftOuterJoinWithGrouper() throws Exception {
        List<Tuple<Department, List<Employee>>> joined = Join
                .leftOuter(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .group((d, es) -> tuple(d, es.collect(toList())))
                .collect(toList());

        assertThat(joined, hasItem(tuple(engineering, asList(jones, heisenberg))));
    }

    @Test
    public void fullOuterJoinWithCombiner() throws Exception {
        List<Tuple<Department, Employee>> joined = Join
                .fullOuter(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .combine(Tuple::tuple)
                .collect(toList());

        assertThat(joined, hasItem(tuple(engineering, jones)));
        assertThat(joined, hasItem(tuple(engineering, heisenberg)));
    }

    @Test
    public void fullOuterJoinWithGrouper() throws Exception {
        List<Tuple<Department, List<Employee>>> joined = Join
                .fullOuter(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .group((d, es) -> tuple(d, es.collect(toList())))
                .collect(toList());

        assertThat(joined, hasItem(tuple(engineering, asList(jones, heisenberg))));
    }
}