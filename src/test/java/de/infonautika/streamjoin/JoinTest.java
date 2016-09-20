package de.infonautika.streamjoin;

import de.infonautika.streamjoin.repo.Department;
import de.infonautika.streamjoin.repo.Employee;
import de.infonautika.streamjoin.repo.Tuple;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.infonautika.streamjoin.StreamMatcher.isStreamOf;
import static de.infonautika.streamjoin.repo.TestRepository.*;
import static de.infonautika.streamjoin.repo.Tuple.tuple;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

@SuppressWarnings("unchecked")
public class JoinTest {
    @Test
    public void innerJoinWithCombiner() throws Exception {
        Stream<Tuple<Department, Employee>> joined = Join
                .join(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isStreamOf(
                Tuple.tuple(sales, rafferty),
                Tuple.tuple(salesTwo, rafferty),
                Tuple.tuple(engineering, jones),
                Tuple.tuple(engineering, heisenberg),
                Tuple.tuple(clerical, robinson),
                Tuple.tuple(clerical, smith)));
    }

    @Test
    public void innerJoinWithCombinerAndCollector() throws Exception {
        List<Tuple<Department, Employee>> joined = Join
                .join(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .combine(Tuple::tuple)
                .collect(Collectors.toList());

        assertThat(joined, containsInAnyOrder(
                Tuple.tuple(sales, rafferty),
                Tuple.tuple(salesTwo, rafferty),
                Tuple.tuple(engineering, jones),
                Tuple.tuple(engineering, heisenberg),
                Tuple.tuple(clerical, robinson),
                Tuple.tuple(clerical, smith)));
    }

    @Test
    public void innerJoinWithNullKey() throws Exception {
        Stream<Tuple<Department, Employee>> joined = Join
                .join(getDepartments())
                .withKey(d -> d.getName().equals("Clerical") ? null : d.getId())
                .on(getEmployees())
                .withKey(e -> e.getName().equals("Jones") ? null : e.getDepartmentId())
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isStreamOf(
                Tuple.tuple(sales, rafferty),
                Tuple.tuple(salesTwo, rafferty),
                Tuple.tuple(engineering, heisenberg)));
    }

    @Test
    public void innerJoinWithGrouper() throws Exception {
        Stream<Tuple<Department, Set<Employee>>> joined = Join
                .join(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .group((d, es) -> tuple(d, es.collect(toSet())))
                .asStream();

        assertThat(joined, isStreamOf(
                Tuple.tuple(sales, asSet(rafferty)),
                Tuple.tuple(salesTwo, asSet(rafferty)),
                Tuple.tuple(engineering, asSet(jones,heisenberg)),
                Tuple.tuple(clerical, asSet(robinson, smith))));
    }

    @Test
    public void leftOuterJoinWithCombiner() throws Exception {
        Stream<Tuple<Department, Employee>> joined = Join
                .leftOuter(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isStreamOf(
                Tuple.tuple(sales, rafferty),
                Tuple.tuple(salesTwo, rafferty),
                Tuple.tuple(engineering, jones),
                Tuple.tuple(engineering, heisenberg),
                Tuple.tuple(clerical, robinson),
                Tuple.tuple(clerical, smith),
                Tuple.tuple(marketing, null),
                Tuple.tuple(storage, null)));
    }

    @Test
    public void leftOuterJoinWithNullKey() throws Exception {
        Stream<Tuple<Department, Employee>> joined = Join
                .leftOuter(getDepartments())
                .withKey(d -> d.getName().equals("Clerical") ? null : d.getId())
                .on(getEmployees())
                .withKey(e -> e.getName().equals("Jones") ? null : e.getDepartmentId())
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isStreamOf(
                Tuple.tuple(sales, rafferty),
                Tuple.tuple(salesTwo, rafferty),
                Tuple.tuple(engineering, heisenberg),
                Tuple.tuple(clerical, null),
                Tuple.tuple(marketing, null),
                Tuple.tuple(storage, null)));
    }

    @Test
    public void leftOuterJoinWithGrouper() throws Exception {
        Stream<Tuple<Department, Set<Employee>>> joined = Join
                .leftOuter(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .group((d, es) -> tuple(d, es.collect(toSet())))
                .withLeftUnmatched(l -> tuple(l, asSet()))
                .asStream();

        assertThat(joined, isStreamOf(
                Tuple.tuple(sales, asSet(rafferty)),
                Tuple.tuple(salesTwo, asSet(rafferty)),
                Tuple.tuple(engineering, asSet(jones,heisenberg)),
                Tuple.tuple(clerical, asSet(robinson, smith)),
                Tuple.tuple(marketing, asSet()),
                Tuple.tuple(storage, asSet())));
    }

    @Test
    public void fullOuterJoinWithCombiner() throws Exception {
        Stream<Tuple<Department, Employee>> joined = Join
                .fullOuter(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isStreamOf(
                Tuple.tuple(sales, rafferty),
                Tuple.tuple(salesTwo, rafferty),
                Tuple.tuple(engineering, jones),
                Tuple.tuple(engineering, heisenberg),
                Tuple.tuple(clerical, robinson),
                Tuple.tuple(clerical, smith),
                Tuple.tuple(marketing, null),
                Tuple.tuple(storage, null),
                Tuple.tuple(null, williams),
                Tuple.tuple(null, scruffy)));
    }

    @Test
    public void fullOuterJoinWithGrouper() throws Exception {
        Stream<Tuple<Department, Set<Employee>>> joined = Join
                .fullOuter(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .group((d, es) -> tuple(d, es.collect(toSet())))
                .withRightUnmatched(r -> tuple(Department.sentinel, asSet(r)))
                .withLeftUnmatched(l -> tuple(l, asSet()))
                .asStream();

        assertThat(joined, isStreamOf(
                Tuple.tuple(sales, asSet(rafferty)),
                Tuple.tuple(salesTwo, asSet(rafferty)),
                Tuple.tuple(engineering, asSet(jones,heisenberg)),
                Tuple.tuple(clerical, asSet(robinson, smith)),
                Tuple.tuple(marketing, asSet()),
                Tuple.tuple(storage, asSet()),
                Tuple.tuple(Department.sentinel, asSet(williams)),
                Tuple.tuple(Department.sentinel, asSet(scruffy))));
    }

    @Test
    public void fullOuterJoinWithNullKey() throws Exception {
        Stream<Tuple<Department, Employee>> joined = Join
                .fullOuter(getDepartments())
                .withKey(d -> d.getName().equals("Clerical") ? null : d.getId())
                .on(getEmployees())
                .withKey(e -> e.getName().equals("Jones") ? null : e.getDepartmentId())
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isStreamOf(
                Tuple.tuple(sales, rafferty),
                Tuple.tuple(salesTwo, rafferty),
                Tuple.tuple(null, jones),
                Tuple.tuple(engineering, heisenberg),
                Tuple.tuple(null, robinson),
                Tuple.tuple(null, smith),
                Tuple.tuple(clerical, null),
                Tuple.tuple(marketing, null),
                Tuple.tuple(storage, null),
                Tuple.tuple(null, williams),
                Tuple.tuple(null, scruffy)));
    }

    private static <T> Set<T> asSet(T... items) {
        return new HashSet<>(asList(items));
    }
}