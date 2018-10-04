package de.infonautika.streamjoin;

import de.infonautika.streamjoin.repo.Department;
import de.infonautika.streamjoin.repo.Employee;
import de.infonautika.streamjoin.repo.Tuple;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.infonautika.streamjoin.StreamMatcher.isEmptyStream;
import static de.infonautika.streamjoin.StreamMatcher.isStreamOf;
import static de.infonautika.streamjoin.repo.TestRepository.*;
import static de.infonautika.streamjoin.repo.Tuple.tuple;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;

@SuppressWarnings("unchecked")
public class JoinTest {
    @Test
    public void innerJoinWithCombiner() {
        Stream<Tuple<Department, Employee>> joined = Join
                .join(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isStreamOf(
                tuple(sales, rafferty),
                tuple(salesTwo, rafferty),
                tuple(engineering, jones),
                tuple(engineering, heisenberg),
                tuple(clerical, robinson),
                tuple(clerical, smith)));
    }

    @Test
    public void innerJoinWithNullKey() {
        Stream<Tuple<Department, Employee>> joined = Join
                .join(getDepartments())
                .withKey(d -> d.getName().equals("Clerical") ? null : d.getId())
                .on(getEmployees())
                .withKey(e -> e.getName().equals("Jones") ? null : e.getDepartmentId())
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isStreamOf(
                tuple(sales, rafferty),
                tuple(salesTwo, rafferty),
                tuple(engineering, heisenberg)));
    }

    @Test
    public void innerJoinWithGrouper() {
        Stream<Tuple<Department, Set<Employee>>> joined = Join
                .join(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .group((d, es) -> tuple(d, es.collect(toSet())))
                .asStream();

        assertThat(joined, isStreamOf(
                tuple(sales, asSet(rafferty)),
                tuple(salesTwo, asSet(rafferty)),
                tuple(engineering, asSet(jones,heisenberg)),
                tuple(clerical, asSet(robinson, smith))));
    }

    @Test
    public void leftOuterJoinWithCombiner() {
        Stream<Tuple<Department, Employee>> joined = Join
                .leftOuter(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isStreamOf(
                tuple(sales, rafferty),
                tuple(salesTwo, rafferty),
                tuple(engineering, jones),
                tuple(engineering, heisenberg),
                tuple(clerical, robinson),
                tuple(clerical, smith),
                tuple(marketing, null),
                tuple(storage, null)));
    }

    @Test
    public void leftOuterJoinWithNullKey() {
        Stream<Tuple<Department, Employee>> joined = Join
                .leftOuter(getDepartments())
                .withKey(d -> d.getName().equals("Clerical") ? null : d.getId())
                .on(getEmployees())
                .withKey(e -> e.getName().equals("Jones") ? null : e.getDepartmentId())
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isStreamOf(
                tuple(sales, rafferty),
                tuple(salesTwo, rafferty),
                tuple(engineering, heisenberg),
                tuple(clerical, null),
                tuple(marketing, null),
                tuple(storage, null)));
    }

    @Test
    public void leftOuterJoinWithGrouper() {
        Stream<Tuple<Department, Set<Employee>>> joined = Join
                .leftOuter(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .group((d, es) -> tuple(d, es.collect(toSet())))
                .withLeftUnmatched(l -> tuple(l, asSet()))
                .asStream();

        assertThat(joined, isStreamOf(
                tuple(sales, asSet(rafferty)),
                tuple(salesTwo, asSet(rafferty)),
                tuple(engineering, asSet(jones,heisenberg)),
                tuple(clerical, asSet(robinson, smith)),
                tuple(marketing, asSet()),
                tuple(storage, asSet())));
    }

    @Test
    public void emptyLeftYieldsEmptyStream() {
        Stream<Tuple<Department, Employee>> joined = Join
                .join(Stream.<Department>empty())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isEmptyStream());
    }

    @Test
    public void emptyRightYieldsEmptyStream() {
        Stream<Tuple<Department, Employee>> joined = Join
                .join(getDepartments())
                .withKey(Department::getId)
                .on(Stream.<Employee>empty())
                .withKey(Employee::getDepartmentId)
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isEmptyStream());
    }

    @Test
    public void innerJoinWithMatchPredicate() {
        Stream<Tuple<Department, Set<Employee>>> joined = Join
                .join(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .matching((k1, k2) -> k1 > k2)
                .group((department, employeeStream) -> tuple(department, employeeStream.collect(Collectors.toSet())))
                .asStream();

        assertThat(joined, isStreamOf(
                tuple(engineering, asSet(rafferty)),
                tuple(clerical, asSet(rafferty, jones, heisenberg)),
                tuple(marketing, asSet(rafferty, jones, heisenberg, robinson, smith))));
    }

    @Test
    public void leftJoinWithMatchPredicate() {
        Stream<Tuple<Department, Employee>> joined = Join
                .leftOuter(getDepartments())
                .withKey(Department::getId)
                .on(getEmployees())
                .withKey(Employee::getDepartmentId)
                .matching((k1, k2) -> k1 > k2)
                .combine(Tuple::tuple)
                .asStream();

        assertThat(joined, isStreamOf(
                tuple(engineering, rafferty),
                tuple(clerical, rafferty),
                tuple(clerical, jones),
                tuple(clerical, heisenberg),
                tuple(marketing, rafferty),
                tuple(marketing, jones),
                tuple(marketing, heisenberg),
                tuple(marketing, robinson),
                tuple(marketing, smith),
                tuple(sales, null),
                tuple(salesTwo, null),
                tuple(storage, null)));
    }

    @SafeVarargs
    private static <T> Set<T> asSet(T... items) {
        return new HashSet<>(asList(items));
    }
}