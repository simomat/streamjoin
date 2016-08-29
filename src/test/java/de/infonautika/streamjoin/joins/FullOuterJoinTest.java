package de.infonautika.streamjoin.joins;

import de.infonautika.streamjoin.joins.indexing.Indexer;
import de.infonautika.streamjoin.joins.repo.Department;
import de.infonautika.streamjoin.joins.repo.Employee;
import de.infonautika.streamjoin.joins.repo.Tuple;
import org.junit.Test;

import java.util.List;
import java.util.stream.Stream;

import static de.infonautika.streamjoin.joins.repo.TestRepository.*;
import static de.infonautika.streamjoin.joins.repo.Tuple.tuple;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

@SuppressWarnings("unchecked")
public class FullOuterJoinTest {

    @Test
    public void completeScenario() throws Exception {

        List<Tuple<Employee, Department>> joined = outerJoin(getEmployees(), getDepartments());

        assertThat(joined, containsInAnyOrder(
                tuple(rafferty, sales),
                tuple(rafferty, salesTwo),
                tuple(heisenberg, engineering),
                tuple(smith, clerical),
                tuple(jones, engineering),
                tuple(robinson, clerical),
                tuple(williams, null),
                tuple(null, marketing),
                tuple(null, storage)
        ));
    }

    @Test
    public void oneToOne() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Employee rafael = new Employee(22, "Rafael");
        Department stock = new Department(5, "Stock");
        Department finance = new Department(22, "Finance");

        List<Tuple<Employee, Department>> joined = new Joiner<>(
                new FullOuterJoin<>(
                        new Indexer<>(
                                Stream.of(rafael, unterberg),
                                Employee::getDepartmentId,
                                Stream.of(stock, finance),
                                Department::getId),
                        (e, ds) -> ds.map(d -> tuple(e, d))))
                .doJoin()
                .collect(toList());

        assertThat(joined, containsInAnyOrder(tuple(unterberg, stock), tuple(rafael, finance)));
    }


    @Test
    public void oneToN() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Employee rafael = new Employee(5, "Rafael");
        Department stock = new Department(5, "Stock");

        List<Tuple<Employee, Department>> joined = outerJoin(Stream.of(rafael, unterberg), Stream.of(stock));

        assertThat(joined, containsInAnyOrder(tuple(unterberg, stock), tuple(rafael, stock)));
    }

    @Test
    public void NToOne() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Employee rafael = new Employee(5, "Rafael");
        Department stock = new Department(5, "Stock");

        List<Tuple<Department, Employee>> joined = new Joiner<>(
                new FullOuterJoin<>(
                        new Indexer<>(
                                Stream.of(stock),
                                Department::getId,
                                Stream.of(rafael, unterberg),
                                Employee::getDepartmentId),
                        (d, es) -> es.map(e -> tuple(d, e))))
                .doJoin()
                .collect(toList());

        assertThat(joined, containsInAnyOrder(tuple(stock, unterberg), tuple(stock, rafael)));
    }

    @Test
    public void unmatchedLeft() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Employee rafael = new Employee(44, "Rafael");
        Department stock = new Department(5, "Stock");

        List<Tuple<Employee, Department>> joined = outerJoin(Stream.of(rafael, unterberg), Stream.of(stock));

        assertThat(joined, containsInAnyOrder(tuple(unterberg, stock), tuple(rafael, null)));
    }

    @Test
    public void unmatchedRight() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Department stock = new Department(5, "Stock");
        Department finance = new Department(2, "Finance");

        List<Tuple<Employee, Department>> joined = outerJoin(Stream.of(unterberg), Stream.of(stock, finance));

        assertThat(joined, containsInAnyOrder(tuple(unterberg, stock), tuple(null, finance)));
    }

    @Test
    public void unmatchedLeftWithNullKey() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Employee rafael = new Employee(null, "Rafael");
        Department stock = new Department(5, "Stock");

        List<Tuple<Employee, Department>> joined = outerJoin(Stream.of(rafael, unterberg), Stream.of(stock));

        assertThat(joined, containsInAnyOrder(tuple(unterberg, stock), tuple(rafael, null)));
    }

    @Test
    public void unmatchedRightWithNullKey() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Department stock = new Department(5, "Stock");
        Department finance = new Department(null, "Finance");

        List<Tuple<Employee, Department>> joined = outerJoin(Stream.of(unterberg), Stream.of(stock, finance));

        assertThat(joined, containsInAnyOrder(tuple(unterberg, stock), tuple(null, finance)));
    }

    @Test
    public void nullKeyDoesNotMatch() throws Exception {
        Employee unterberg = new Employee(null, "Unterberg");
        Department finance = new Department(null, "Finance");

        Stream<Employee> left = Stream.of(unterberg);
        Stream<Department> right = Stream.of(finance);
        List<Tuple<Employee, Department>> joined = outerJoin(left, right);

        assertThat(joined, containsInAnyOrder(tuple(unterberg, null), tuple(null, finance)));
    }

    private List<Tuple<Employee, Department>> outerJoin(Stream<Employee> left, Stream<Department> right) {
        return new Joiner<>(
                new FullOuterJoin<>(
                        new Indexer<>(left,
                                Employee::getDepartmentId,
                                right,
                                Department::getId),
                        (e, ds) -> ds.map(d -> tuple(e, d))))
                .doJoin()
                .collect(toList());
    }

}