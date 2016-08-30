package de.infonautika.streamjoin.joins;

import de.infonautika.streamjoin.consumer.CombiningConsumer;
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
public class LeftOuterJoinTest {
    @Test
    public void completeScenario() throws Exception {
        List<Tuple<Employee, Department>> joined = leftOuterJoin(getEmployees(), getDepartments());

        assertThat(joined, containsInAnyOrder(
                tuple(rafferty, sales),
                tuple(rafferty, salesTwo),
                tuple(heisenberg, engineering),
                tuple(smith, clerical),
                tuple(jones, engineering),
                tuple(robinson, clerical),
                tuple(williams, null)
        ));
    }

    @Test
    public void oneToOne() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Employee rafael = new Employee(22, "Rafael");
        Department stock = new Department(5, "Stock");
        Department finance = new Department(22, "Finance");

        List<Tuple<Employee, Department>> joined = leftOuterJoin(Stream.of(rafael, unterberg), Stream.of(stock, finance));

        assertThat(joined, containsInAnyOrder(tuple(unterberg, stock), tuple(rafael, finance)));
    }


    @Test
    public void oneToN() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Employee rafael = new Employee(5, "Rafael");
        Department stock = new Department(5, "Stock");

        List<Tuple<Employee, Department>> joined = leftOuterJoin(Stream.of(rafael, unterberg), Stream.of(stock));

        assertThat(joined, containsInAnyOrder(tuple(unterberg, stock), tuple(rafael, stock)));
    }

    @Test
    public void NToOne() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Employee rafael = new Employee(5, "Rafael");
        Department stock = new Department(5, "Stock");

        List<Tuple<Department, Employee>> joined =
                new Joiner<>(
                        new LeftOuterJoin<>(
                                new Indexer<>(
                                        Stream.of(stock),
                                        Department::getId,
                                        Stream.of(rafael, unterberg),
                                        Employee::getDepartmentId)),
                        new CombiningConsumer<>(Tuple::new)
                )
                .doJoin()
                .collect(toList());

        assertThat(joined, containsInAnyOrder(tuple(stock, unterberg), tuple(stock, rafael)));
    }

    @Test
    public void unmatchedLeft() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Employee rafael = new Employee(44, "Rafael");
        Department stock = new Department(5, "Stock");

        List<Tuple<Employee, Department>> joined = leftOuterJoin(Stream.of(rafael, unterberg), Stream.of(stock));

        assertThat(joined, containsInAnyOrder(tuple(unterberg, stock), tuple(rafael, null)));
    }

    @Test
    public void unmatchedRight() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Department stock = new Department(5, "Stock");
        Department finance = new Department(2, "Finance");

        List<Tuple<Employee, Department>> joined = leftOuterJoin(Stream.of(unterberg), Stream.of(stock, finance));

        assertThat(joined, containsInAnyOrder(tuple(unterberg, stock)));
    }

    @Test
    public void unmatchedLeftWithNullKey() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Employee rafael = new Employee(null, "Rafael");
        Department stock = new Department(5, "Stock");

        List<Tuple<Employee, Department>> joined = leftOuterJoin(Stream.of(rafael, unterberg), Stream.of(stock));

        assertThat(joined, containsInAnyOrder(tuple(unterberg, stock), tuple(rafael, null)));
    }

    @Test
    public void unmatchedRightWithNullKey() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Department stock = new Department(5, "Stock");
        Department finance = new Department(null, "Finance");

        List<Tuple<Employee, Department>> joined = leftOuterJoin(Stream.of(unterberg), Stream.of(stock, finance));

        assertThat(joined, containsInAnyOrder(tuple(unterberg, stock)));
    }

    @Test
    public void nullKeyDoesNotMatch() throws Exception {
        Employee unterberg = new Employee(null, "Unterberg");
        Department finance = new Department(null, "Finance");

        List<Tuple<Employee, Department>> joined = leftOuterJoin(Stream.of(unterberg), Stream.of(finance));

        assertThat(joined, containsInAnyOrder(tuple(unterberg, null)));
    }

    private List<Tuple<Employee, Department>> leftOuterJoin(Stream<Employee> left, Stream<Department> right) {
        return new Joiner<>(
                new LeftOuterJoin<>(
                        new Indexer<>(
                                left,
                                Employee::getDepartmentId,
                                right,
                                Department::getId)),
                new CombiningConsumer<>(Tuple::new))
                .doJoin()
                .collect(toList());
    }
}