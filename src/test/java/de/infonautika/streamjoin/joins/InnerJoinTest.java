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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;

@SuppressWarnings("unchecked")
public class InnerJoinTest {

    @Test
    public void completeScenario() throws Exception {
        List<Tuple<Employee, Department>> joined = innerJoin(getEmployees(), getDepartments());

        assertThat(joined, containsInAnyOrder(
                tuple(rafferty, sales),
                tuple(rafferty, salesTwo),
                tuple(heisenberg, engineering),
                tuple(smith, clerical),
                tuple(jones, engineering),
                tuple(robinson, clerical)
        ));
    }

    @Test
    public void oneToOne() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Employee rafael = new Employee(22, "Rafael");
        Department stock = new Department(5, "Stock");
        Department finance = new Department(22, "Finance");

        List<Tuple<Employee, Department>> joined = innerJoin(Stream.of(rafael, unterberg), Stream.of(stock, finance));

        assertThat(joined, containsInAnyOrder(tuple(unterberg, stock), tuple(rafael, finance)));
    }

    @Test
    public void oneToN() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Employee rafael = new Employee(5, "Rafael");
        Department stock = new Department(5, "Stock");

        List<Tuple<Employee, Department>> joined = innerJoin(Stream.of(rafael, unterberg), Stream.of(stock));

        assertThat(joined, containsInAnyOrder(tuple(unterberg, stock), tuple(rafael, stock)));
    }

    @Test
    public void NToOne() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Employee rafael = new Employee(5, "Rafael");
        Department stock = new Department(5, "Stock");

        List<Tuple<Department, Employee>> joined = new Joiner<>(
                new InnerEquiJoin<>(new Indexer<>(Stream.of(stock),
                        Department::getId,
                        Stream.of(rafael, unterberg),
                        Employee::getDepartmentId), (d, es) -> es.map(e -> tuple(d, e))))
                .doJoin()
                .collect(toList());

        assertThat(joined, containsInAnyOrder(tuple(stock, unterberg), tuple(stock, rafael)));
    }

    @Test
    public void unmatchedLeftWithNullKey() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Employee rafael = new Employee(null, "Rafael");
        Department stock = new Department(5, "Stock");

        List<Tuple<Employee, Department>> joined = innerJoin(Stream.of(rafael, unterberg), Stream.of(stock));

        assertThat(joined, containsInAnyOrder(tuple(unterberg, stock)));
    }

    @Test
    public void unmatchedRightWithNullKey() throws Exception {
        Employee unterberg = new Employee(5, "Unterberg");
        Department stock = new Department(5, "Stock");
        Department finance = new Department(null, "Finance");

        List<Tuple<Employee, Department>> joined = innerJoin(Stream.of(unterberg), Stream.of(stock, finance));

        assertThat(joined, containsInAnyOrder(tuple(unterberg, stock)));
    }

    @Test
    public void nullKeyDoesNotMatch() throws Exception {
        Employee unterberg = new Employee(null, "Unterberg");
        Department finance = new Department(null, "Finance");

        List<Tuple<Employee, Department>> joined = innerJoin(Stream.of(unterberg), Stream.of(finance));

        assertThat(joined, is(empty()));
    }

    @Test
    public void emptyStreamsAccepted() throws Exception {
        List<Tuple<Employee, Department>> joined = innerJoin(Stream.empty(), Stream.empty());

        assertThat(joined, is(empty()));
    }
/*
    @Test
    public void noEquiMatchPredicate() throws Exception {
        Player freddy = new Player("Freddy", 34);
        Player zora = new Player("Zora", 22);
        Player tyrant = new Player("Tyrant", 33);
        Player gonzo = new Player("Gonzo", 1);

        List<Player> players = asList(freddy, zora, tyrant, gonzo);

        List<Tuple<Player, Player>> joined = new InnerEquiJoin<>(
                players.stream(),
                Player::getRank,
                players.stream(),
                Player::getRank,
                (l, r) -> l > r,
                (l, rs) -> rs.map(r -> Tuple.tuple(l, r)))
                .doJoin()
                .collect(toList());

        assertThat(joined, containsInAnyOrder(
                Tuple.tuple(freddy, zora), Tuple.tuple(freddy, tyrant), Tuple.tuple(freddy, gonzo),
                Tuple.tuple(tyrant, zora), Tuple.tuple(tyrant, gonzo),
                Tuple.tuple(zora, gonzo)));
    }
*/
    private List<Tuple<Employee, Department>> innerJoin(Stream<Employee> left, Stream<Department> right) {
        return new Joiner<>(
                new InnerEquiJoin<>(new Indexer<>(left,
                        Employee::getDepartmentId,
                        right,
                        Department::getId), (e, ds) -> ds.map(d -> tuple(e, d))))
                .doJoin()
                .collect(toList());
    }

}