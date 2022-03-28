package ch.heig.mac;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;

public class Requests {
    private static final  Logger LOGGER = Logger.getLogger(Requests.class.getName());
    private final Driver driver;

    public Requests(Driver driver) {
        this.driver = driver;
    }

    public List<String> getDbLabels() {
        var dbVisualizationQuery = "CALL db.labels";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list(t -> t.get("label").asString());
        }
    }

    public List<Record> possibleSpreaders() {
        var dbVisualizationQuery = "match(pSick:Person{healthstatus: 'Sick'})-[vSick:VISITS]->(place:Place)<-[vH:VISITS]-(pH:Person{healthstatus: 'Healthy'})\n" +
                "where vH.starttime > pSick.confirmedtime and vSick.starttime > pSick.confirmedtime\n" +
                "return distinct pSick.name as sickName";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> possibleSpreadCounts() {
        var dbVisualizationQuery =
                "MATCH(pSick:Person{healthstatus: 'Sick'})-[vSick:VISITS]->(place:Place)<-[vH:VISITS]-(pH:Person{healthstatus: 'Healthy'})\n" +
                "WHERE vH.starttime > pSick.confirmedtime AND vSick.starttime > pSick.confirmedtime\n" +
                "RETURN distinct pSick.name as sickName, size(collect(distinct pH.name)) as nbHealthy";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> carelessPeople() {
        var dbVisualizationQuery =
                "MATCH (n:Person{healthstatus:'Sick'})-[v:VISITS]-(p:Place)\n" +
                "WHERE v.starttime > n.confirmedtime\n" +
                "WITH n, size(collect(p.name)) AS nbPlaces\n" +
                "WHERE nbPlaces > 10\n" +
                "RETURN n.name AS sickName, nbPlaces\n" +
                "ORDER BY nbPlaces";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> sociallyCareful() {
        var dbVisualizationQuery =
                "MATCH(pSick:Person{healthstatus: 'Sick'})-[vSick:VISITS]->(place:Place)\n" +
                "WHERE vSick.starttime > pSick.confirmedtime\n" +
                "WITH collect(distinct pSick) AS sickeWithBar\n" +
                "MATCH (p:Person{healthstatus: 'Sick'})\n" +
                "WITH collect(distinct p) AS sickPeople, sickeWithBar\n" +
                "WITH apoc.coll.subtract(sickPeople, sickeWithBar) AS sickWithoutBar\n" +
                "UNWIND sickWithoutBar AS tmp\n" +
                "RETURN tmp.name AS sickName";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> peopleToInform() {
        var dbVisualizationQuery =
                "MATCH(pSick:Person{healthstatus: 'Sick'})-[vSick:VISITS]->(place:Place)<-[vH:VISITS]-(pH:Person{healthstatus: 'Healthy'})\n" +
                "WHERE vSick.starttime > pSick.confirmedtime\n"+
                "AND vH.endtime > vSick.starttime\n"+
                "AND datetime() + duration.inSeconds(apoc.coll.max([vSick.starttime, vH.starttime]), apoc.coll.min([vSick.endtime, vH.endtime])) >= datetime() + duration(\"PT2H\")\n" +
                "RETURN distinct pSick.name AS sickName, collect(pH.name) AS peopleToInform";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> setHighRisk() {
        var dbVisualizationQuery =
                "MATCH(pSick:Person{healthstatus: 'Sick'})-[vSick:VISITS]->(place:Place)<-[vH:VISITS]-(pH:Person{healthstatus: 'Healthy'})\n" +
                "WHERE vSick.starttime > pSick.confirmedtime\n"+
                "AND vH.endtime > vSick.starttime\n"+
                "AND datetime() + duration.inSeconds(apoc.coll.max([vSick.starttime, vH.starttime]), apoc.coll.min([vSick.endtime, vH.endtime])) >= datetime() + duration(\"PT2H\")\n" +
                "WITH pSick.name as malade, collect(pH) as persons\n" +
                "UNWIND persons as listOfRisk\n" +
                "FOREACH(n in persons | set n.risk = \"high\")\n" +
                "RETURN distinct listOfRisk.name AS highRiskName";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.list();
        }
    }

    public List<Record> healthyCompanionsOf(String name) {
        var dbVisualizationQuery =
                "MATCH(p:Person{name:$name})-[v:VISITS]->(k:Place)<-[u:VISITS]-(o:Person{healthstatus: 'Healthy'})\n" +
                "WITH collect(distinct o) AS oc\n" +
                "UNWIND oc AS ou\n" +
                "MATCH(ou)-[v:VISITS]->(k:Place)<-[u:VISITS]-(o:Person{healthstatus: 'Healthy'})\n" +
                "WITH collect(distinct o) AS occ\n" +
                "UNWIND occ AS ouu\n" +
                "MATCH(ouu)-[v:VISITS]->(k:Place)<-[u:VISITS]-(o:Person{healthstatus: 'Healthy'})\n" +
                "WITH collect(distinct o) AS occc\n" +
                "UNWIND occc AS ouuu\n" +
                "RETURN ouuu.name AS healthyName";
        Map<String,Object> params = new HashMap<>();
        params.put( "name", name );

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery, params);
            return result.list();
        }
    }

    public Record topSickSite() {
        var dbVisualizationQuery =
                "MATCH(pSick:Person{healthstatus: 'Sick'})-[vSick:VISITS]->(place:Place)\n" +
                "WITH place, size(collect(distinct pSick)) AS nbOfSickVisits\n" +
                "RETURN place.type AS placeType, nbOfSickVisits\n" +
                "ORDER BY nbOfSickVisits desc\n" +
                "LIMIT 1";

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery);
            return result.single();
        }
    }

    public List<Record> sickFrom(List<String> names) {
        var dbVisualizationQuery =
                "MATCH (per:Person{healthstatus:'Sick'})\n" +
                "WHERE per.name IN $lst " +
                "RETURN per.name AS sickName";
        Map<String,Object> params = new HashMap<>();
        params.put( "lst", names );

        try (var session = driver.session()) {
            var result = session.run(dbVisualizationQuery, params);
            return result.list();
        }
    }
}
