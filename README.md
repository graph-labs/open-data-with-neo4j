## Datasources

 - [ANSM Database](http://base-donnees-publique.medicaments.gouv.fr/telechargement.php)
 - [ETALab lab<->doctor gift exports](https://www.google.com/url?q=https%3A%2F%2Fwww.transparence.sante.gouv.fr%2Fexports-etalab%2Fexports-etalab.zip&sa=D&sntz=1&usg=AFQjCNEJa-Qa-OI1wOPmnLOGwh5XiV8OkQ)

## Import example

### First: Labs

```
 $> java -jar import/target/import.jar companies --defined-in ~/path/to/entreprise_xxx.csv \
                                                 --to-graph bolt://localhost:7687 \
                                                 --username neo4j \
                                                 --password
 Neo4j password:
```

### Second: Drugs

```
 $> java -jar import/target/import.jar drugs --defined-in ~/path/to/CIS_bdpm.txt \
                                             --to-graph bolt://localhost:7687 \
                                             --username neo4j \
                                             --password
 Neo4j password:
```

### Third: packages

```
 $> java -jar import/target/import.jar packages --defined-in ~/path/to/CIS_CIP_bdpm.txt \
                                                --to-graph bolt://localhost:7687 \
                                                --username neo4j \
                                                --password
 Neo4j password:
```
