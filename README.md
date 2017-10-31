## Datasources

 - [ANSM Database](http://base-donnees-publique.medicaments.gouv.fr/telechargement.php)
 - [ETALab lab<->doctor gift exports](https://www.transparence.sante.gouv.fr/exports-etalab/exports-etalab.zip)

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

First, copy the extension JAR:
```
 $> mvn -am -pl similarity-extension clean package \
    && neo4j stop \
    && cp similarity-extension/target/similarity-extension.jar $NEO4J_HOME/plugins \
    && neo4j start
```

Then, convert the file to UTF-8:

```
 $> iconv -f ISO-8859-1 -t UTF-8//TRANSLIT /path/to/CIS_bdpm.txt -o /path/to/CIS_bdpm.utf8.txt 
```

Then, execute the import:
```
 $> java -jar import/target/import.jar drugs --defined-in ~/path/to/CIS_bdpm.txt \
                                             --to-graph bolt://localhost:7687 \
                                             --username neo4j \
                                             --password
 Neo4j password:
```

### Third: packages


First, convert the file to UTF-8:

```
 $> iconv -f ISO-8859-1 -t UTF-8//TRANSLIT /path/to/CIS_CIP_bdpm.txt -o /path/to/CIS_CIP_bdpm.utf8.txt
```

```
 $> java -jar import/target/import.jar packages --defined-in ~/path/to/CIS_CIP_bdpm.txt \
                                                --to-graph bolt://localhost:7687 \
                                                --username neo4j \
                                                --password
 Neo4j password:
```
