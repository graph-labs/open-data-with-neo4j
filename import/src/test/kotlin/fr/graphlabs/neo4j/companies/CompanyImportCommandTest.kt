/**
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.graphlabs.neo4j.companies

import fr.graphlabs.neo4j.main
import org.assertj.core.api.Assertions
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.neo4j.harness.junit.Neo4jRule
import org.slf4j.bridge.SLF4JBridgeHandler
import java.nio.file.Files
import java.nio.file.Path
import java.util.stream.Collectors

class CompanyImportCommandTest {

    companion object {
        @JvmStatic
        @BeforeClass
        fun prepareAll() {
            SLF4JBridgeHandler.removeHandlersForRootLogger()
        }
    }

    @get:Rule
    val file = TemporaryFolder()

    @get:Rule
    val db = Neo4jRule()

    private lateinit var csvFile: Path

    @Before
    fun prepare() {
        csvFile = file.newFile().toPath()
        Files.write(csvFile, listOf(
                "identifiant,pays_code,pays,secteur_activite_code,secteur,denomination_sociale,adresse_1,adresse_2,adresse_3,adresse_4,code_postal,ville",
                "XSQKIAGK,[FR],FRANCE,[DM],Dispositifs médicaux,Cook France SARL,2 Rue due Nouveau Bercy,\"\",\"\",\"\",94227,Charenton Le Pont Cedex",
                "ARHHJTWT,[FR],FRANCE,[DM],Dispositifs médicaux,EYETECHCARE,2871 Avenue de l'Europe,\"\",\"\",\"\",69140,RILLIEUX-LA-PAPE"
        ))
    }

    @Test
    fun `imports specified dump into specified database`() {
        main(arrayOf(
                "companies",
                "--defined-in",
                csvFile.toFile().absolutePath,
                "--to-graph",
                db.boltURI().toString()
        ))

        db.graphDatabaseService.execute("MATCH (c:Company) RETURN COUNT(c) AS count").use {
            val counts = it.columnAs<Long>("count").stream().collect(Collectors.toList())
            Assertions.assertThat(counts)
                    .overridingErrorMessage("Expected 2 companies to be imported from this dump")
                    .containsExactly(2)
        }
    }
}
