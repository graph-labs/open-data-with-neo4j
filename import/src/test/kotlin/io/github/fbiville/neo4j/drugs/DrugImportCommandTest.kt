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
package io.github.fbiville.neo4j.drugs

import io.github.fbiville.neo4j.StringSimilarityFunction
import io.github.fbiville.neo4j.main
import org.assertj.core.api.Assertions.assertThat
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

class DrugImportCommandTest {

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
    val db = Neo4jRule().withFunction(StringSimilarityFunction::class.java)

    private lateinit var tsvFile: Path

    @Before
    fun prepare() {
        tsvFile = file.newFile().toPath()
        Files.write(tsvFile, listOf(
                "61266250\tA 313 200 000 UI POUR CENT, pommade\tpommade\tcutanée\tAutorisation active\tProcédure nationale\tCommercialisée\t12/03/1998\t\t\t PHARMA DEVELOPPEMENT\tNon",
                "62869109\tA 313 50 000 U.I., capsule molle\tcapsule molle\torale\tAutorisation active\tProcédure nationale\tCommercialisée\t07/07/1997\t\t\t PHARMA DEVELOPPEMENT\tNon"
        ))
    }

    @Test
    fun `imports specified dump into specified database`() {
        main(arrayOf(
                "drugs",
                "--defined-in",
                tsvFile.toFile().absolutePath,
                "--to-graph",
                db.boltURI().toString()
        ))

        db.graphDatabaseService.execute("MATCH (d:Drug) RETURN COUNT(d) AS count").use {
            val counts = it.columnAs<Long>("count").stream().collect(Collectors.toList())
            assertThat(counts)
                    .overridingErrorMessage("Expected 2 drugs to be imported from this dump, got: %s", counts)
                    .containsExactly(2)
        }
    }
}