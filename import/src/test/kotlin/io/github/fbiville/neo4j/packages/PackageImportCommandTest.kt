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
package io.github.fbiville.neo4j.packages

import io.github.fbiville.neo4j.StringSimilarityFunction
import io.github.fbiville.neo4j.main
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

class PackageImportCommandTest {

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
            "60002283\t4949729\tplaquette(s) PVC PVDC aluminium de 30 comprimé(s)\tPrésentation active\tDéclaration de commercialisation\t16/03/2011\t3400949497294\toui\t100%\t44,38\t45,40\t1,02",
            "60002283\t4949770\tplaquette(s) PVC PVDC aluminium de 90 comprimé(s)\tPrésentation active\tDéclaration de commercialisation\t19/09/2011\t3400949497706\toui\t100%\t120,04\t121,06\t1,02",
            "60002504\t3320863\ttube(s) polypropylène de 30 comprimé(s)\tPrésentation active\tDéclaration de commercialisation\t03/12/2003\t3400933208639\toui\t15%\t7,75\t8,77\t1,02",
            "60003620\t3696350\t20 récipient(s) unidose(s) polyéthylène de 2 ml suremballée(s)/surpochée(s) par plaquette de 5 récipients unidoses\tPrésentation active\tDéclaration de commercialisation\t30/11/2006\t3400936963504\toui\t65%\t38,11\t39,13\t1,02"
        ))
    }

    @Test
    fun `imports specified dump into specified database`() {
        main(arrayOf(
                "packages",
                "--defined-in",
                tsvFile.toFile().absolutePath,
                "--to-graph",
                db.boltURI().toString()
        ))

        db.graphDatabaseService.execute("MATCH (p:Package) RETURN COUNT(p) AS count").use {
            val counts = it.columnAs<Long>("count").stream().collect(Collectors.toList())
            Assertions.assertThat(counts)
                    .overridingErrorMessage("Expected 4 packages to be imported from this dump, got: %s", counts)
                    .containsExactly(4)
        }
    }
}