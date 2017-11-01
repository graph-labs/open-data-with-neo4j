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
package fr.graphlabs.neo4j.packages

import fr.graphlabs.neo4j.CommitCounter
import fr.graphlabs.neo4j.Readers.newReader
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.groups.Tuple
import org.junit.*
import org.neo4j.graphdb.Label
import org.neo4j.harness.junit.Neo4jRule
import org.slf4j.bridge.SLF4JBridgeHandler

class PackageImporterTest {

    @get:Rule
    val graphDb = Neo4jRule()

    private lateinit var subject: PackageImporter

    private lateinit var commitCounter: CommitCounter

    @Before
    fun prepare() {
        subject = PackageImporter(graphDb.boltURI().toString())
        commitCounter = CommitCounter()
        graphDb.graphDatabaseService.registerTransactionEventHandler(commitCounter)
    }

    companion object {
        @JvmStatic
        @BeforeClass
        fun prepareAll() {
            SLF4JBridgeHandler.removeHandlersForRootLogger()
        }
    }

    @Test
    fun `imports packages`() {
        newReader("/packages.tsv").use {
            subject.import(it)
        }

        graphDb.graphDatabaseService.execute("""
            |MATCH (package:Package)<-[:DRUG_PACKAGED_AS]-(drug:Drug)
            |RETURN drug {.cis_code}, package {.cip13_code, .name}
            |ORDER BY drug.cis_code, package.cip13_code""".trimMargin()).use {

            assertThat(it).containsExactly(
                    mapOf(
                            Pair("drug", mapOf(Pair("cis_code", "60002283"))),
                            Pair("package", mapOf(
                                    Pair("cip13_code", "3400949497294"),
                                    Pair("name", "PLAQUETTE(S) PVC PVDC ALUMINIUM DE 30 COMPRIMÉ(S)")))
                    ),
                    mapOf(
                            Pair("drug", mapOf(Pair("cis_code", "60002283"))),
                            Pair("package", mapOf(
                                    Pair("cip13_code", "3400949497706"),
                                    Pair("name", "PLAQUETTE(S) PVC PVDC ALUMINIUM DE 90 COMPRIMÉ(S)")))
                    ),
                    mapOf(
                            Pair("drug", mapOf(Pair("cis_code", "60002504"))),
                            Pair("package", mapOf(
                                    Pair("cip13_code", "3400933208639"),
                                    Pair("name", "TUBE(S) POLYPROPYLÈNE DE 30 COMPRIMÉ(S)")))
                    ),
                    mapOf(
                            Pair("drug", mapOf(Pair("cis_code", "60003620"))),
                            Pair("package", mapOf(
                                    Pair("cip13_code", "3400936963504"),
                                    Pair("name", "20 RÉCIPIENT(S) UNIDOSE(S) POLYÉTHYLÈNE DE 2 ML SUREMBALLÉE(S)/SURPOCHÉE(S) PAR PLAQUETTE DE 5 RÉCIPIENTS UNIDOSES")))
                    )
            )
        }
        assertThat(commitCounter.getCount()).isEqualTo(1)
    }

    @Test
    fun `creates indices for packages`() {
        newReader("/packages.tsv").use {
            subject.import(it)
        }

        val packageLabel = Label.label("Package")
        graphDb.graphDatabaseService.beginTx().use {
            assertThat(graphDb.graphDatabaseService.schema().indexes)
                    .extracting("label", "propertyKeys", "constraintIndex")
                    .contains(
                            Tuple.tuple(packageLabel, listOf("name"), false),
                            Tuple.tuple(packageLabel, listOf("cip13_code"), true)
                    )
        }
    }

    @Test
    fun `creates indices for unmatched drugs`() {
        newReader("/packages.tsv").use {
            subject.import(it)
        }

        graphDb.graphDatabaseService.beginTx().use {
            assertThat(graphDb.graphDatabaseService.schema().indexes)
                    .extracting("label", "propertyKeys", "constraintIndex")
                    .contains(
                            Tuple.tuple(Label.label("DrugFromPackage"), listOf("cis_code"), true)
                    )
        }
    }
}
