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
package fr.graphlabs.neo4j.labs

import org.assertj.core.api.Assertions.assertThat
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.harness.junit.Neo4jRule

class LabsRepositoryTest {

    @get:Rule
    val neo4j = Neo4jRule()

    lateinit var subject: LabsRepository

    @Before
    fun prepare() {
        subject = LabsRepository(GraphDatabase.driver(neo4j.boltURI()))
    }

    @Test
    fun `finds labs by marketed drug name`() {
        execute("""
                CREATE (lab1:Company {identifier: 'XXX', name: 'Lab 1'})
                CREATE (lab2:Company {identifier: 'YYY', name: 'Lab 2'})
                CREATE (pack: Package {name:'12 mouchoirs', cip13Code: '1234567891011'})
                CREATE (drug: Drug {name:'Mouchoir', cisCode: '1234567'})
                CREATE (pack)<-[:DRUG_PACKAGED_AS]-(drug)-[:DRUG_HELD_BY]->(lab1),
                       (drug)-[:DRUG_HELD_BY]->(lab2)
                """.trimIndent())

        val labs = subject.findAllByMarketedDrugPackage("12 mouchoirs")

        assertThat(labs)
                .containsExactly(Lab("XXX", "Lab 1"), Lab("YYY", "Lab 2"))

    }

    private fun execute(cypher: String) {
        val db = neo4j.graphDatabaseService
        db.beginTx().use {
            db.execute(cypher)
            it.success()
        }
    }
}
