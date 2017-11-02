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
package fr.graphlabs.neo4j.health_professionals

import org.neo4j.driver.v1.AccessMode
import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.Record
import org.springframework.stereotype.Repository

@Repository
class HealthProfessionalsRepository(private val driver: Driver) {

    fun findTopThreeByMostBenefitsWithinYear(year: String): List<Pair<HealthProfessional, AggregatedBenefits>> {
        val result = driver.session(AccessMode.READ).use {
            val parameters = mapOf(Pair("year", year))
            it.run("""
                MATCH (:Year {year: {year}})<-[:MONTH_IN_YEAR]-(:Month)<-[:DAY_IN_MONTH]-(d:Day),
                      (bt:BenefitType)<-[:HAS_BENEFIT_TYPE]-(b:Benefit)-[:GIVEN_AT_DATE]->(d),
                      (lab:Company)-[:HAS_GIVEN_BENEFIT]->(b)-[:HAS_RECEIVED_BENEFIT]->(hp:HealthProfessional),
                      (hp)-[:SPECIALIZES_IN]->(ms:MedicalSpecialty)

                WITH ms, hp, SUM(toFloat(b.amount)) AS total_amount, COLLECT(DISTINCT lab.name) AS labs, COLLECT(bt.type) AS benefit_types
                ORDER BY total_amount DESC
                RETURN ms {.code, .name}, hp {.first_name, .last_name}, total_amount, labs, benefit_types
                LIMIT 3
            """.trimIndent(), parameters)
        }
        return result.list().map(this::toAggregatedHealthProfessionalBenefits)
    }

    private fun toAggregatedHealthProfessionalBenefits(record: Record): Pair<HealthProfessional, AggregatedBenefits> {
        val healthProfessional = record["hp"].asMap()
        val medicalSpecialty = record["ms"].asMap()
        return Pair(
                toHealthProfessional(healthProfessional, medicalSpecialty),
                toAggregatedBenefits(record)
        )
    }

    private fun toHealthProfessional(healthProfessional: MutableMap<String, Any>, medicalSpecialty: MutableMap<String, Any>): HealthProfessional {
        return HealthProfessional(
                healthProfessional["first_name"].toString(),
                healthProfessional["last_name"].toString(),
                toMedicalSpecialty(medicalSpecialty)
        )
    }

    private fun toAggregatedBenefits(record: Record): AggregatedBenefits {
        return AggregatedBenefits(
                record["total_amount"].asDouble().toString(),
                record["labs"].asList { it.asString() },
                record["benefit_types"].asList { it.asString() }
        )
    }

    private fun toMedicalSpecialty(medicalSpecialty: MutableMap<String, Any>): MedicalSpecialty {
        return MedicalSpecialty(
                medicalSpecialty["code"].toString(),
                medicalSpecialty["name"].toString()
        )
    }

}
