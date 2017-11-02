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

import com.nhaarman.mockito_kotlin.mock
import com.nhaarman.mockito_kotlin.whenever
import fr.graphlabs.neo4j.labs.*
import org.assertj.core.api.Assertions
import org.junit.Test

class HealthProfessionalApiTest {

    private val repository = mock<HealthProfessionalsRepository>()
    private val subject = HealthProfessionalApi(repository)

    private val secretarySpecialty = MedicalSpecialty("[SE]", "Secrétaire médical(e)")
    private val doctorSpecialty = MedicalSpecialty("[GE]", "Généraliste")
    private val aggregatedResults = listOf(
            Pair(
                    HealthProfessional("Jane2", "Doe", secretarySpecialty),
                    AggregatedBenefits("89000.0",
                            listOf("Censored labs", "XYZ labs"),
                            listOf("RESTAURANT", "TRANSPORT"))
            ),
            Pair(
                    HealthProfessional("Jane1", "Doe", doctorSpecialty),
                    AggregatedBenefits("85999.0",
                            listOf("Censored labs", "XYZ labs"),
                            listOf("RESTAURANT", "TRANSPORT"))
            ),
            Pair(
                    HealthProfessional("Jane4", "Doe", secretarySpecialty),
                    AggregatedBenefits("7000.0",
                            listOf("Censored labs"),
                            listOf("RESTAURANT"))
            )

    )

    @Test
    fun `returns labs matching the marketed drug package`() {
        whenever(repository.findTopThreeByMostBenefitsWithinYear("2022")).thenReturn(aggregatedResults)

        val result = subject.findTopThreeHealthProfessionalsWithBenefits("2022")

        Assertions.assertThat(result).isEqualTo(aggregatedResults)
    }
}
