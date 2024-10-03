/*
 * SPDX-FileCopyrightText: 2024 Marshall Wace <opensource@mwam.com>
 *
 * SPDX-License-Identifier: Apache-2.0
 */

/* do not remove or change this comment as this indicates the top of the file for the spotless licenseHeaderFile
 *
 * This file was generated by the Gradle 'init' task.
 */

plugins {
    id("buildlogic.kotlin-application-conventions")
    id("io.ktor.plugin")
}

dependencies {
    implementation("org.apache.commons:commons-text")
    implementation(project(":kafkakewl-common"))
    implementation(project(":kafkakewl-domain"))
    implementation(project(":kafkakewl-utils"))
}

application {
    mainClass.set("com.mwam.kafkakewl.deploy.ApplicationKt")
}
