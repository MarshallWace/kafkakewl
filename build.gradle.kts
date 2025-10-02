/* do not remove or change this comment as this indicates the top of the file for the spotless licenseHeaderFile
 *
 */

import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("com.diffplug.spotless")
    kotlin("plugin.serialization")
}

subprojects {
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "org.jetbrains.kotlin.plugin.serialization")

    spotless {
        kotlin {
            trimTrailingWhitespace()
            leadingTabsToSpaces()
            endWithNewline()
            licenseHeaderFile(rootProject.file("license-header.kt"))
        }
        kotlinGradle {
            target("*.gradle.kts")
            trimTrailingWhitespace()
            leadingTabsToSpaces()
            endWithNewline()
            licenseHeaderFile(rootProject.file("license-header.kt"), "/\\* do not remove or change")
        }
    }

    tasks.withType<KotlinCompile>().configureEach {
        compilerOptions {
            freeCompilerArgs.add("-opt-in=kotlin.time.ExperimentalTime")
        }
    }
}