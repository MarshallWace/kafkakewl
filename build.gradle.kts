/* do not remove or change this comment as this indicates the top of the file for the spotless licenseHeaderFile
 *
 */

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
            indentWithSpaces()
            endWithNewline()
            licenseHeaderFile(rootProject.file("license-header.kt"))
        }
        kotlinGradle {
            target("*.gradle.kts")
            trimTrailingWhitespace()
            indentWithSpaces()
            endWithNewline()
            licenseHeaderFile(rootProject.file("license-header.kt"), "/\\* do not remove or change")
        }
    }
}