/* do not remove or change this comment as this indicates the top of the file for the spotless licenseHeaderFile
 *
 * This file was generated by the Gradle 'init' task.
 *
 * This settings file is used to specify which projects to include in your build-logic build.
 */

dependencyResolutionManagement {
    // Reuse version catalog from the main build.
    versionCatalogs {
        create("libs", { from(files("../gradle/libs.versions.toml")) })
    }
}

rootProject.name = "buildSrc"
