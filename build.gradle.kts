import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    kotlin("jvm") version "1.4.10"
    kotlin("plugin.serialization") version "1.4.10"
    id("com.github.johnrengelman.shadow") version "5.2.0"
    application
}
group = "me.ben"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven(url = "https://kotlin.bintray.com/kotlinx/")
    jcenter()
}

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.0.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.4.2")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactive:1.4.2")
    implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.1.0")
    implementation("io.lettuce:lettuce-core:6.0.1.RELEASE")
    testImplementation(kotlin("test-junit"))
}
tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "1.8"
}
application {
    mainClassName = "MainKt"
}

tasks.withType<ShadowJar>() {
    manifest {
        attributes["Main-Class"] = "MainKt"
    }
}

