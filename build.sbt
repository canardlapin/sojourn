import Dependencies.*

ThisBuild / tlBaseVersion := "0.2"
ThisBuild / organization := "io.github.bbuchsbaum"
ThisBuild / organizationName := "Bradley Buchsbaum"
ThisBuild / startYear := Some(2026)
ThisBuild / licenses := Seq(License.Apache2)
ThisBuild / developers := List(tlGitHubDev("bbuchsbaum", "Bradley Buchsbaum"))
ThisBuild / scalaVersion := Versions.scala3
ThisBuild / crossScalaVersions := Seq(Versions.scala3)
ThisBuild / tlJdkRelease := Some(17)
ThisBuild / githubWorkflowJavaVersions := Seq(JavaSpec.temurin("17"), JavaSpec.temurin("21"))
ThisBuild / tlCiScalafmtCheck := true
ThisBuild / tlCiHeaderCheck := false
ThisBuild / scalacOptions += "-Xmax-inlines:64"
ThisBuild / Test / fork := true

lazy val root = project
  .in(file("."))
  .enablePlugins(NoPublishPlugin)
  .aggregate(core, runtime, local, tck, demo)
  .settings(name := "sojourn")

// The scheduler-neutral kernel: typed site/lease/task surface plus the spool
// wire protocol. Depends on scala-slurm-core for the shared identifier and
// evidence vocabulary only; no scheduler runtime crosses this boundary.
lazy val core = project
  .in(file("modules/core"))
  .settings(
    name := "sojourn-core",
    libraryDependencies ++= Seq(
      Libraries.scalaSlurmCore,
      Libraries.catsEffect,
      Libraries.fs2Core,
      Libraries.fs2Io,
      Libraries.circeCore,
      Libraries.circeParser,
      Libraries.munit % Test,
      Libraries.munitCatsEffect % Test,
      Libraries.munitScalaCheck % Test,
      Libraries.scalaCheck % Test
    )
  )

// Scheduler-neutral effectful machinery: the content-addressed shared-filesystem
// store, the operation registry, the one-binary entry point (one-shot batch mode
// now; pilot mode arrives with the spool runtime), site preflight probes, and
// release staging. Depends on scala-slurm worker for the AtomicFiles kernel and
// the envelope/publication discipline — worker is scheduler-free.
lazy val runtime = project
  .in(file("modules/runtime"))
  .dependsOn(core)
  .settings(
    name := "sojourn-runtime",
    libraryDependencies ++= Seq(
      Libraries.scalaSlurmWorker,
      Libraries.scalaSlurmProtocol,
      Libraries.catsEffect,
      Libraries.fs2Core,
      Libraries.fs2Io,
      Libraries.munit % Test,
      Libraries.munitCatsEffect % Test,
      Libraries.munitScalaCheck % Test,
      Libraries.scalaCheck % Test
    )
  )

// The scheduler-free backend: batch execution on supervised fibers through the
// real envelope-publication path over a local filesystem store. Proves the SPI
// is implementable without a scheduler and gives the TCK a fast target.
lazy val local = project
  .in(file("modules/local"))
  .dependsOn(runtime, tck % "test->compile")
  .settings(
    name := "sojourn-local",
    libraryDependencies ++= Seq(
      Libraries.munit % Test,
      Libraries.munitCatsEffect % Test
    )
  )

// Published conformance kit (main scope, cats-laws pattern): abstract suites a
// Site[F] backend instantiates over a TckHarness Resource.
lazy val tck = project
  .in(file("modules/tck"))
  .dependsOn(core)
  .settings(
    name := "sojourn-tck",
    libraryDependencies ++= Seq(
      Libraries.catsEffect,
      Libraries.munit,
      Libraries.munitCatsEffect,
      Libraries.scalaCheck,
      Libraries.munitScalaCheck % Test
    )
  )

// Unpublished demo operations and entry-point mains used by tests and acceptance.
lazy val demo = project
  .in(file("modules/demo"))
  .enablePlugins(NoPublishPlugin)
  .dependsOn(local)
  .settings(
    name := "sojourn-demo",
    libraryDependencies ++= Seq(
      Libraries.munit % Test,
      Libraries.munitCatsEffect % Test
    )
  )
