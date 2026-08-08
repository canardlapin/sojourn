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
ThisBuild / githubWorkflowBuild += WorkflowStep.Sbt(
  List("checkModuleBoundaries"),
  name = Some("Check module boundaries")
)
ThisBuild / scalacOptions ++= Seq(
  "-Xmax-inlines:64",
  "-language:strictEquality",
  "-Wunused:imports",
  "-Wunused:locals",
  "-Wunused:privates",
  "-Wvalue-discard",
  "-Wnonunit-statement"
)
ThisBuild / Test / fork := true
ThisBuild / dependencyOverrides += Libraries.munit

/** Fail if scheduler artifacts leak into modules that must stay provider-neutral. */
lazy val checkModuleBoundaries = taskKey[Unit](
  "Verify sojourn-core and sojourn-tck do not depend on slurm4s artifacts"
)

def assertNoSlurm4s(moduleLabel: String, report: UpdateReport): Unit = {
  val forbidden = report.allModules.filter { m =>
    m.organization == "io.github.bbuchsbaum" && m.name.startsWith("slurm4s-")
  }
  if (forbidden.nonEmpty)
    sys.error(
      s"$moduleLabel must not depend on slurm4s artifacts; found: " +
        forbidden.map(m => s"${m.name}:${m.revision}").mkString(", ")
    )
}

lazy val root = project
  .in(file("."))
  .enablePlugins(NoPublishPlugin)
  .aggregate(core, worker, runtime, local, slurm, dsl, all, tck, demo)
  .settings(
    name := "sojourn",
    checkModuleBoundaries := {
      (core / checkModuleBoundaries).value
      (worker / checkModuleBoundaries).value
      (dsl / checkModuleBoundaries).value
      (tck / checkModuleBoundaries).value
    }
  )

// The scheduler-neutral kernel: typed site/lease/task surface plus the spool
// wire protocol. Its only cross-repository dependency is the provider-neutral
// remote-exec kernel; no scheduler runtime crosses this boundary.
lazy val core = project
  .in(file("modules/core"))
  .settings(
    name := "sojourn-core",
    libraryDependencies ++= Seq(
      Libraries.remoteExecKernel,
      Libraries.catsEffect,
      Libraries.fs2Core,
      Libraries.fs2Io,
      Libraries.circeCore,
      Libraries.circeParser,
      Libraries.munit % Test,
      Libraries.munitCatsEffect % Test,
      Libraries.munitScalaCheck % Test,
      Libraries.scalaCheck % Test
    ),
    checkModuleBoundaries := assertNoSlurm4s("sojourn-core", update.value)
  )

// Program / operation registry execution — backend-neutral. Must not depend on slurm4s.
lazy val worker = project
  .in(file("modules/worker"))
  .dependsOn(core)
  .settings(
    name := "sojourn-worker",
    libraryDependencies ++= Seq(
      Libraries.remoteExecKernel,
      Libraries.catsEffect,
      Libraries.fs2Core,
      Libraries.munit % Test,
      Libraries.munitCatsEffect % Test
    ),
    checkModuleBoundaries := assertNoSlurm4s("sojourn-worker", update.value)
  )

// Scheduler-neutral effectful machinery: store, spool, preflight. Still hosts
// SojournEntryPoint/WorkerBridge (slurm4s-worker) until those move fully into
// sojourn-slurm; registry types re-export from sojourn-worker.
lazy val runtime = project
  .in(file("modules/runtime"))
  .dependsOn(core, worker)
  .settings(
    name := "sojourn-runtime",
    libraryDependencies ++= Seq(
      Libraries.remoteExecKernel,
      Libraries.slurm4sWorker,
      Libraries.slurm4sProtocol,
      Libraries.catsEffect,
      Libraries.fs2Core,
      Libraries.fs2Io,
      Libraries.catsEffectTestkit % Test,
      Libraries.munit % Test,
      Libraries.munitCatsEffect % Test,
      Libraries.munitScalaCheck % Test,
      Libraries.scalaCheck % Test
    )
  )

// The scheduler-free backend: PoolCapableSite.
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

// Exemplary Slurm backend — Site (batch) today; PoolCapableSite before 1.0.0.
lazy val slurm = project
  .in(file("modules/slurm"))
  .dependsOn(runtime, tck % "test->compile")
  .settings(
    name := "sojourn-slurm",
    libraryDependencies ++= Seq(
      Libraries.slurm4sManaged,
      Libraries.slurm4sLocal,
      Libraries.slurm4sSsh,
      Libraries.munit % Test,
      Libraries.munitCatsEffect % Test
    )
  )

// Ergonomics: Op / Wire / Program / Simple* — no backend dependencies.
lazy val dsl = project
  .in(file("modules/dsl"))
  .dependsOn(core, worker)
  .settings(
    name := "sojourn-dsl",
    libraryDependencies ++= Seq(
      Libraries.catsEffect,
      Libraries.munit % Test,
      Libraries.munitCatsEffect % Test
    ),
    checkModuleBoundaries := assertNoSlurm4s("sojourn-dsl", update.value)
  )

// Convenience aggregate: Sojourn.local / slurm4sBatch constructors.
lazy val all = project
  .in(file("modules/all"))
  .dependsOn(dsl, local, slurm)
  .settings(
    name := "sojourn-all",
    libraryDependencies ++= Seq(
      Libraries.munit % Test,
      Libraries.munitCatsEffect % Test
    )
  )

// Published conformance kit.
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
    ),
    checkModuleBoundaries := assertNoSlurm4s("sojourn-tck", update.value)
  )

lazy val demo = project
  .in(file("modules/demo"))
  .enablePlugins(NoPublishPlugin)
  .dependsOn(local, slurm, tck, all)
  .settings(
    name := "sojourn-demo",
    Compile / mainClass := Some("io.github.bbuchsbaum.sojourn.demo.DemoWorkerMain"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case _                             => MergeStrategy.first
    },
    libraryDependencies ++= Seq(
      Libraries.munit % Test,
      Libraries.munitCatsEffect % Test
    )
  )
