#!/usr/bin/env bun
/**
 * CLI tool for building the nationwide address database.
 *
 * Commands: urls - Generate list of TIGER ADDRFEAT URLs download - Download
 * ADDRFEAT files from URL list build - Build SQLite database from downloaded
 * files gazetteer - Download ZCTA gazetteer for ZIP centroids
 */

import { Database } from "bun:sqlite"
import fs from "node:fs/promises"
import path from "node:path"
import { Glob } from "bun"
import { program } from "commander"
import { DBFFile } from "dbffile"
import { doubleMetaphone } from "double-metaphone"
import pLimit from "p-limit"
import * as yauzl from "yauzl-promise"
import packageJson from "../package.json"

const TIGER_BASE_URL = "https://www2.census.gov/geo/tiger/TIGER2024/ADDRFEAT"
const GAZETTEER_URL =
  "https://www2.census.gov/geo/docs/maps-data/data/gazetteer/2024_Gazetteer/2024_Gaz_zcta_national.zip"

// ─────────────────────────────────────────────────────────────────────────────
// URL List Command
// ─────────────────────────────────────────────────────────────────────────────

async function generateUrls(output: string) {
  console.log("Fetching ADDRFEAT file list from Census Bureau...")

  const response = await fetch(TIGER_BASE_URL)
  const html = await response.text()

  const matches = html.match(/tl_2024_\d+_addrfeat\.zip/g) ?? []
  const unique = [...new Set(matches)]
  const urls = unique.map((filename) => `${TIGER_BASE_URL}/${filename}`)

  console.log(`Found ${urls.length} ADDRFEAT files`)

  if (output === "-") {
    for (const url of urls) console.log(url)
  } else {
    await Bun.write(output, urls.join("\n") + "\n")
    console.log(`Written to ${output}`)
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Download Command
// ─────────────────────────────────────────────────────────────────────────────

async function downloadFiles(
  urlFile: string,
  outputDir: string,
  concurrency: number,
) {
  const content = await Bun.file(urlFile).text()
  const urls = content.trim().split("\n").filter(Boolean)

  console.log(`Downloading ${urls.length} files to ${outputDir}...`)
  await fs.mkdir(outputDir, { recursive: true })

  const limit = pLimit(concurrency)
  let completed = 0
  let errors = 0

  const tasks = urls.map((url) =>
    limit(async () => {
      const filename = path.basename(url)
      const outputPath = path.join(outputDir, filename)

      // Skip if already exists
      try {
        await fs.access(outputPath)
        completed++
        return
      } catch {
        // File doesn't exist, download it
      }

      try {
        const response = await fetch(url)
        if (!response.ok) throw new Error(`HTTP ${response.status}`)
        const buffer = await response.arrayBuffer()
        await Bun.write(outputPath, buffer)
        completed++
        const pct = ((completed / urls.length) * 100).toFixed(1)
        process.stdout.write(`\r[${pct}%] Downloaded ${filename}`.padEnd(60))
      } catch (err) {
        errors++
        console.error(`\nFailed: ${filename} - ${String(err)}`)
      }
    }),
  )

  await Promise.all(tasks)
  console.log(`\nComplete. Downloaded: ${completed}, Errors: ${errors}`)
}

// ─────────────────────────────────────────────────────────────────────────────
// Gazetteer Command
// ─────────────────────────────────────────────────────────────────────────────

async function downloadGazetteer(outputPath: string) {
  console.log("Downloading ZCTA gazetteer...")

  const response = await fetch(GAZETTEER_URL)
  if (!response.ok) throw new Error(`HTTP ${response.status}`)

  const zipBuffer = Buffer.from(await response.arrayBuffer())
  const zipfile = await yauzl.fromBuffer(zipBuffer)

  try {
    for await (const entry of zipfile) {
      if (entry.filename.endsWith(".txt")) {
        const readStream = await zipfile.openReadStream(entry)
        const chunks: Buffer[] = []
        for await (const chunk of readStream) chunks.push(chunk)

        await fs.mkdir(path.dirname(outputPath), { recursive: true })
        await Bun.write(outputPath, Buffer.concat(chunks))
        console.log(`Extracted to ${outputPath}`)
        return
      }
    }
  } finally {
    await zipfile.close()
  }

  throw new Error("No .txt file found in gazetteer zip")
}

// ─────────────────────────────────────────────────────────────────────────────
// Build Command
// ─────────────────────────────────────────────────────────────────────────────

interface AddrfeatRecord {
  fullname: string
  zipL: string | null
  zipR: string | null
  stateFips: string
  countyFips: string
}

interface ZipCentroid {
  zip: string
  lat: number
  lon: number
}

async function extractAddrfeatRecords(
  zipPath: string,
  tempDir: string,
): Promise<AddrfeatRecord[]> {
  const records: AddrfeatRecord[] = []
  const filename = path.basename(zipPath)

  const fipsMatch = filename.match(/tl_2024_(\d{2})(\d{3})_addrfeat\.zip/)
  if (!fipsMatch?.[1] || !fipsMatch[2]) return records

  const stateFips = fipsMatch[1]
  const countyFips = fipsMatch[2]

  try {
    const zipBuffer = Buffer.from(await Bun.file(zipPath).arrayBuffer())
    const zipfile = await yauzl.fromBuffer(zipBuffer)

    try {
      let dbfEntry: yauzl.Entry | null = null
      for await (const entry of zipfile) {
        if (entry.filename.endsWith(".dbf")) {
          dbfEntry = entry
          break
        }
      }
      if (!dbfEntry) return records

      await fs.mkdir(tempDir, { recursive: true })
      const tempPath = path.join(
        tempDir,
        `dbf-${Date.now()}-${Math.random().toString(36).slice(2)}.dbf`,
      )

      const readStream = await zipfile.openReadStream(dbfEntry)
      const chunks: Buffer[] = []
      for await (const chunk of readStream) chunks.push(chunk)
      await Bun.write(tempPath, Buffer.concat(chunks))

      try {
        const dbf = await DBFFile.open(tempPath)
        let batch: Array<Record<string, unknown>>

        while ((batch = await dbf.readRecords(1000)).length > 0) {
          for (const record of batch) {
            const fullname = record.FULLNAME as string | undefined
            if (!fullname) continue

            const zipL = (record.ZIPL as string | undefined)?.trim() || null
            const zipR = (record.ZIPR as string | undefined)?.trim() || null
            if (!zipL && !zipR) continue

            records.push({
              fullname: fullname.trim(),
              zipL,
              zipR,
              stateFips,
              countyFips,
            })
          }
        }
      } finally {
        await fs.unlink(tempPath).catch(() => {})
      }
    } finally {
      await zipfile.close()
    }
  } catch {
    // Extraction failed
  }

  return records
}

function parseGazetteer(content: string): ZipCentroid[] {
  const centroids: ZipCentroid[] = []
  const lines = content.split("\n")

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i]?.trim()
    if (!line) continue

    const parts = line.split("\t")
    const zip = parts[0]?.trim()
    const lat = parseFloat(parts[5] ?? "")
    const lon = parseFloat(parts[6] ?? "")

    if (zip && !isNaN(lat) && !isNaN(lon)) {
      centroids.push({ zip, lat, lon })
    }
  }

  return centroids
}

async function buildDatabase(
  inputDir: string,
  gazetteerPath: string,
  outputPath: string,
  concurrency: number,
) {
  const startTime = Date.now()

  // Find all ADDRFEAT zips
  const glob = new Glob("tl_2024_*_addrfeat.zip")
  const files: string[] = []
  for await (const file of glob.scan(inputDir)) {
    files.push(path.join(inputDir, file))
  }
  console.log(`Found ${files.length} ADDRFEAT files`)

  // Create database
  console.log(`Creating database at ${outputPath}...`)
  await fs.mkdir(path.dirname(outputPath), { recursive: true })
  for (const suffix of ["", "-wal", "-shm"]) {
    await fs.unlink(outputPath + suffix).catch(() => {})
  }

  const db = new Database(outputPath)
  db.run("PRAGMA journal_mode = WAL")
  db.run("PRAGMA synchronous = NORMAL")

  db.run(`
    CREATE TABLE IF NOT EXISTS streets (
      id INTEGER PRIMARY KEY,
      state_fips TEXT NOT NULL,
      county_fips TEXT NOT NULL,
      fullname TEXT NOT NULL,
      metaphone_primary TEXT NOT NULL,
      metaphone_secondary TEXT
    )
  `)
  db.run(`
    CREATE TABLE IF NOT EXISTS street_zips (
      street_id INTEGER NOT NULL,
      zip TEXT NOT NULL,
      FOREIGN KEY (street_id) REFERENCES streets(id)
    )
  `)
  db.run(`
    CREATE TABLE IF NOT EXISTS zip_centroids (
      zip TEXT PRIMARY KEY,
      lat REAL NOT NULL,
      lon REAL NOT NULL
    )
  `)

  // Load gazetteer
  console.log("Loading ZIP centroids...")
  const gazContent = await Bun.file(gazetteerPath).text()
  const centroids = parseGazetteer(gazContent)

  const insertCentroid = db.prepare(
    "INSERT OR REPLACE INTO zip_centroids (zip, lat, lon) VALUES (?, ?, ?)",
  )
  const centroidTx = db.transaction(() => {
    for (const { zip, lat, lon } of centroids) {
      insertCentroid.run(zip, lat, lon)
    }
  })
  centroidTx()
  console.log(`Inserted ${centroids.length} ZIP centroids`)

  // Process ADDRFEAT files
  const tempDir = path.join(path.dirname(outputPath), "build-temp")
  const limit = pLimit(concurrency)
  let processed = 0
  let totalStreets = 0
  let totalZips = 0

  const insertStreet = db.prepare(`
    INSERT INTO streets (state_fips, county_fips, fullname, metaphone_primary, metaphone_secondary)
    VALUES (?, ?, ?, ?, ?)
  `)
  const insertZip = db.prepare(
    "INSERT INTO street_zips (street_id, zip) VALUES (?, ?)",
  )

  console.log(
    `Processing ${files.length} files with concurrency ${concurrency}...`,
  )

  const tasks = files.map((file) =>
    limit(async () => {
      const records = await extractAddrfeatRecords(file, tempDir)

      if (records.length > 0) {
        // Deduplicate within batch
        const seen = new Map<
          string,
          { record: AddrfeatRecord; zips: Set<string> }
        >()
        for (const record of records) {
          const key = `${record.stateFips}|${record.countyFips}|${record.fullname}`
          const existing = seen.get(key)
          if (existing) {
            if (record.zipL) existing.zips.add(record.zipL)
            if (record.zipR) existing.zips.add(record.zipR)
          } else {
            const zips = new Set<string>()
            if (record.zipL) zips.add(record.zipL)
            if (record.zipR) zips.add(record.zipR)
            seen.set(key, { record, zips })
          }
        }

        const tx = db.transaction(() => {
          for (const { record, zips } of seen.values()) {
            const [primary, secondary] = doubleMetaphone(record.fullname)
            insertStreet.run(
              record.stateFips,
              record.countyFips,
              record.fullname,
              primary,
              secondary ?? null,
            )
            const streetId = db
              .query("SELECT last_insert_rowid() as id")
              .get() as { id: number }
            for (const zip of zips) {
              insertZip.run(streetId.id, zip)
              totalZips++
            }
            totalStreets++
          }
        })
        tx()
      }

      processed++
      const pct = ((processed / files.length) * 100).toFixed(1)
      process.stdout.write(
        `\r[${pct}%] Processed ${path.basename(file)}`.padEnd(60),
      )
    }),
  )

  await Promise.all(tasks)
  console.log("\n")

  // Create indexes
  console.log("Creating indexes...")
  db.run(
    "CREATE INDEX IF NOT EXISTS idx_streets_metaphone_primary ON streets(metaphone_primary)",
  )
  db.run(
    "CREATE INDEX IF NOT EXISTS idx_streets_metaphone_secondary ON streets(metaphone_secondary)",
  )
  db.run("CREATE INDEX IF NOT EXISTS idx_street_zips_zip ON street_zips(zip)")
  db.run(
    "CREATE INDEX IF NOT EXISTS idx_street_zips_street_id ON street_zips(street_id)",
  )

  // Optimize
  console.log("Optimizing...")
  db.run("PRAGMA optimize")
  db.run("VACUUM")

  db.close()

  // Cleanup
  await fs.rm(tempDir, { recursive: true }).catch(() => {})

  const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1)
  console.log(`\nBuild complete in ${elapsed} minutes`)
  console.log(`  Streets: ${totalStreets}`)
  console.log(`  Street-ZIP links: ${totalZips}`)
  console.log(`  ZIP centroids: ${centroids.length}`)
}

// ─────────────────────────────────────────────────────────────────────────────
// CLI Setup
// ─────────────────────────────────────────────────────────────────────────────

program
  .name("phonetic-tiger")
  .description("CLI tool for building the nationwide address database")
  .version(packageJson.version)

program
  .command("urls")
  .description("Generate list of TIGER ADDRFEAT URLs")
  .option("-o, --output <file>", "Output file (use - for stdout)", "urls.txt")
  .action(async (opts) => {
    await generateUrls(opts.output)
  })

program
  .command("download")
  .description("Download ADDRFEAT files from URL list")
  .option("-i, --input <file>", "URL list file", "./urls.txt")
  .option("-o, --output <dir>", "Output directory", "./addrfeat")
  .option("-c, --concurrency <n>", "Download concurrency", "10")
  .action(async (opts) => {
    await downloadFiles(opts.input, opts.output, parseInt(opts.concurrency))
  })

program
  .command("gazetteer")
  .description("Download ZCTA gazetteer for ZIP centroids")
  .option("-o, --output <file>", "Output file", "./gazetteer.txt")
  .action(async (opts) => {
    await downloadGazetteer(opts.output)
  })

program
  .command("build")
  .description("Build SQLite database from downloaded files")
  .option(
    "-i, --input <dir>",
    "Directory containing ADDRFEAT zips",
    "./addrfeat",
  )
  .option("-g, --gazetteer <file>", "Gazetteer file path", "./gazetteer.txt")
  .option("-o, --output <file>", "Output database path", "./addresses.db")
  .option("-c, --concurrency <n>", "Processing concurrency", "5")
  .action(async (opts) => {
    await buildDatabase(
      opts.input,
      opts.gazetteer,
      opts.output,
      parseInt(opts.concurrency),
    )
  })

program.parse()
