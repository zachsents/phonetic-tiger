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
// @ts-expect-error -- no types available for parse-address
import { parseLocation } from "parse-address"
import pLimit from "p-limit"
import { bufferCount, from, lastValueFrom, mergeMap, tap } from "rxjs"
import * as yauzl from "yauzl-promise"
import packageJson from "../package.json"

/** Builds TIGER ADDRFEAT URL for a given year. */
function getTigerUrl(year: number) {
  return `https://www2.census.gov/geo/tiger/TIGER${year}/ADDRFEAT`
}

/** Builds Gazetteer URL for a given year. */
function getGazetteerUrl(year: number) {
  return `https://www2.census.gov/geo/docs/maps-data/data/gazetteer/${year}_Gazetteer/${year}_Gaz_zcta_national.zip`
}

/** GeoNames US ZIP code data URL (includes city, state, coordinates). */
const GEONAMES_ZIP_URL = "https://download.geonames.org/export/zip/US.zip"

/**
 * Auto-resolves the latest available TIGER year by checking URLs starting from
 * the current year and working backwards.
 */
async function resolveYear(): Promise<number> {
  const currentYear = new Date().getFullYear()

  for (let year = currentYear; year >= 2020; year--) {
    const url = getTigerUrl(year)
    try {
      const response = await fetch(url, { method: "HEAD" })
      if (response.ok) {
        console.log(`Resolved TIGER year: ${year}`)
        return year
      }
    } catch {
      // Try next year
    }
  }

  throw new Error("Could not find any valid TIGER dataset (checked 2020-now)")
}

// ─────────────────────────────────────────────────────────────────────────────
// URL List Command
// ─────────────────────────────────────────────────────────────────────────────

async function generateUrls(output: string, year: number) {
  const tigerUrl = getTigerUrl(year)
  console.log(`Fetching ADDRFEAT file list from Census Bureau (${year})...`)

  const response = await fetch(tigerUrl)
  const html = await response.text()
  const pattern = new RegExp(`tl_${year}_\\d+_addrfeat\\.zip`, "g")
  const matches = html.match(pattern) ?? []
  const unique = [...new Set(matches)]
  const urls = unique.map((f) => `${tigerUrl}/${f}`)

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

async function checkUnzipInstalled() {
  const result = await Bun.$`which unzip`.quiet().nothrow()
  if (result.exitCode !== 0) {
    throw new Error("unzip is required but not installed. Please install it.")
  }
}

async function downloadFiles(
  urlFile: string,
  outputDir: string,
  concurrency: number,
) {
  await checkUnzipInstalled()

  const content = await Bun.file(urlFile).text()
  const urls = content.trim().split("\n").filter(Boolean)

  console.log(`Downloading ${urls.length} ADDRFEAT files...`)
  await fs.mkdir(outputDir, { recursive: true })

  const limit = pLimit(concurrency)
  let completed = 0
  let skipped = 0
  let errors = 0

  const tasks = urls.map((url) =>
    limit(async () => {
      const filename = path.basename(url)
      const outputPath = path.join(outputDir, filename)

      // Skip if already exists
      try {
        await fs.access(outputPath)
        skipped++
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
  console.log(
    `\nComplete. Downloaded: ${completed - skipped}, Skipped: ${skipped}, Errors: ${errors}`,
  )

  // Extract DBF files (skip if DBF already exists)
  console.log("\nExtracting DBF files (skipping existing)...")

  const dbfDir = path.join(outputDir, "dbf")
  await fs.mkdir(dbfDir, { recursive: true })

  const zipGlob = new Glob("*_addrfeat.zip")
  const zipsToExtract: string[] = []

  for await (const zipFile of zipGlob.scan(outputDir)) {
    const dbfName = zipFile.replace(".zip", ".dbf")
    const dbfPath = path.join(dbfDir, dbfName)

    try {
      await fs.access(dbfPath)
      // DBF exists, skip
    } catch {
      // DBF doesn't exist, need to extract
      zipsToExtract.push(path.join(outputDir, zipFile))
    }
  }

  if (zipsToExtract.length === 0) {
    console.log("  All DBFs already extracted")
  } else {
    console.log(`  Extracting ${zipsToExtract.length} files...`)

    const extractLimit = pLimit(concurrency)
    await Promise.all(
      zipsToExtract.map((zipPath) =>
        extractLimit(async () => {
          await Bun.$`unzip -jo ${zipPath} "*.dbf" -d ${dbfDir}`
            .quiet()
            .nothrow()
        }),
      ),
    )
  }

  const dbfCount = await Bun.$`ls ${dbfDir}/*.dbf 2>/dev/null | wc -l`
    .quiet()
    .text()

  console.log(`Extracted ${dbfCount.trim()} DBFs to ${dbfDir}`)
}

// ─────────────────────────────────────────────────────────────────────────────
// Gazetteer Command
// ─────────────────────────────────────────────────────────────────────────────

async function downloadGazetteer(outputPath: string, year: number) {
  const gazetteerUrl = getGazetteerUrl(year)
  console.log(`Downloading ZCTA gazetteer (${year})...`)

  const response = await fetch(gazetteerUrl)
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

interface ZipInfo {
  zip: string
  city: string
  state: string
  lat: number
  lon: number
}

/** Processed street ready for database insertion. */
interface ProcessedStreet {
  stateFips: string
  countyFips: string
  prefix: string | null
  name: string
  type: string | null
  suffix: string | null
  metaphonePrimary: string
  metaphoneSecondary: string | null
  zips: string[]
}

/** Result from parse-address for a street name. */
interface ParsedStreet {
  prefix?: string
  street?: string
  type?: string
  suffix?: string
}

/**
 * Processes a single ADDRFEAT DBF file, parsing FULLNAME with parse-address to
 * get structured name fields.
 */
async function processAddrfeatFile(
  dbfPath: string,
): Promise<ProcessedStreet[]> {
  const filename = path.basename(dbfPath)

  const fipsMatch = filename.match(/tl_\d{4}_(\d{2})(\d{3})_addrfeat\.dbf/)
  if (!fipsMatch?.[1] || !fipsMatch[2]) return []

  const stateFips = fipsMatch[1]
  const countyFips = fipsMatch[2]

  // Map: unique key -> { parsed, zips }
  const streetMap = new Map<
    string,
    { parsed: ParsedStreet; fullname: string; zips: Set<string> }
  >()

  try {
    const dbf = await DBFFile.open(dbfPath)
    let batch: Array<Record<string, unknown>>

    while ((batch = await dbf.readRecords(1000)).length > 0) {
      for (const record of batch) {
        const fullname = (record.FULLNAME as string | undefined)?.trim()
        if (!fullname) continue

        const zipL = (record.ZIPL as string | undefined)?.trim() || null
        const zipR = (record.ZIPR as string | undefined)?.trim() || null
        if (!zipL && !zipR) continue

        // Parse the fullname to get structured components
        const parsed = (parseLocation(fullname) as ParsedStreet | null) ?? {}

        // Create unique key based on parsed components
        const key = `${stateFips}|${countyFips}|${parsed.prefix ?? ""}|${parsed.street ?? fullname}|${parsed.type ?? ""}|${parsed.suffix ?? ""}`
        const existing = streetMap.get(key)
        if (existing) {
          if (zipL) existing.zips.add(zipL)
          if (zipR) existing.zips.add(zipR)
        } else {
          const zips = new Set<string>()
          if (zipL) zips.add(zipL)
          if (zipR) zips.add(zipR)
          streetMap.set(key, { parsed, fullname, zips })
        }
      }
    }
  } catch {
    return []
  }

  // Convert to processed streets with metaphone on NAME only
  const results: ProcessedStreet[] = []
  for (const { parsed, fullname, zips } of streetMap.values()) {
    const streetName = parsed.street ?? fullname

    // Compute metaphone on just the street name
    const [primary, secondary] = doubleMetaphone(streetName)

    results.push({
      stateFips,
      countyFips,
      prefix: parsed.prefix ?? null,
      name: streetName,
      type: parsed.type ?? null,
      suffix: parsed.suffix ?? null,
      metaphonePrimary: primary,
      metaphoneSecondary: secondary ?? null,
      zips: [...zips],
    })
  }

  return results
}

/**
 * Parses GeoNames US.txt format (tab-separated). Format: country, zip, city,
 * state_name, state_abbrev, ..., lat, lon, accuracy
 */
function parseGeoNamesZips(content: string): ZipInfo[] {
  const zips: ZipInfo[] = []
  const lines = content.split("\n")

  for (const line of lines) {
    if (!line.trim()) continue

    const parts = line.split("\t")
    // GeoNames columns: 0=country, 1=zip, 2=city, 3=state_name, 4=state_abbrev,
    // 5-8=admin2/3, 9=lat, 10=lon, 11=accuracy
    const zip = parts[1]?.trim()
    const city = parts[2]?.trim()
    const state = parts[4]?.trim() // 2-letter abbreviation
    const lat = parseFloat(parts[9] ?? "")
    const lon = parseFloat(parts[10] ?? "")

    if (zip && city && state && !isNaN(lat) && !isNaN(lon)) {
      zips.push({ zip, city, state, lat, lon })
    }
  }

  return zips
}

/** Opens (or creates) the database and ensures all tables exist. */
function openDb(outputPath: string): Database {
  const db = new Database(outputPath)
  db.run("PRAGMA journal_mode = WAL")
  db.run("PRAGMA synchronous = NORMAL")

  db.run(`
    CREATE TABLE IF NOT EXISTS streets (
      id INTEGER PRIMARY KEY,
      state_fips TEXT NOT NULL,
      county_fips TEXT NOT NULL,
      prefix TEXT,
      name TEXT NOT NULL,
      type TEXT,
      suffix TEXT,
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
    CREATE TABLE IF NOT EXISTS zips (
      zip TEXT PRIMARY KEY,
      city TEXT NOT NULL,
      state TEXT NOT NULL,
      lat REAL,
      lon REAL
    )
  `)

  return db
}

/** Downloads and extracts GeoNames US ZIP code data. */
async function downloadGeoNamesZips(outputPath: string) {
  console.log("Downloading GeoNames US ZIP data...")

  const response = await fetch(GEONAMES_ZIP_URL)
  if (!response.ok) throw new Error(`HTTP ${response.status}`)

  const zipBuffer = Buffer.from(await response.arrayBuffer())
  const zipfile = await yauzl.fromBuffer(zipBuffer)

  try {
    for await (const entry of zipfile) {
      if (entry.filename === "US.txt") {
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

  throw new Error("No US.txt file found in GeoNames zip")
}

/** Clears and rebuilds the zips table from GeoNames data. */
async function buildZips(db: Database, geonamesPath: string) {
  console.log("Loading ZIP codes with city/state...")
  const content = await Bun.file(geonamesPath).text()
  const zips = parseGeoNamesZips(content)

  db.run("DELETE FROM zips")

  const insertZip = db.prepare(
    "INSERT OR REPLACE INTO zips (zip, city, state, lat, lon) VALUES (?, ?, ?, ?, ?)",
  )

  const tx = db.transaction(() => {
    for (const { zip, city, state, lat, lon } of zips) {
      insertZip.run(zip, city, state, lat, lon)
    }
  })
  tx()
  console.log(`Inserted ${zips.length} ZIP codes with city/state`)
  return zips.length
}

/** Clears and rebuilds the streets/street_zips tables from ADDRFEAT files. */
async function buildStreets(
  db: Database,
  inputDir: string,
  readConcurrency: number,
  writeBatchSize: number,
) {
  // Find all extracted ADDRFEAT DBF files
  const dbfDir = path.join(inputDir, "dbf")
  const glob = new Glob("tl_*_addrfeat.dbf")
  const files: string[] = []
  for await (const file of glob.scan(dbfDir)) {
    files.push(path.join(dbfDir, file))
  }
  console.log(`Found ${files.length} ADDRFEAT DBF files`)

  // Clear existing data
  console.log("Clearing existing street data...")
  db.run("DELETE FROM street_zips")
  db.run("DELETE FROM streets")

  // Drop indexes for faster inserts
  db.run("DROP INDEX IF EXISTS idx_streets_metaphone_primary")
  db.run("DROP INDEX IF EXISTS idx_streets_metaphone_secondary")
  db.run("DROP INDEX IF EXISTS idx_street_zips_zip")
  db.run("DROP INDEX IF EXISTS idx_street_zips_street_id")

  const insertStreet = db.prepare(`
    INSERT INTO streets (state_fips, county_fips, prefix, name, type, suffix, metaphone_primary, metaphone_secondary)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `)
  const insertZip = db.prepare(
    "INSERT INTO street_zips (street_id, zip) VALUES (?, ?)",
  )

  let filesProcessed = 0
  let totalStreets = 0
  let totalZips = 0

  const writeBatch = (batch: ProcessedStreet[]) => {
    const tx = db.transaction(() => {
      for (const street of batch) {
        insertStreet.run(
          street.stateFips,
          street.countyFips,
          street.prefix,
          street.name,
          street.type,
          street.suffix,
          street.metaphonePrimary,
          street.metaphoneSecondary,
        )
        const streetId = db.query("SELECT last_insert_rowid() as id").get() as {
          id: number
        }
        for (const zip of street.zips) {
          insertZip.run(streetId.id, zip)
          totalZips++
        }
        totalStreets++
      }
    })
    tx()
  }

  console.log(
    `Processing ADDRFEAT files (read concurrency: ${readConcurrency}, write batch: ${writeBatchSize})...`,
  )

  await lastValueFrom(
    from(files).pipe(
      mergeMap(async (file) => {
        const streets = await processAddrfeatFile(file)
        filesProcessed++
        const pct = ((filesProcessed / files.length) * 100).toFixed(1)
        process.stdout.write(
          `\r[${pct}%] Read ${filesProcessed}/${files.length} files, ${totalStreets} streets`.padEnd(
            70,
          ),
        )
        return streets
      }, readConcurrency),
      mergeMap((streets) => from(streets)),
      bufferCount(writeBatchSize),
      tap((batch) => writeBatch(batch)),
    ),
    { defaultValue: undefined },
  )

  console.log("\n")

  // Recreate indexes
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

  return { totalStreets, totalZips }
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
  .option(
    "-y, --year <year>",
    "TIGER dataset year (or 'auto' to resolve)",
    "auto",
  )
  .action(async (opts) => {
    const year =
      opts.year === "auto" ? await resolveYear() : parseInt(opts.year)
    await generateUrls(opts.output, year)
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
  .description("[DEPRECATED] Download ZCTA gazetteer (use 'geonames' instead)")
  .option("-o, --output <file>", "Output file", "./gazetteer.txt")
  .option("-y, --year <year>", "Gazetteer year (or 'auto' to resolve)", "auto")
  .action(async (opts) => {
    console.warn(
      "Warning: 'gazetteer' is deprecated. Use 'geonames' command for city/state data.",
    )
    const year =
      opts.year === "auto" ? await resolveYear() : parseInt(opts.year)
    await downloadGazetteer(opts.output, year)
  })

program
  .command("geonames")
  .description("Download GeoNames US ZIP data (includes city, state, coords)")
  .option("-o, --output <file>", "Output file", "./geonames-us.txt")
  .action(async (opts) => {
    await downloadGeoNamesZips(opts.output)
  })

const build = program
  .command("build")
  .description("Build SQLite database (run without subcommand for full build)")
  .option("-o, --output <file>", "Output database path", "./addresses.db")

build
  .command("zips")
  .description("Rebuild only ZIP codes from GeoNames data")
  .option("-g, --geonames <file>", "GeoNames file path", "./geonames-us.txt")
  .action(async (opts, cmd) => {
    const parentOpts = cmd.parent?.opts() ?? {}
    const outputPath = parentOpts.output ?? "./addresses.db"

    const startTime = Date.now()
    console.log(`Opening database at ${outputPath}...`)
    await fs.mkdir(path.dirname(outputPath), { recursive: true })
    const db = openDb(outputPath)

    const count = await buildZips(db, opts.geonames)

    db.run("PRAGMA optimize")
    db.close()

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
    console.log(`\nZIPs build complete in ${elapsed}s`)
    console.log(`  ZIP codes: ${count}`)
  })

build
  .command("streets")
  .description("Rebuild only streets from ADDRFEAT files")
  .option(
    "-i, --input <dir>",
    "Directory containing ADDRFEAT zips",
    "./addrfeat",
  )
  .option("-c, --concurrency <n>", "DBF read concurrency", "10")
  .option("-b, --batch-size <n>", "DB write batch size", "5000")
  .action(async (opts, cmd) => {
    const parentOpts = cmd.parent?.opts() ?? {}
    const outputPath = parentOpts.output ?? "./addresses.db"

    const startTime = Date.now()
    console.log(`Opening database at ${outputPath}...`)
    await fs.mkdir(path.dirname(outputPath), { recursive: true })
    const db = openDb(outputPath)

    const { totalStreets, totalZips } = await buildStreets(
      db,
      opts.input,
      parseInt(opts.concurrency),
      parseInt(opts.batchSize),
    )

    console.log("Optimizing...")
    db.run("PRAGMA optimize")
    db.run("VACUUM")
    db.close()

    const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1)
    console.log(`\nStreets build complete in ${elapsed} minutes`)
    console.log(`  Streets: ${totalStreets}`)
    console.log(`  Street-ZIP links: ${totalZips}`)
  })

// Default action when no subcommand - full build
build
  .option("-g, --geonames <file>", "GeoNames file path", "./geonames-us.txt")
  .option(
    "-i, --input <dir>",
    "Directory containing ADDRFEAT zips",
    "./addrfeat",
  )
  .option("-c, --concurrency <n>", "DBF read concurrency", "10")
  .option("-b, --batch-size <n>", "DB write batch size", "5000")
  .action(async (opts) => {
    const startTime = Date.now()

    // Delete existing DB files for clean rebuild
    console.log(`Creating fresh database at ${opts.output}...`)
    await fs.mkdir(path.dirname(opts.output), { recursive: true })
    for (const suffix of ["", "-wal", "-shm"]) {
      await fs.unlink(opts.output + suffix).catch(() => {})
    }

    const db = openDb(opts.output)

    const zipCount = await buildZips(db, opts.geonames)
    const { totalStreets, totalZips } = await buildStreets(
      db,
      opts.input,
      parseInt(opts.concurrency),
      parseInt(opts.batchSize),
    )

    console.log("Optimizing...")
    db.run("PRAGMA optimize")
    db.run("VACUUM")
    db.close()

    const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1)
    console.log(`\nFull build complete in ${elapsed} minutes`)
    console.log(`  Streets: ${totalStreets}`)
    console.log(`  Street-ZIP links: ${totalZips}`)
    console.log(`  ZIP codes: ${zipCount}`)
  })

program.parse()
