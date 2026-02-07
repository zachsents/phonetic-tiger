# phonetic-tiger

Build a phonetic street index from US Census TIGER data for fuzzy address matching.

## Usage

```bash
# Generate URL list for all TIGER ADDRFEAT files
phonetic-tiger urls -o urls.txt

# Download ADDRFEAT files
phonetic-tiger download -i urls.txt -o ./addrfeat -c 10

# Download GeoNames ZIP data (includes city, state, coordinates)
phonetic-tiger geonames -o geonames-us.txt

# Build SQLite database
phonetic-tiger build -i ./addrfeat -g geonames-us.txt -o addresses.db
```

## Database Schema

The built database contains:

- **`streets`** - Street names with phonetic codes for fuzzy matching
- **`street_zips`** - Links streets to ZIP codes
- **`zips`** - ZIP codes with city, state, and centroid coordinates

```sql
CREATE TABLE zips (
  zip TEXT PRIMARY KEY,
  city TEXT NOT NULL,
  state TEXT NOT NULL,  -- 2-letter abbreviation
  lat REAL NOT NULL,
  lon REAL NOT NULL
);
```

## Install

```bash
bun install
bun run start -- --help
```

## Build executable

```bash
bun run build          # macOS
bun run build:linux    # Linux
```
