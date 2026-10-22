//! Geo commands — `GEOADD`/`GEOPOS`/`GEODIST`/`GEOSEARCH`, layered on
//! `RObj::ZSet` exactly like real Redis (`geo.c`): a member's longitude/
//! latitude is encoded into a single 52-bit interleaved geohash integer
//! (exactly representable as an `f64`, since that has 52 bits of mantissa),
//! stored as the ZSet score. Distance/search queries decode that geohash
//! back to an approximate lon/lat and compute a haversine distance — the
//! same two-step "encoding loses a little precision, decoding recovers an
//! approximation" design real Redis uses (documented there too: `GEOPOS`
//! doesn't return exactly what you `GEOADD`ed), not a shortcut unique to
//! this port.
//!
//! "남은 부분" pass addition. Scope: `GEOADD`, `GEOPOS`, `GEODIST`,
//! `GEOSEARCH`/`GEOSEARCHSTORE` (`FROMLONLAT`/`FROMMEMBER`, `BYRADIUS`/
//! `BYBOX`, `ASC`/`DESC`, `COUNT`, `WITHCOORD`/`WITHDIST`, `STOREDIST`). Not
//! included: the legacy `GEORADIUS[BYMEMBER]` family (`GEOSEARCH`/
//! `GEOSEARCHSTORE` are their modern replacements and cover the same
//! queries).
//!
//! `BYBOX`'s membership test is also a simplification: a flat-earth
//! lon/lat-delta-to-meters approximation at the search origin's latitude,
//! not real Redis's exact box-corner geometry — fine for typical
//! city-scale box sizes, increasingly inaccurate for enormous boxes near
//! the poles.

const GEO_STEP: u32 = 26;
const LON_MIN: f64 = -180.0;
const LON_MAX: f64 = 180.0;
// Real Redis's own latitude bound: the usable range of the Mercator-ish
// projection its geohash cells are laid out on, not Earth's actual poles.
const LAT_MIN: f64 = -85.051_128_78;
const LAT_MAX: f64 = 85.051_128_78;
pub const EARTH_RADIUS_M: f64 = 6_372_797.560_856;

fn spread_bits(v: u32) -> u64 {
    let mut v = v as u64;
    v = (v | (v << 16)) & 0x0000_FFFF_0000_FFFF;
    v = (v | (v << 8)) & 0x00FF_00FF_00FF_00FF;
    v = (v | (v << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
    v = (v | (v << 2)) & 0x3333_3333_3333_3333;
    (v | (v << 1)) & 0x5555_5555_5555_5555
}

fn squash_bits(v: u64) -> u32 {
    let mut v = v & 0x5555_5555_5555_5555;
    v = (v | (v >> 1)) & 0x3333_3333_3333_3333;
    v = (v | (v >> 2)) & 0x0F0F_0F0F_0F0F_0F0F;
    v = (v | (v >> 4)) & 0x00FF_00FF_00FF_00FF;
    v = (v | (v >> 8)) & 0x0000_FFFF_0000_FFFF;
    v = (v | (v >> 16)) & 0x0000_0000_FFFF_FFFF;
    v as u32
}

pub fn valid_coords(lon: f64, lat: f64) -> bool {
    (LON_MIN..=LON_MAX).contains(&lon) && (LAT_MIN..=LAT_MAX).contains(&lat)
}

/// `geohashEncode` — packs `(lon, lat)` into one 52-bit interleaved integer
/// (returned widened to `u64`, but only the low 52 bits are ever set).
pub fn encode(lon: f64, lat: f64) -> u64 {
    let scale = f64::from(1u32 << GEO_STEP);
    let lon_bits = (((lon - LON_MIN) / (LON_MAX - LON_MIN)) * scale) as u32;
    let lat_bits = (((lat - LAT_MIN) / (LAT_MAX - LAT_MIN)) * scale) as u32;
    spread_bits(lat_bits) | (spread_bits(lon_bits) << 1)
}

/// `geohashDecode` — the inverse, returning the *center* of the cell
/// `bits` names (not the exact original point — see this module's doc).
pub fn decode(bits: u64) -> (f64, f64) {
    let lat_bits = squash_bits(bits);
    let lon_bits = squash_bits(bits >> 1);
    let scale = f64::from(1u32 << GEO_STEP);
    let cell_center = |bits: u32, min: f64, max: f64| {
        let lo = min + (f64::from(bits) / scale) * (max - min);
        let hi = min + (f64::from(bits + 1) / scale) * (max - min);
        (lo + hi) / 2.0
    };
    (cell_center(lon_bits, LON_MIN, LON_MAX), cell_center(lat_bits, LAT_MIN, LAT_MAX))
}

/// Haversine great-circle distance in meters.
pub fn distance_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let (lat1r, lat2r) = (lat1.to_radians(), lat2.to_radians());
    let u = ((lat2r - lat1r) / 2.0).sin();
    let v = ((lon2 - lon1).to_radians() / 2.0).sin();
    2.0 * EARTH_RADIUS_M * (u * u + lat1r.cos() * lat2r.cos() * v * v).sqrt().asin()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trips_approximately() {
        // Pompidou-ish coordinates, same ballpark `geo.c`'s own doc
        // examples use — precision at step=26 is well under a meter.
        let (lon, lat) = (13.361389, 38.115556);
        let bits = encode(lon, lat);
        let (dlon, dlat) = decode(bits);
        assert!((dlon - lon).abs() < 0.0001, "lon drifted too far: {dlon} vs {lon}");
        assert!((dlat - lat).abs() < 0.0001, "lat drifted too far: {dlat} vs {lat}");
    }

    #[test]
    fn distance_between_known_cities_matches_known_approximate_value() {
        // Palermo <-> Catania, the exact pair `geo.c`'s own test suite
        // uses, real Redis documents ~166274.1516 meters between them.
        let d = distance_m(13.361389, 38.115556, 15.087269, 37.502669);
        assert!((d - 166_274.0).abs() < 2000.0, "got {d}");
    }

    #[test]
    fn same_point_has_zero_distance() {
        assert_eq!(distance_m(10.0, 20.0, 10.0, 20.0), 0.0);
    }

    #[test]
    fn valid_coords_rejects_out_of_range() {
        assert!(valid_coords(0.0, 0.0));
        assert!(!valid_coords(181.0, 0.0));
        assert!(!valid_coords(0.0, 86.0));
    }
}
