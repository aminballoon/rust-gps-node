use super::GpsTelemetry;
use chrono::Utc;

pub fn verify_checksum(sentence: &str) -> bool {
    let sentence = sentence.trim();
    if !sentence.starts_with('$') {
        return false;
    }
    let parts: Vec<&str> = sentence[1..].split('*').collect();
    if parts.len() != 2 {
        return false;
    }
    let payload = parts[0];
    let checksum_str = parts[1];
    
    let expected_checksum = match u8::from_str_radix(checksum_str, 16) {
        Ok(val) => val,
        Err(_) => return false,
    };
    
    let calculated_checksum = payload.as_bytes().iter().fold(0, |acc, &b| acc ^ b);
    calculated_checksum == expected_checksum
}

fn parse_latitude(lat_str: &str, ns: &str) -> Option<f64> {
    if lat_str.len() < 4 {
        return None;
    }
    let deg_str = &lat_str[..2];
    let min_str = &lat_str[2..];
    let deg: f64 = deg_str.parse().ok()?;
    let min: f64 = min_str.parse().ok()?;
    let mut lat = deg + (min / 60.0);
    if ns == "S" {
        lat = -lat;
    }
    Some(lat)
}

fn parse_longitude(lon_str: &str, ew: &str) -> Option<f64> {
    if lon_str.len() < 5 {
        return None;
    }
    let deg_str = &lon_str[..3];
    let min_str = &lon_str[3..];
    let deg: f64 = deg_str.parse().ok()?;
    let min: f64 = min_str.parse().ok()?;
    let mut lon = deg + (min / 60.0);
    if ew == "W" {
        lon = -lon;
    }
    Some(lon)
}

pub fn update_from_nmea(sentence: &str, telemetry: &mut GpsTelemetry) -> bool {
    if !verify_checksum(sentence) {
        return false;
    }
    
    let clean = sentence.trim().trim_start_matches('$');
    let star_idx = clean.find('*').unwrap_or(clean.len());
    let payload = &clean[..star_idx];
    let fields: Vec<&str> = payload.split(',').collect();
    
    if fields.is_empty() {
        return false;
    }
    
    let msg_type = fields[0];
    // Check type suffix (e.g. GNGGA, GPGGA, etc.)
    if msg_type.ends_with("GGA") {
        if fields.len() < 10 {
            return false;
        }
        telemetry.timestamp = Utc::now().to_rfc3339();
        
        if !fields[2].is_empty() && !fields[3].is_empty() {
            telemetry.latitude = parse_latitude(fields[2], fields[3]);
        }
        if !fields[4].is_empty() && !fields[5].is_empty() {
            telemetry.longitude = parse_longitude(fields[4], fields[5]);
        }
        
        let quality = fields[6].parse::<u32>().unwrap_or(0);
        telemetry.fix_type = match quality {
            1 => "GPS_SPS".to_string(),
            2 => "DGPS".to_string(),
            4 => "RTK_FIXED".to_string(),
            5 => "RTK_FLOAT".to_string(),
            _ => "NO_FIX".to_string(),
        };
        
        telemetry.satellites = fields[7].parse::<u32>().unwrap_or(0);
        
        if !fields[9].is_empty() {
            telemetry.altitude = fields[9].parse::<f64>().ok();
        }
        log::debug!("Parsed NMEA GGA: {}", sentence.trim());
        return true;
    } else if msg_type.ends_with("RMC") {
        if fields.len() < 9 {
            return false;
        }
        telemetry.timestamp = Utc::now().to_rfc3339();
        
        let status = fields[2];
        if status == "A" {
            if !fields[3].is_empty() && !fields[4].is_empty() {
                telemetry.latitude = parse_latitude(fields[3], fields[4]);
            }
            if !fields[5].is_empty() && !fields[6].is_empty() {
                telemetry.longitude = parse_longitude(fields[5], fields[6]);
            }
            if !fields[7].is_empty() {
                // knots to km/h
                let knots: f64 = fields[7].parse().unwrap_or(0.0);
                telemetry.speed_kmh = Some(knots * 1.852);
            }
            if !fields[8].is_empty() {
                telemetry.heading = fields[8].parse().ok();
            }
        }
        log::debug!("Parsed NMEA RMC: {}", sentence.trim());
        return true;
    } else if msg_type.ends_with("HDT") {
        if fields.len() < 2 {
            return false;
        }
        if !fields[1].is_empty() {
            telemetry.heading = fields[1].parse().ok();
        }
        log::debug!("Parsed NMEA HDT: {}", sentence.trim());
        return true;
    }
    
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum() {
        assert!(verify_checksum("$GNGGA,001043.00,4404.14036,N,12118.85961,W,1,12,0.98,1113.0,M,-21.3,M,,*47"));
        assert!(!verify_checksum("$GNGGA,001043.00,4404.14036,N,12118.85961,W,1,12,0.98,1113.0,M,-21.3,M,,*48"));
    }

    #[test]
    fn test_parse_gga() {
        let mut tel = GpsTelemetry::default();
        let success = update_from_nmea(
            "$GNGGA,001043.00,1345.3782,N,10030.1092,E,4,24,0.98,15.2,M,-21.3,M,,*58",
            &mut tel
        );
        assert!(success);
        assert_eq!(tel.fix_type, "RTK_FIXED");
        assert_eq!(tel.satellites, 24);
        assert!(tel.latitude.unwrap() - 13.756303 < 0.0001);
        assert!(tel.longitude.unwrap() - 100.50182 < 0.0001);
        assert_eq!(tel.altitude, Some(15.2));
    }
}
