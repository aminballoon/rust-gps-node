use super::GpsTelemetry;
use chrono::Utc;

pub fn calc_crc32(data: &[u8]) -> u32 {
    let mut crc = 0u32;
    for &b in data {
        let mut temp = (crc ^ (b as u32)) & 0xFF;
        for _ in 0..8 {
            if (temp & 1) != 0 {
                temp = (temp >> 1) ^ 0xEDB88320;
            } else {
                temp >>= 1;
            }
        }
        crc = (crc >> 8) ^ temp;
    }
    crc
}

pub fn verify_unicore_checksum(line: &str) -> bool {
    let line = line.trim();
    if !line.starts_with('#') {
        return false;
    }
    
    let parts: Vec<&str> = line[1..].split('*').collect();
    if parts.len() != 2 {
        return false;
    }
    
    let payload = parts[0];
    let checksum_str = parts[1];
    
    let expected_checksum = match u32::from_str_radix(checksum_str, 16) {
        Ok(val) => val,
        Err(_) => return false,
    };
    
    let calculated_checksum = calc_crc32(payload.as_bytes());
    calculated_checksum == expected_checksum
}

pub fn update_from_unicore(line: &str, telemetry: &mut GpsTelemetry) -> bool {
    if !verify_unicore_checksum(line) {
        return false;
    }
    
    let clean = line.trim().trim_start_matches('#');
    let star_idx = clean.find('*').unwrap_or(clean.len());
    let payload = &clean[..star_idx];
    
    let parts: Vec<&str> = payload.split(';').collect();
    if parts.len() != 2 {
        return false;
    }
    
    let header_fields: Vec<&str> = parts[0].split(',').collect();
    let body_fields: Vec<&str> = parts[1].split(',').collect();
    
    if header_fields.is_empty() {
        return false;
    }
    
    let msg_name = header_fields[0];
    
    if msg_name == "BESTPOSA" {
        if body_fields.len() < 15 {
            return false;
        }
        telemetry.timestamp = Utc::now().to_rfc3339();
        
        let sol_status = body_fields[0];
        let pos_type = body_fields[1];
        
        if sol_status == "SOL_COMPUTED" {
            telemetry.latitude = body_fields[2].parse().ok();
            telemetry.longitude = body_fields[3].parse().ok();
            telemetry.altitude = body_fields[4].parse().ok();
            
            telemetry.fix_type = match pos_type {
                "NARROW_INT" | "WIDE_INT" => "RTK_FIXED".to_string(),
                "NARROW_FLOAT" | "WIDE_FLOAT" => "RTK_FLOAT".to_string(),
                "PSRDIFF" => "DGPS".to_string(),
                "SINGLE" => "3D_FIX".to_string(),
                _ => "NO_FIX".to_string(),
            };
        } else {
            telemetry.fix_type = "NO_FIX".to_string();
        }
        
        telemetry.satellites = body_fields[14].parse().unwrap_or(0);
        log::info!("Parsed Unicore BESTPOSA: {}", line.trim());
        return true;
    } else if msg_name == "HEADINGA" {
        if body_fields.len() < 5 {
            return false;
        }
        let sol_status = body_fields[0];
        if sol_status == "SOL_COMPUTED" {
            telemetry.heading = body_fields[3].parse().ok();
        }
        log::info!("Parsed Unicore HEADINGA: {}", line.trim());
        return true;
    }
    
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unicore_crc() {
        // Test CRC calculation matching
        let test_payload = "BESTPOSA,COM1,0,72.5,FINESTEERING,2215,232100.00,00000000,a000,18;SOL_COMPUTED,NARROW_INT,13.75630248,100.50182471,15.2415,-18.2321,WGS84,0.0120,0.0120,0.0245,\"\",0.0,0.0,24,24,24,24,0,0,0,15";
        let crc = calc_crc32(test_payload.as_bytes());
        let line = format!("#{}*{:08x}\r\n", test_payload, crc);
        assert!(verify_unicore_checksum(&line));
    }
    
    #[test]
    fn test_parse_bestposa() {
        let test_payload = "BESTPOSA,COM1,0,72.5,FINESTEERING,2215,232100.00,00000000,a000,18;SOL_COMPUTED,NARROW_INT,13.75630248,100.50182471,15.2415,-18.2321,WGS84,0.0120,0.0120,0.0245,\"\",0.0,0.0,24,24,24,24,0,0,0,15";
        let crc = calc_crc32(test_payload.as_bytes());
        let line = format!("#{}*{:08x}\r\n", test_payload, crc);
        
        let mut tel = GpsTelemetry::default();
        let success = update_from_unicore(&line, &mut tel);
        
        assert!(success);
        assert_eq!(tel.fix_type, "RTK_FIXED");
        assert_eq!(tel.satellites, 24);
        assert_eq!(tel.latitude, Some(13.75630248));
        assert_eq!(tel.longitude, Some(100.50182471));
        assert_eq!(tel.altitude, Some(15.2415));
    }
}
