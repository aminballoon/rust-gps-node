use super::GpsTelemetry;
use chrono::Utc;

pub fn calc_fletcher(data: &[u8]) -> (u8, u8) {
    let mut ck_a = 0u8;
    let mut ck_b = 0u8;
    for &b in data {
        ck_a = ck_a.wrapping_add(b);
        ck_b = ck_b.wrapping_add(ck_a);
    }
    (ck_a, ck_b)
}

pub fn update_from_ubx(class: u8, id: u8, payload: &[u8], telemetry: &mut GpsTelemetry) -> bool {
    // UBX-NAV-PVT is Class 0x01, ID 0x07, payload is at least 92 bytes
    if class == 0x01 && id == 0x07 {
        if payload.len() < 92 {
            return false;
        }
        
        telemetry.timestamp = Utc::now().to_rfc3339();
        
        // Extract Lon, Lat, Height (MSL)
        let lon_raw = i32::from_le_bytes([payload[24], payload[25], payload[26], payload[27]]);
        let lat_raw = i32::from_le_bytes([payload[28], payload[29], payload[30], payload[31]]);
        let h_msl_raw = i32::from_le_bytes([payload[36], payload[37], payload[38], payload[39]]);
        
        telemetry.longitude = Some(lon_raw as f64 * 1e-7);
        telemetry.latitude = Some(lat_raw as f64 * 1e-7);
        telemetry.altitude = Some(h_msl_raw as f64 / 1000.0);
        
        // Extract satellites
        telemetry.satellites = payload[23] as u32;
        
        // Speed (gSpeed in mm/s -> km/h)
        let g_speed_raw = i32::from_le_bytes([payload[60], payload[61], payload[62], payload[63]]);
        telemetry.speed_kmh = Some((g_speed_raw as f64 / 1000.0) * 3.6);
        
        // Heading (headVeh in scale 1e-5)
        let head_veh_raw = i32::from_le_bytes([payload[84], payload[85], payload[86], payload[87]]);
        telemetry.heading = Some(head_veh_raw as f64 * 1e-5);
        
        // Fix status
        let fix_type_raw = payload[20];
        let flags = payload[21];
        
        // carrSoln is bits 6-7 of flags
        let carr_soln = (flags >> 6) & 0x03;
        
        telemetry.fix_type = if (flags & 0x01) == 0 {
            "NO_FIX".to_string()
        } else if carr_soln == 2 {
            "RTK_FIXED".to_string()
        } else if carr_soln == 1 {
            "RTK_FLOAT".to_string()
        } else if (flags & 0x02) != 0 {
            "DGPS".to_string()
        } else {
            match fix_type_raw {
                3 => "3D_FIX".to_string(),
                2 => "2D_FIX".to_string(),
                _ => "NO_FIX".to_string(),
            }
        };
        
        log::info!(
            "Parsed UBX NAV-PVT: lat={:?}, lon={:?}, alt={:?}, fix={}, sats={}",
            telemetry.latitude,
            telemetry.longitude,
            telemetry.altitude,
            telemetry.fix_type,
            telemetry.satellites
        );
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fletcher() {
        let data = [0x01, 0x07, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00];
        let (ck_a, ck_b) = calc_fletcher(&data);
        assert_eq!((ck_a, ck_b), (0x0c, 0x51));
    }
    
    #[test]
    fn test_parse_nav_pvt() {
        let mut payload = vec![0u8; 92];
        
        // Mocking values
        // satellites: offset 23
        payload[23] = 18;
        // lon: offset 24 (100.5018247 -> 1005018247)
        let lon_bytes = 1005018247i32.to_le_bytes();
        payload[24..28].copy_from_slice(&lon_bytes);
        // lat: offset 28 (13.7563024 -> 137563024)
        let lat_bytes = 137563024i32.to_le_bytes();
        payload[28..32].copy_from_slice(&lat_bytes);
        // hMSL: offset 36 (15.24 m -> 15240 mm)
        let h_bytes = 15240i32.to_le_bytes();
        payload[36..40].copy_from_slice(&h_bytes);
        
        // fixType: offset 20 (3 = 3D)
        payload[20] = 3;
        // flags: offset 21 (gnssFixOK=1, diffSoln=1, carrSoln=2 (RTK Fixed) -> 1 | 2 | (2<<6) -> 3 | 128 = 131)
        payload[21] = 131;
        
        // gSpeed: offset 60 (1000 mm/s -> 3.6 km/h)
        let speed_bytes = 1000i32.to_le_bytes();
        payload[60..64].copy_from_slice(&speed_bytes);
        
        // headVeh: offset 84 (182.5021 deg -> 18250210)
        let head_bytes = 18250210i32.to_le_bytes();
        payload[84..88].copy_from_slice(&head_bytes);
        
        let mut tel = GpsTelemetry::default();
        let success = update_from_ubx(0x01, 0x07, &payload, &mut tel);
        
        assert!(success);
        assert_eq!(tel.satellites, 18);
        assert!(tel.longitude.unwrap() - 100.5018247 < 1e-6);
        assert!(tel.latitude.unwrap() - 13.7563024 < 1e-6);
        assert!(tel.altitude.unwrap() - 15.24 < 1e-3);
        assert_eq!(tel.fix_type, "RTK_FIXED");
        assert!(tel.speed_kmh.unwrap() - 3.6 < 1e-3);
        assert!(tel.heading.unwrap() - 182.5021 < 1e-4);
    }
}
