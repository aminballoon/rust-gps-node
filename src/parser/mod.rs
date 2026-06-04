pub mod nmea;
pub mod ublox;
pub mod unicore;

use serde::Serialize;

#[derive(Debug, Clone, Serialize, Default, PartialEq)]
pub struct GpsTelemetry {
    pub device_id: String,
    pub timestamp: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub altitude: Option<f64>,
    pub fix_type: String, // "NO_FIX", "3D_FIX", "RTK_FLOAT", "RTK_FIXED", "DGPS", "GPS_SPS"
    pub satellites: u32,
    pub heading: Option<f64>,
    pub speed_kmh: Option<f64>,
}

pub struct Parser {
    buffer: Vec<u8>,
    pub telemetry: GpsTelemetry,
}

impl Parser {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            telemetry: GpsTelemetry::default(),
        }
    }

    pub fn consume(&mut self, data: &[u8]) -> Vec<String> {
        self.buffer.extend_from_slice(data);
        self.process_buffer()
    }

    fn process_buffer(&mut self) -> Vec<String> {
        let mut gga_sentences = Vec::new();
        let mut i = 0;
        while i < self.buffer.len() {
            let b = self.buffer[i];

            // 1. UBX Binary Sync
            if b == 0xB5 && i + 1 < self.buffer.len() && self.buffer[i + 1] == 0x62 {
                if i + 6 <= self.buffer.len() {
                    let len_bytes = [self.buffer[i + 4], self.buffer[i + 5]];
                    let payload_len = u16::from_le_bytes(len_bytes) as usize;
                    let total_len = 6 + payload_len + 2; // Sync(2) + Class(1) + ID(1) + Len(2) + Payload + Checksum(2)

                    if i + total_len <= self.buffer.len() {
                        let packet = &self.buffer[i..i + total_len];
                        let class = packet[2];
                        let id = packet[3];
                        let payload = &packet[6..6 + payload_len];
                        let ck_a = packet[6 + payload_len];
                        let ck_b = packet[6 + payload_len + 1];

                        // Fletcher checksum over Class, ID, Length, and Payload
                        let ck_data = &packet[2..6 + payload_len];
                        let (calc_a, calc_b) = ublox::calc_fletcher(ck_data);
                        if calc_a == ck_a && calc_b == ck_b {
                            ublox::update_from_ubx(class, id, payload, &mut self.telemetry);
                        }
                        i += total_len;
                        continue;
                    } else {
                        // Wait for more data to complete the packet
                        break;
                    }
                } else {
                    // Wait for header to be completed
                    break;
                }
            }
            // 2. NMEA Sentence Sync
            else if b == b'$' {
                if let Some(nl_offset) = self.buffer[i..].iter().position(|&x| x == b'\n') {
                    let end_idx = i + nl_offset;
                    if let Ok(sentence) = std::str::from_utf8(&self.buffer[i..=end_idx]) {
                        nmea::update_from_nmea(sentence, &mut self.telemetry);
                        if sentence.contains("GGA") {
                            gga_sentences.push(sentence.to_string());
                        }
                    }
                    i = end_idx + 1;
                    continue;
                } else {
                    if self.buffer.len() - i > 256 {
                        i += 1; // Discard invalid/too long start
                    }
                    break;
                }
            }
            // 3. Unicore ASCII Sync
            else if b == b'#' {
                if let Some(nl_offset) = self.buffer[i..].iter().position(|&x| x == b'\n') {
                    let end_idx = i + nl_offset;
                    if let Ok(sentence) = std::str::from_utf8(&self.buffer[i..=end_idx]) {
                        unicore::update_from_unicore(sentence, &mut self.telemetry);
                    }
                    i = end_idx + 1;
                    continue;
                } else {
                    if self.buffer.len() - i > 1024 {
                        i += 1; // Discard invalid/too long start
                    }
                    break;
                }
            }
            // 4. Garbage bytes
            else {
                i += 1;
            }
        }

        if i > 0 {
            self.buffer.drain(..std::cmp::min(i, self.buffer.len()));
        }
        gga_sentences
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_parser_mix() {
        let mut parser = Parser::new();
        
        // 1. Send NMEA sentence
        let nmea_data = b"$GNGGA,001043.00,1345.3782,N,10030.1092,E,4,24,0.98,15.2,M,-21.3,M,,*58\n";
        parser.consume(nmea_data);
        assert_eq!(parser.telemetry.fix_type, "RTK_FIXED");
        assert_eq!(parser.telemetry.satellites, 24);
        
        // 2. Send partial UBX, then complete it
        let mut ubx_data = vec![0xB5, 0x62, 0x01, 0x07, 92, 0];
        let mut payload = vec![0u8; 92];
        payload[23] = 12; // satellites
        payload[20] = 3;  // 3D fix
        payload[21] = 131; // RTK Fixed
        ubx_data.extend_from_slice(&payload);
        let (ck_a, ck_b) = ublox::calc_fletcher(&ubx_data[2..]);
        ubx_data.push(ck_a);
        ubx_data.push(ck_b);
        
        // Send half
        parser.consume(&ubx_data[..50]);
        // Satellites should still be 24 (from NMEA) because UBX is not processed yet
        assert_eq!(parser.telemetry.satellites, 24);
        
        // Send remaining half
        parser.consume(&ubx_data[50..]);
        // Now it should be updated to 12
        assert_eq!(parser.telemetry.satellites, 12);
    }
}
