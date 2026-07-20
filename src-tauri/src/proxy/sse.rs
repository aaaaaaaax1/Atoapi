const DEFAULT_MAX_SSE_FRAME_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SseFrame {
    pub event: Option<String>,
    pub data: String,
}

#[derive(Debug, Clone)]
pub(crate) struct SseFrameDecoder {
    current_line: Vec<u8>,
    line_had_bytes: bool,
    event: Option<String>,
    data: String,
    has_data: bool,
    frame_bytes: usize,
    max_frame_bytes: usize,
    discarding_oversized_frame: bool,
    overflowed: bool,
    pending_cr: bool,
    first_line: bool,
}

impl Default for SseFrameDecoder {
    fn default() -> Self {
        Self {
            current_line: Vec::new(),
            line_had_bytes: false,
            event: None,
            data: String::new(),
            has_data: false,
            frame_bytes: 0,
            max_frame_bytes: DEFAULT_MAX_SSE_FRAME_BYTES,
            discarding_oversized_frame: false,
            overflowed: false,
            pending_cr: false,
            first_line: true,
        }
    }
}

impl SseFrameDecoder {
    #[cfg(test)]
    pub(crate) fn with_max_frame_bytes(max_frame_bytes: usize) -> Self {
        Self {
            max_frame_bytes: max_frame_bytes.max(1),
            ..Self::default()
        }
    }

    pub(crate) fn push(&mut self, chunk: &[u8]) -> Vec<SseFrame> {
        let mut frames = Vec::new();
        for &byte in chunk {
            if self.pending_cr {
                self.pending_cr = false;
                if byte == b'\n' {
                    continue;
                }
            }

            match byte {
                b'\r' => {
                    self.finish_line(&mut frames);
                    self.pending_cr = true;
                }
                b'\n' => self.finish_line(&mut frames),
                _ => self.push_line_byte(byte),
            }
        }
        frames
    }

    pub(crate) fn finish(&mut self) -> Vec<SseFrame> {
        // SSE dispatches an event only after a blank line. A pending block at
        // EOF may be a truncated terminal event and must not be promoted.
        self.current_line.clear();
        self.line_had_bytes = false;
        self.reset_frame();
        self.pending_cr = false;
        Vec::new()
    }

    pub(crate) fn overflowed(&self) -> bool {
        self.overflowed
    }

    fn push_line_byte(&mut self, byte: u8) {
        self.line_had_bytes = true;
        if self.discarding_oversized_frame {
            return;
        }
        if self.frame_bytes.saturating_add(1) > self.max_frame_bytes {
            self.mark_frame_overflow();
            return;
        }
        self.frame_bytes += 1;
        self.current_line.push(byte);
    }

    fn finish_line(&mut self, frames: &mut Vec<SseFrame>) {
        let line_had_bytes = self.line_had_bytes;
        self.line_had_bytes = false;

        if self.discarding_oversized_frame {
            self.current_line.clear();
            if !line_had_bytes {
                self.reset_frame();
            }
            self.first_line = false;
            return;
        }

        if !line_had_bytes {
            if self.has_data {
                frames.push(SseFrame {
                    event: self.event.take(),
                    data: std::mem::take(&mut self.data),
                });
            }
            self.reset_frame();
            self.first_line = false;
            return;
        }

        if self.frame_bytes.saturating_add(1) > self.max_frame_bytes {
            self.mark_frame_overflow();
            self.current_line.clear();
            self.first_line = false;
            return;
        }
        self.frame_bytes += 1;

        let line = String::from_utf8_lossy(&self.current_line);
        let line = if self.first_line {
            line.strip_prefix('\u{FEFF}').unwrap_or(&line)
        } else {
            &line
        };
        self.first_line = false;
        if !line.starts_with(':') {
            let (field, value) = line.split_once(':').unwrap_or((line, ""));
            let value = value.strip_prefix(' ').unwrap_or(value);
            match field {
                "event" => self.event = Some(value.to_string()),
                "data" => {
                    if self.has_data {
                        self.data.push('\n');
                    }
                    self.data.push_str(value);
                    self.has_data = true;
                }
                _ => {}
            }
        }
        self.current_line.clear();
    }

    fn mark_frame_overflow(&mut self) {
        self.overflowed = true;
        self.discarding_oversized_frame = true;
        self.current_line.clear();
        self.event = None;
        self.data.clear();
        self.has_data = false;
        self.frame_bytes = 0;
    }

    fn reset_frame(&mut self) {
        self.current_line.clear();
        self.event = None;
        self.data.clear();
        self.has_data = false;
        self.frame_bytes = 0;
        self.discarding_oversized_frame = false;
    }
}

#[cfg(test)]
mod tests {
    use super::{SseFrame, SseFrameDecoder};

    #[test]
    fn frame_decoder_combines_data_lines_and_preserves_event_name() {
        let mut decoder = SseFrameDecoder::default();
        let frames = decoder.push(
            b"event: response.completed\r\ndata: {\"type\":\r\ndata: \"response.completed\"}\r\n\r\n",
        );

        assert_eq!(
            frames,
            vec![SseFrame {
                event: Some("response.completed".to_string()),
                data: "{\"type\":\n\"response.completed\"}".to_string(),
            }]
        );
    }

    #[test]
    fn frame_decoder_handles_split_utf8_and_multiple_frames() {
        let mut decoder = SseFrameDecoder::default();
        let bytes = "data: 你\n\ndata: [DONE]\n\n".as_bytes();
        let split = bytes
            .windows(3)
            .position(|part| part == "你".as_bytes())
            .unwrap()
            + 1;

        assert!(decoder.push(&bytes[..split]).is_empty());
        let frames = decoder.push(&bytes[split..]);

        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].data, "你");
        assert_eq!(frames[1].data, "[DONE]");
    }

    #[test]
    fn frame_decoder_is_independent_of_chunk_boundaries() {
        let bytes = b"\xEF\xBB\xBFevent: response.output_text.delta\r\ndata: {\"delta\":\"ok\"}\r\n\r\ndata: [DONE]\n\n";
        let mut whole = SseFrameDecoder::default();
        let expected = whole.push(bytes);

        for split in 0..=bytes.len() {
            let mut decoder = SseFrameDecoder::default();
            let mut frames = decoder.push(&bytes[..split]);
            frames.extend(decoder.push(&bytes[split..]));
            assert_eq!(frames, expected, "split at byte {split}");
        }
    }

    #[test]
    fn frame_decoder_replaces_invalid_utf8_without_losing_valid_suffix() {
        let mut decoder = SseFrameDecoder::default();
        let frames = decoder.push(b"data: hi\xFFok\n\n");

        assert_eq!(frames.len(), 1);
        assert!(frames[0].data.starts_with("hi"));
        assert!(frames[0].data.ends_with("ok"));
        assert!(frames[0].data.contains('\u{FFFD}'));
    }

    #[test]
    fn frame_decoder_handles_crlf_delimiter_split_across_chunks() {
        let mut decoder = SseFrameDecoder::default();
        let frames = decoder.push(b"data: ok\r\n\r");
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, "ok");

        assert!(decoder.push(b"\n").is_empty());
    }

    #[test]
    fn frame_decoder_supports_bom_cr_and_mixed_line_endings() {
        let mut decoder = SseFrameDecoder::default();
        let frames = decoder.push(b"\xEF\xBB\xBFevent: response.completed\rdata: ok\n\r\n");

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].event.as_deref(), Some("response.completed"));
        assert_eq!(frames[0].data, "ok");
    }

    #[test]
    fn frame_decoder_ignores_event_only_frame() {
        let mut decoder = SseFrameDecoder::default();
        assert!(decoder.push(b"event: response.completed\n\n").is_empty());

        let frames = decoder.push(b"event: response.completed\ndata:\n\n");
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].event.as_deref(), Some("response.completed"));
        assert!(frames[0].data.is_empty());
    }

    #[test]
    fn frame_decoder_discards_unterminated_frame_at_eof() {
        let mut decoder = SseFrameDecoder::default();
        assert!(decoder
            .push(b"data: {\"type\":\"response.completed\"}")
            .is_empty());

        assert!(decoder.finish().is_empty());
    }

    #[test]
    fn frame_decoder_discards_one_oversized_frame_and_recovers() {
        let mut decoder = SseFrameDecoder::with_max_frame_bytes(16);
        assert!(decoder
            .push(b"data: this frame is much too large")
            .is_empty());
        let frames = decoder.push(b"\n\ndata: ok\n\n");

        assert!(decoder.overflowed());
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, "ok");
    }

    #[test]
    fn frame_decoder_recovers_when_oversized_delimiter_is_split() {
        let mut decoder = SseFrameDecoder::with_max_frame_bytes(16);
        assert!(decoder
            .push(b"data: this frame is much too large\n")
            .is_empty());
        let frames = decoder.push(b"\ndata: ok\n\n");

        assert!(decoder.overflowed());
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].data, "ok");
    }
}
