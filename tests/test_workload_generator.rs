use ferrous_cx::WorkloadRecord;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::process::Command;
use std::time::Instant;
use tempfile::NamedTempFile;

#[test]
fn test_cli_file_output_and_exact_byte_sizes() {
    let temp_file = NamedTempFile::new().unwrap();
    let temp_path = temp_file.path().to_str().unwrap().to_string();

    let output = Command::new(env!("CARGO_BIN_EXE_workload-generator"))
        .args([
            "--count",
            "1000",
            "--payload-size",
            "1024",
            "--partitions",
            "4",
            "--run-id",
            "bench-run-99",
            "--output",
            &temp_path,
        ])
        .output()
        .expect("Failed to execute workload-generator");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let file = File::open(&temp_path).unwrap();
    let reader = BufReader::new(file);

    let mut line_count = 0;
    for (seq, line_res) in reader.lines().enumerate() {
        let line = line_res.unwrap();
        assert_eq!(
            line.len(),
            1024,
            "Line {} length {} != 1024",
            seq,
            line.len()
        );

        let rec: WorkloadRecord =
            serde_json::from_str(&line).expect("Must parse into WorkloadRecord");
        assert_eq!(rec.run_id, "bench-run-99");
        assert_eq!(rec.seq, seq as u64);
        assert_eq!(rec.record_id, format!("bench-run-99:{:08}", seq));
        assert_eq!(rec.partition, (seq % 4) as u32);
        assert_eq!(rec.payload_size_bytes, 1024);
        line_count += 1;
    }

    assert_eq!(line_count, 1000);
}

#[test]
fn test_cli_stdout_stream() {
    let output = Command::new(env!("CARGO_BIN_EXE_workload-generator"))
        .args(["-c", "100", "-s", "256", "-p", "2", "-o", "-"])
        .output()
        .expect("Failed to execute workload-generator");

    assert!(output.status.success());
    let stdout_str = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = stdout_str.lines().collect();
    assert_eq!(lines.len(), 100);

    for (seq, line) in lines.iter().enumerate() {
        assert_eq!(line.len(), 256);
        let rec: WorkloadRecord = serde_json::from_str(line).unwrap();
        assert_eq!(rec.seq, seq as u64);
        assert_eq!(rec.partition, (seq % 2) as u32);
        assert_eq!(rec.payload_size_bytes, 256);
    }
}

#[test]
fn test_cli_high_throughput_generation() {
    let temp_file = NamedTempFile::new().unwrap();
    let temp_path = temp_file.path().to_str().unwrap().to_string();

    let start = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_workload-generator"))
        .args(["-c", "50000", "-s", "512", "-p", "4", "-o", &temp_path])
        .output()
        .expect("Failed to execute workload-generator");

    let duration = start.elapsed();
    assert!(output.status.success());

    let records_per_sec = 50_000.0 / duration.as_secs_f64();
    println!(
        "Workload generation: 50,000 records in {:?} ({:.0} recs/sec)",
        duration, records_per_sec
    );
    assert!(
        records_per_sec > 10_000.0,
        "Throughput was too slow: {:.0} recs/sec",
        records_per_sec
    );
}
