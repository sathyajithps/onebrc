use std::sync::mpsc;
use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    thread::{self},
};

// Number of threads that will be spawned to process chunks
const THREADS: u64 = 13;
const MEASUREMENT_FILE: &'static str = "measurements.txt";

// Result from each thread
struct ComputeResult {
    pub consumed_lines: u64,
    pub consumed_bytes: usize,
    //                        city     min  sum  max  count
    pub weather_data: HashMap<String, (f32, f32, f32, f32)>,
}

struct ChunkData {
    pub start: u64,
    pub end: u64,
    pub chunk_size: u64,
}

// The chunk_file function divides a file into smaller, nearly equal parts for multiple threads to
// process in parallel, ensuring each chunk has a defined start and end position.
// If only one thread is used, the entire file is treated as a single chunk.
fn chunk_file(file_size: u64, threads: u64) -> Vec<ChunkData> {
    if threads == 0 || threads == 1 {
        return vec![ChunkData {
            start: 0,
            chunk_size: file_size,
            end: file_size - 1,
        }];
    }

    let round_off = threads - (file_size % threads);

    let rounded_size = file_size + round_off;

    let chunk_size = rounded_size / threads;

    (0..threads)
        .into_iter()
        .map(|n| {
            let start = n * chunk_size;

            let (end, chunk_size) = if n == threads - 1 {
                // For the last chunk, set the end to the actual file size - 1.
                // This avoids overshooting the file size by using the rounded size.
                (file_size - 1, file_size - start)
            } else {
                (start + chunk_size - 1, chunk_size)
            };

            ChunkData {
                start,
                chunk_size,
                end,
            }
        })
        .collect()
}

// This function adjusts the starting positions of the file chunks so that each chunk begins at a
// newline character, ensuring clean line breaks. This prevents data overlap and misalignment
// between chunks when splitting measurements across multiple threads.
fn correct_chunk_position<F: Read + Seek>(file: &mut F, chunks: &mut Vec<ChunkData>, threads: u64) {
    const NEW_LINE: u8 = b'\n';

    for thread_num in 0..threads {
        if thread_num == 0 {
            continue;
        }

        let current = chunks.get_mut(thread_num as usize).unwrap();

        if let Err(_) = file.seek(SeekFrom::Start(current.start - 1)) {
            return;
        }

        let mut file_buf: [u8; 108] = [0; 108];

        file.read_exact(&mut file_buf).unwrap();

        if file_buf[1] == NEW_LINE {
            current.chunk_size -= 1;
            current.start += 1;

            let previous = chunks.get_mut((thread_num - 1) as usize).unwrap();
            previous.chunk_size += 1;
            previous.end += 1;
        } else if file_buf[0] == NEW_LINE {
            // ok
        } else {
            let mut current_index = 2;

            loop {
                if file_buf[current_index] == NEW_LINE {
                    current.start += current_index as u64;
                    current.chunk_size = current.end - current.start + 1;

                    let previous = chunks.get_mut((thread_num - 1) as usize).unwrap();
                    previous.end += current_index as u64;
                    previous.chunk_size = previous.end - previous.start + 1;

                    break;
                }

                current_index += 1;

                if current_index == 107 {
                    return;
                }
            }
        }
    }
}

fn main() {
    let meta = fs::metadata(MEASUREMENT_FILE).unwrap();

    let mut chunks = chunk_file(meta.len(), THREADS);

    let mut file = OpenOptions::new()
        .read(true)
        .open(MEASUREMENT_FILE)
        .unwrap();

    correct_chunk_position(&mut file, &mut chunks, THREADS);

    let mut handles = vec![];

    let (tx, rx) = mpsc::channel::<ComputeResult>();

    for chunk in chunks {
        let tx = tx.clone();
        handles.push(thread::spawn(move || {
            let mut file = OpenOptions::new()
                .read(true)
                .open(MEASUREMENT_FILE)
                .unwrap();

            file.seek(SeekFrom::Start(chunk.start)).unwrap();

            let mut reader = BufReader::with_capacity(4194304, file);

            let mut weather_data = HashMap::<String, (f32, f32, f32, f32)>::new();

            let mut consumed_bytes = 0;

            let mut consumed_lines = 0;

            let mut line = String::new();

            loop {
                let size = match reader.read_until(b'\n', unsafe { line.as_mut_vec() }) {
                    Ok(s) => s,
                    _ => panic!("failed to read line"),
                };

                if size == 0 {
                    break;
                }

                consumed_bytes += size;

                consumed_lines += 1;

                let mut delim_idx = line.len() - 5;

                let bytes = line.as_bytes();

                loop {
                    if bytes[delim_idx] == b';' {
                        break;
                    }
                    delim_idx -= 1;
                }

                let city = &line[0..delim_idx];

                let temp = *&line[delim_idx + 1..(line.len() - 1)]
                    .parse::<f32>()
                    .unwrap();

                match weather_data.get_mut(city) {
                    Some(entry) => {
                        if temp < entry.0 {
                            entry.0 = temp;
                        }
                        if temp > entry.2 {
                            entry.2 = temp;
                        }
                        entry.1 += temp;
                        entry.3 += 1.0;
                    }
                    None => {
                        weather_data.insert(city.to_string(), (temp, temp, temp, 1.0));
                    }
                };

                if consumed_bytes == chunk.chunk_size as usize {
                    break;
                }

                line.clear();
            }

            tx.send(ComputeResult {
                consumed_bytes,
                consumed_lines,
                weather_data,
            })
            .unwrap();
        }));
    }

    let mut consumed_bytes = 0;
    let mut consumed_lines = 0;
    let mut weather_data = HashMap::<String, (f32, f32, f32, f32)>::new();

    let mut received_count = 0;
    while let Ok(compute) = rx.recv() {
        received_count += 1;

        consumed_bytes += compute.consumed_bytes;
        consumed_lines += compute.consumed_lines;
        for (city, readings) in compute.weather_data {
            if let Some(entry) = weather_data.get_mut(&city) {
                if readings.0 < entry.0 {
                    entry.0 = readings.0;
                }

                if readings.2 > entry.2 {
                    entry.2 = readings.2;
                }
                entry.1 += readings.1;
                entry.3 += readings.3;
            } else {
                weather_data.insert(
                    city.to_string(),
                    (readings.0, readings.1, readings.2, readings.3),
                );
            }
        }

        if received_count == THREADS {
            break;
        }
    }

    let mut final_data = vec![];

    for (city, data) in weather_data {
        final_data.push((city, data.0, data.1 / data.3, data.2));
    }

    final_data.sort_by_key(|i| i.0.to_lowercase());

    let mut result = "{".to_string();

    for data in final_data {
        result.push_str(&data.0);
        result.push_str("=");
        result.push_str(&format!("{:.1}/{:.1}/{:.1}, ", data.1, data.2, data.3));
    }

    result = result.trim_end_matches(", ").to_string();

    result.push_str("}");

    println!("{result}");

    println!();
    println!("Consumed lines: {consumed_lines}");
    println!("Consumed bytes: {consumed_bytes}");
}
